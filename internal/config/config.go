package config

import (
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type ServerConfig struct {
	Port         int           `yaml:"port"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
}

type LoggingConfig struct {
	Level string `yaml:"level"`
}

type TracingConfig struct {
	Enabled     bool   `yaml:"enabled"`
	ServiceName string `yaml:"service_name"`
}

type OpenAPIConfig struct {
	Enabled     bool   `yaml:"enabled"`
	Title       string `yaml:"title"`
	Version     string `yaml:"version"`
	Description string `yaml:"description"`
}

type HealthCheckConfig struct {
	Enabled  bool          `yaml:"enabled"`
	Interval time.Duration `yaml:"interval"`
}

type RetryConfig struct {
	Enabled           bool          `yaml:"enabled"`
	MaxRetries        int           `yaml:"max_retries"`
	RetryInterval     time.Duration `yaml:"retry_interval"`
	RetryableStatus   []int         `yaml:"retryable_status"`
	IdempotentMethods []string      `yaml:"idempotent_methods"`
}

type UpstreamConfig struct {
	Name         string           `yaml:"name"`
	Target       string           `yaml:"target"`
	DryRun       bool             `yaml:"dry_run"`
	HealthCheck  HealthCheckConfig `yaml:"health_check"`
	BodySizeLimit int64           `yaml:"body_size_limit"`
	Retry        RetryConfig      `yaml:"retry"`
}

type CanaryVariant struct {
	Upstream string `yaml:"upstream"`
	Weight   int    `yaml:"weight"`
}

type CanaryConfig struct {
	Enabled  bool            `yaml:"enabled"`
	Variants []CanaryVariant `yaml:"variants"`
}

type StainConfig struct {
	CanaryHeader  string            `yaml:"canary_header"`
	CustomHeaders map[string]string `yaml:"custom_headers"`
}

type RateLimitConfig struct {
	RPS           int    `yaml:"rps"`
	MaxConcurrent int    `yaml:"max_concurrent"`
	Mode          string `yaml:"mode"`
}

type BreakerConfig struct {
	Enabled  bool          `yaml:"enabled"`
	Threshold int          `yaml:"threshold"`
	Timeout  time.Duration `yaml:"timeout"`
}

type AuthHookConfig struct {
	Type   string `yaml:"type"`
	Header string `yaml:"header"`
	EnvVar string `yaml:"env_var"`
}

type RouteConfig struct {
	PathPrefix string          `yaml:"path_prefix"`
	Upstream   string          `yaml:"upstream"`
	WebSocket  bool            `yaml:"websocket"`
	StripPrefix string         `yaml:"strip_prefix"`
	AddPrefix  string          `yaml:"add_prefix"`
	AuthHooks  []AuthHookConfig `yaml:"auth_hooks"`
	Canary     CanaryConfig    `yaml:"canary"`
	Stain      StainConfig     `yaml:"stain"`
	RateLimit  RateLimitConfig `yaml:"rate_limit"`
	Breaker    BreakerConfig   `yaml:"breaker"`
}

type Config struct {
	Server      ServerConfig     `yaml:"server"`
	Logging     LoggingConfig    `yaml:"logging"`
	Tracing     TracingConfig    `yaml:"tracing"`
	OpenAPI     OpenAPIConfig    `yaml:"openapi"`
	IPBlacklist []string         `yaml:"ip_blacklist"`
	Upstreams   []UpstreamConfig `yaml:"upstreams"`
	Routes      []RouteConfig    `yaml:"routes"`
}

type ConfigManager struct {
	config     atomic.Pointer[Config]
	configPath string
	watcher    *fsnotify.Watcher
	mu         sync.RWMutex
	onChange   []func(*Config)
	logger     *zap.Logger
}

func NewConfigManager(path string, logger *zap.Logger) (*ConfigManager, error) {
	cm := &ConfigManager{
		configPath: path,
		logger:     logger,
	}

	cfg, err := cm.load()
	if err != nil {
		return nil, err
	}
	cm.config.Store(cfg)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	cm.watcher = watcher

	if err := watcher.Add(path); err != nil {
		return nil, err
	}

	go cm.watch()
	return cm, nil
}

func (cm *ConfigManager) load() (*Config, error) {
	data, err := os.ReadFile(cm.configPath)
	if err != nil {
		return nil, err
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	if cfg.Server.Port == 0 {
		cfg.Server.Port = 8080
	}
	if cfg.Server.ReadTimeout == 0 {
		cfg.Server.ReadTimeout = 30 * time.Second
	}
	if cfg.Server.WriteTimeout == 0 {
		cfg.Server.WriteTimeout = 30 * time.Second
	}
	if cfg.Logging.Level == "" {
		cfg.Logging.Level = "info"
	}

	for i := range cfg.Upstreams {
		if cfg.Upstreams[i].HealthCheck.Interval == 0 {
			cfg.Upstreams[i].HealthCheck.Interval = 5 * time.Second
		}
		if cfg.Upstreams[i].Retry.RetryInterval == 0 {
			cfg.Upstreams[i].Retry.RetryInterval = 100 * time.Millisecond
		}
		if cfg.Upstreams[i].Retry.MaxRetries == 0 {
			cfg.Upstreams[i].Retry.MaxRetries = 1
		}
		if len(cfg.Upstreams[i].Retry.RetryableStatus) == 0 {
			cfg.Upstreams[i].Retry.RetryableStatus = []int{502, 503, 504}
		}
		if len(cfg.Upstreams[i].Retry.IdempotentMethods) == 0 {
			cfg.Upstreams[i].Retry.IdempotentMethods = []string{"GET", "PUT", "DELETE"}
		}
	}

	return cfg, nil
}

func (cm *ConfigManager) watch() {
	var debounceTimer *time.Timer
	for {
		select {
		case event, ok := <-cm.watcher.Events:
			if !ok {
				return
			}
			if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) || event.Has(fsnotify.Rename) {
				if debounceTimer != nil {
					debounceTimer.Stop()
				}
				debounceTimer = time.AfterFunc(500*time.Millisecond, func() {
					cm.reload()
				})
			}
		case err, ok := <-cm.watcher.Errors:
			if !ok {
				return
			}
			cm.logger.Error("config watcher error", zap.Error(err))
		}
	}
}

func (cm *ConfigManager) reload() {
	cfg, err := cm.load()
	if err != nil {
		cm.logger.Error("failed to reload config", zap.Error(err))
		return
	}

	cm.config.Store(cfg)
	cm.logger.Info("config reloaded successfully")

	cm.mu.RLock()
	defer cm.mu.RUnlock()
	for _, fn := range cm.onChange {
		fn(cfg)
	}
}

func (cm *ConfigManager) Get() *Config {
	return cm.config.Load()
}

func (cm *ConfigManager) OnChange(fn func(*Config)) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.onChange = append(cm.onChange, fn)
}

func (cm *ConfigManager) Close() error {
	if cm.watcher != nil {
		return cm.watcher.Close()
	}
	return nil
}
