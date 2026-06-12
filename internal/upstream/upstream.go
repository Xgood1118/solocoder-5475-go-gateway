package upstream

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/api-gateway/internal/config"
	"go.uber.org/zap"
)

type HealthStatus struct {
	Healthy  bool      `json:"healthy"`
	LastCheck time.Time `json:"last_check"`
	Error     string    `json:"error,omitempty"`
}

type Upstream struct {
	Name          string
	Target        string
	DryRun        bool
	Healthy       atomic.Bool
	HealthCheck   HealthCheckConfig
	BodySizeLimit int64
	Retry         config.RetryConfig
	LastCheck     atomic.Int64
	CheckError    atomic.Pointer[string]
}

type HealthCheckConfig struct {
	Enabled  bool
	Interval time.Duration
}

type Manager struct {
	upstreams map[string]*Upstream
	mu        sync.RWMutex
	client    *http.Client
	logger    *zap.Logger
	stopCh    chan struct{}
}

func NewManager(logger *zap.Logger) *Manager {
	return &Manager{
		upstreams: make(map[string]*Upstream),
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
		logger: logger,
		stopCh: make(chan struct{}),
	}
}

func (mgr *Manager) Get(name string) *Upstream {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	return mgr.upstreams[name]
}

func (mgr *Manager) Register(name, target string, dryRun bool, hc HealthCheckConfig, bodySizeLimit int64, retryCfg config.RetryConfig) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	u, ok := mgr.upstreams[name]
	if !ok {
		u = &Upstream{
			Name:          name,
			Target:        target,
			DryRun:        dryRun,
			HealthCheck:   hc,
			BodySizeLimit: bodySizeLimit,
			Retry:         retryCfg,
		}
		u.Healthy.Store(true)
		mgr.upstreams[name] = u
	} else {
		u.Target = target
		u.DryRun = dryRun
		u.HealthCheck = hc
		u.BodySizeLimit = bodySizeLimit
		u.Retry = retryCfg
	}
}

func (mgr *Manager) Remove(name string) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	delete(mgr.upstreams, name)
}

func (mgr *Manager) StartHealthChecks() {
	go mgr.runHealthChecks()
}

func (mgr *Manager) runHealthChecks() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-mgr.stopCh:
			return
		case <-ticker.C:
			mgr.checkAll()
		}
	}
}

func (mgr *Manager) checkAll() {
	mgr.mu.RLock()
	upstreams := make([]*Upstream, 0, len(mgr.upstreams))
	for _, u := range mgr.upstreams {
		if u.HealthCheck.Enabled {
			upstreams = append(upstreams, u)
		}
	}
	mgr.mu.RUnlock()

	for _, u := range upstreams {
		mgr.checkOne(u)
	}
}

func (mgr *Manager) checkOne(u *Upstream) {
	url := u.Target + "/health"
	req, err := http.NewRequest(http.MethodHead, url, nil)
	if err != nil {
		u.Healthy.Store(false)
		errStr := err.Error()
		u.CheckError.Store(&errStr)
		u.LastCheck.Store(time.Now().UnixNano())
		mgr.logger.Warn("health check failed",
			zap.String("upstream", u.Name),
			zap.Error(err),
		)
		return
	}

	resp, err := mgr.client.Do(req)
	u.LastCheck.Store(time.Now().UnixNano())
	if err != nil {
		u.Healthy.Store(false)
		errStr := err.Error()
		u.CheckError.Store(&errStr)
		mgr.logger.Warn("health check failed",
			zap.String("upstream", u.Name),
			zap.Error(err),
		)
		return
	}
	resp.Body.Close()

	if resp.StatusCode >= 500 {
		u.Healthy.Store(false)
		errStr := "upstream returned " + http.StatusText(resp.StatusCode)
		u.CheckError.Store(&errStr)
		mgr.logger.Warn("health check unhealthy",
			zap.String("upstream", u.Name),
			zap.Int("status", resp.StatusCode),
		)
		return
	}

	u.Healthy.Store(true)
	u.CheckError.Store(nil)
	mgr.logger.Debug("health check passed", zap.String("upstream", u.Name))
}

func (mgr *Manager) AllHealthy() (bool, map[string]HealthStatus) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	statuses := make(map[string]HealthStatus)
	allHealthy := true

	for name, u := range mgr.upstreams {
		status := HealthStatus{
			Healthy:   u.Healthy.Load(),
			LastCheck: time.Unix(0, u.LastCheck.Load()),
		}
		if errPtr := u.CheckError.Load(); errPtr != nil {
			status.Error = *errPtr
		}
		statuses[name] = status
		if !u.Healthy.Load() {
			allHealthy = false
		}
	}
	return allHealthy, statuses
}

func (mgr *Manager) Stop() {
	close(mgr.stopCh)
}

func (mgr *Manager) List() []string {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	names := make([]string, 0, len(mgr.upstreams))
	for name := range mgr.upstreams {
		names = append(names, name)
	}
	return names
}
