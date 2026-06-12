package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func New(level string) *zap.Logger {
	var zapLevel zapcore.Level
	if err := zapLevel.UnmarshalText([]byte(level)); err != nil {
		zapLevel = zapcore.InfoLevel
	}

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.MillisDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.AddSync(os.Stdout),
		zapLevel,
	)

	return zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))
}

type AccessLogEntry struct {
	Method      string  `json:"method"`
	Path        string  `json:"path"`
	Upstream    string  `json:"upstream"`
	StatusCode  int     `json:"status_code"`
	DurationMs  float64 `json:"duration_ms"`
	RequestID   string  `json:"request_id"`
	TraceID     string  `json:"trace_id"`
	ClientIP    string  `json:"client_ip"`
	UserAgent   string  `json:"user_agent"`
	IsRetry     bool    `json:"is_retry"`
	IsCanary    bool    `json:"is_canary"`
}

func LogAccess(logger *zap.Logger, entry AccessLogEntry) {
	logger.Info("access",
		zap.String("method", entry.Method),
		zap.String("path", entry.Path),
		zap.String("upstream", entry.Upstream),
		zap.Int("status_code", entry.StatusCode),
		zap.Float64("duration_ms", entry.DurationMs),
		zap.String("request_id", entry.RequestID),
		zap.String("trace_id", entry.TraceID),
		zap.String("client_ip", entry.ClientIP),
		zap.String("user_agent", entry.UserAgent),
		zap.Bool("is_retry", entry.IsRetry),
		zap.Bool("is_canary", entry.IsCanary),
	)
}
