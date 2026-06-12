package metrics

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	requestTotal    *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	upstreamErrors  *prometheus.CounterVec
	activeConns     *prometheus.GaugeVec
	retryTotal      *prometheus.CounterVec
	breakerState    *prometheus.GaugeVec
	rateLimitReject *prometheus.CounterVec
	bodySizeExceed  *prometheus.CounterVec
	dryRunRequests  *prometheus.CounterVec
	canaryRequests  *prometheus.CounterVec

	ringBuffers map[string]*ringBuffer
	mu          sync.RWMutex
}

type ringBuffer struct {
	minutely [60]atomic.Int64
	hourly   [60]atomic.Int64
	daily    [24]atomic.Int64

	minuteIdx atomic.Int64
	hourIdx   atomic.Int64
	dayIdx    atomic.Int64
}

func New() *Metrics {
	m := &Metrics{
		ringBuffers: make(map[string]*ringBuffer),
		requestTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_request_total",
			Help: "Total number of requests processed",
		}, []string{"upstream", "method", "status"}),
		requestDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gateway_request_duration_seconds",
			Help:    "Request duration in seconds",
			Buckets: prometheus.DefBuckets,
		}, []string{"upstream", "method"}),
		upstreamErrors: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_upstream_errors_total",
			Help: "Total upstream errors",
		}, []string{"upstream", "error_type"}),
		activeConns: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gateway_active_connections",
			Help: "Current active connections per upstream",
		}, []string{"upstream"}),
		retryTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_retry_total",
			Help: "Total number of retries",
		}, []string{"upstream", "method", "status"}),
		breakerState: promauto.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gateway_breaker_state",
			Help: "Circuit breaker state: 0=closed, 1=open, 2=half-open",
		}, []string{"upstream"}),
		rateLimitReject: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_rate_limit_reject_total",
			Help: "Total rate limit rejections",
		}, []string{"upstream"}),
		bodySizeExceed: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_body_size_exceed_total",
			Help: "Total body size limit exceeded",
		}, []string{"upstream"}),
		dryRunRequests: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_dry_run_total",
			Help: "Total dry-run mode requests",
		}, []string{"upstream"}),
		canaryRequests: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_canary_requests_total",
			Help: "Total canary requests",
		}, []string{"upstream", "variant"}),
	}

	go m.rotateRingBuffers()
	return m
}

func (m *Metrics) RecordRequest(upstream, method string, status int, duration time.Duration) {
	m.requestTotal.WithLabelValues(upstream, method, statusCode(status)).Inc()
	m.requestDuration.WithLabelValues(upstream, method).Observe(duration.Seconds())

	m.mu.RLock()
	rb, ok := m.ringBuffers[upstream]
	m.mu.RUnlock()
	if ok {
		now := time.Now()
		minuteSlot := now.Second() % 60
		rb.minutely[minuteSlot].Add(1)

		hourSlot := now.Minute() % 60
		rb.hourly[hourSlot].Add(1)

		daySlot := now.Hour() % 24
		rb.daily[daySlot].Add(1)
	}
}

func (m *Metrics) RecordRetry(upstream, method string, status int) {
	m.retryTotal.WithLabelValues(upstream, method, statusCode(status)).Inc()
}

func (m *Metrics) RecordError(upstream, errorType string) {
	m.upstreamErrors.WithLabelValues(upstream, errorType).Inc()
}

func (m *Metrics) IncConn(upstream string) {
	m.activeConns.WithLabelValues(upstream).Inc()
}

func (m *Metrics) DecConn(upstream string) {
	m.activeConns.WithLabelValues(upstream).Dec()
}

func (m *Metrics) SetBreakerState(upstream string, state float64) {
	m.breakerState.WithLabelValues(upstream).Set(state)
}

func (m *Metrics) RecordRateLimitReject(upstream string) {
	m.rateLimitReject.WithLabelValues(upstream).Inc()
}

func (m *Metrics) RecordBodySizeExceed(upstream string) {
	m.bodySizeExceed.WithLabelValues(upstream).Inc()
}

func (m *Metrics) RecordDryRun(upstream string) {
	m.dryRunRequests.WithLabelValues(upstream).Inc()
}

func (m *Metrics) RecordCanary(upstream, variant string) {
	m.canaryRequests.WithLabelValues(upstream, variant).Inc()
}

func (m *Metrics) EnsureUpstream(upstream string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.ringBuffers[upstream]; !ok {
		m.ringBuffers[upstream] = &ringBuffer{}
	}
}

func (m *Metrics) GetQPS(upstream string, granularity string) int64 {
	m.mu.RLock()
	rb, ok := m.ringBuffers[upstream]
	m.mu.RUnlock()
	if !ok {
		return 0
	}

	switch granularity {
	case "minute":
		var total int64
		for i := 0; i < 60; i++ {
			total += rb.minutely[i].Load()
		}
		return total
	case "hour":
		var total int64
		for i := 0; i < 60; i++ {
			total += rb.hourly[i].Load()
		}
		return total
	case "day":
		var total int64
		for i := 0; i < 24; i++ {
			total += rb.daily[i].Load()
		}
		return total
	default:
		return 0
	}
}

func (m *Metrics) GetQPSRanking(granularity string) []struct {
	Upstream string
	Count    int64
} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ranking := make([]struct {
		Upstream string
		Count    int64
	}, 0, len(m.ringBuffers))

	for upstream := range m.ringBuffers {
		count := m.GetQPS(upstream, granularity)
		ranking = append(ranking, struct {
			Upstream string
			Count    int64
		}{Upstream: upstream, Count: count})
	}
	return ranking
}

func (m *Metrics) rotateRingBuffers() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		m.mu.RLock()
		for _, rb := range m.ringBuffers {
			minuteSlot := time.Now().Second() % 60
			rb.minutely[minuteSlot].Store(0)
		}
		m.mu.RUnlock()
	}

}

func statusCode(s int) string {
	if s >= 200 && s < 300 {
		return "2xx"
	}
	if s >= 300 && s < 400 {
		return "3xx"
	}
	if s >= 400 && s < 500 {
		return "4xx"
	}
	return "5xx"
}
