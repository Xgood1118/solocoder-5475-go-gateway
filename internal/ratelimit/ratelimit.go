package ratelimit

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/api-gateway/internal/metrics"
	"golang.org/x/time/rate"
)

type entry struct {
	limiter  *rate.Limiter
	lastUsed atomicTime
}

type atomicTime struct {
	v int64
}

func (t *atomicTime) Store(v time.Time) {
	t.v = v.UnixNano()
}

func (t *atomicTime) Load() time.Time {
	return time.Unix(0, t.v)
}

type RateLimiter struct {
	rps           int
	maxConcurrent int
	mode          string
	limiters      map[string]*entry
	conns         map[string]int64
	mu            sync.RWMutex
	metrics       *metrics.Metrics
	upstream      string
}

type Manager struct {
	limiters map[string]*RateLimiter
	mu       sync.RWMutex
	metrics  *metrics.Metrics
}

func NewManager(m *metrics.Metrics) *Manager {
	return &Manager{
		limiters: make(map[string]*RateLimiter),
		metrics:  m,
	}
}

func (mgr *Manager) Get(upstream string) *RateLimiter {
	mgr.mu.RLock()
	rl, ok := mgr.limiters[upstream]
	mgr.mu.RUnlock()
	if ok {
		return rl
	}

	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if rl, ok := mgr.limiters[upstream]; ok {
		return rl
	}

	rl = &RateLimiter{
		upstream: upstream,
		limiters: make(map[string]*entry),
		conns:    make(map[string]int64),
		metrics:  mgr.metrics,
		mode:     "ip",
	}
	mgr.limiters[upstream] = rl
	return rl
}

func (mgr *Manager) Update(upstream string, rps, maxConcurrent int, mode string) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	rl, ok := mgr.limiters[upstream]
	if !ok {
		rl = &RateLimiter{
			upstream: upstream,
			limiters: make(map[string]*entry),
			conns:    make(map[string]int64),
			metrics:  mgr.metrics,
		}
		mgr.limiters[upstream] = rl
	}
	rl.rps = rps
	rl.maxConcurrent = maxConcurrent
	rl.mode = mode
}

func (rl *RateLimiter) extractKey(r *http.Request) string {
	switch rl.mode {
	case "token":
		token := r.Header.Get("Authorization")
		if token != "" {
			parts := strings.SplitN(token, " ", 2)
			if len(parts) == 2 {
				return parts[1]
			}
			return token
		}
		return extractIP(r)
	case "both":
		token := r.Header.Get("Authorization")
		ip := extractIP(r)
		if token != "" {
			parts := strings.SplitN(token, " ", 2)
			if len(parts) == 2 {
				return ip + ":" + parts[1]
			}
		}
		return ip
	default:
		return extractIP(r)
	}
}

func extractIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.SplitN(xff, ",", 2)
		return strings.TrimSpace(parts[0])
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}
	idx := strings.LastIndex(r.RemoteAddr, ":")
	if idx >= 0 {
		return r.RemoteAddr[:idx]
	}
	return r.RemoteAddr
}

func (rl *RateLimiter) getLimiter(key string) *rate.Limiter {
	rl.mu.RLock()
	e, ok := rl.limiters[key]
	rl.mu.RUnlock()
	if ok {
		e.lastUsed.Store(time.Now())
		return e.limiter
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	if e, ok := rl.limiters[key]; ok {
		e.lastUsed.Store(time.Now())
		return e.limiter
	}

	limiter := rate.NewLimiter(rate.Limit(rl.rps), rl.rps)
	rl.limiters[key] = &entry{
		limiter:  limiter,
		lastUsed: atomicTime{v: time.Now().UnixNano()},
	}
	return limiter
}

func (rl *RateLimiter) Allow(r *http.Request) error {
	if rl.rps <= 0 && rl.maxConcurrent <= 0 {
		return nil
	}

	if rl.rps > 0 {
		key := rl.extractKey(r)
		limiter := rl.getLimiter(key)
		if !limiter.Allow() {
			rl.metrics.RecordRateLimitReject(rl.upstream)
			return fmt.Errorf("rate limit exceeded for upstream %s", rl.upstream)
		}
	}

	if rl.maxConcurrent > 0 {
		rl.mu.Lock()
		current := rl.conns[rl.upstream]
		if current >= int64(rl.maxConcurrent) {
			rl.mu.Unlock()
			rl.metrics.RecordRateLimitReject(rl.upstream)
			return fmt.Errorf("concurrency limit exceeded for upstream %s", rl.upstream)
		}
		rl.conns[rl.upstream] = current + 1
		rl.mu.Unlock()
	}

	return nil
}

func (rl *RateLimiter) AllowBypassRPS(r *http.Request) error {
	if rl.maxConcurrent <= 0 {
		return nil
	}

	rl.mu.Lock()
	current := rl.conns[rl.upstream]
	if current >= int64(rl.maxConcurrent) {
		rl.mu.Unlock()
		rl.metrics.RecordRateLimitReject(rl.upstream)
		return fmt.Errorf("concurrency limit exceeded for upstream %s", rl.upstream)
	}
	rl.conns[rl.upstream] = current + 1
	rl.mu.Unlock()
	return nil
}

func (rl *RateLimiter) Release() {
	if rl.maxConcurrent > 0 {
		rl.mu.Lock()
		if rl.conns[rl.upstream] > 0 {
			rl.conns[rl.upstream]--
		}
		rl.mu.Unlock()
	}
}

func (mgr *Manager) Cleanup(maxAge time.Duration) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	now := time.Now()
	for _, rl := range mgr.limiters {
		rl.mu.Lock()
		for key, e := range rl.limiters {
			if now.Sub(e.lastUsed.Load()) > maxAge {
				delete(rl.limiters, key)
			}
		}
		rl.mu.Unlock()
	}
}
