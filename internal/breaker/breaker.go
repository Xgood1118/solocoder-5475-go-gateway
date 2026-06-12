package breaker

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/api-gateway/internal/metrics"
)

type State int32

const (
	Closed   State = 0
	Open     State = 1
	HalfOpen State = 2
)

type Breaker struct {
	name          string
	threshold     int
	timeout       time.Duration
	state         atomic.Int32
	failures      atomic.Int64
	lastFailure   atomic.Int64
	halfOpenProbe atomic.Bool
	metrics       *metrics.Metrics
	mu            sync.Mutex
}

type Manager struct {
	breakers map[string]*Breaker
	mu       sync.RWMutex
	metrics  *metrics.Metrics
}

func NewManager(m *metrics.Metrics) *Manager {
	return &Manager{
		breakers: make(map[string]*Breaker),
		metrics:  m,
	}
}

func (mgr *Manager) Get(name string) *Breaker {
	mgr.mu.RLock()
	b, ok := mgr.breakers[name]
	mgr.mu.RUnlock()
	if ok {
		return b
	}

	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if b, ok := mgr.breakers[name]; ok {
		return b
	}

	b = &Breaker{
		name:      name,
		threshold: 5,
		timeout:   30 * time.Second,
		metrics:   mgr.metrics,
	}
	b.state.Store(int32(Closed))
	mgr.breakers[name] = b
	return b
}

func (mgr *Manager) Update(name string, threshold int, timeout time.Duration) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	b, ok := mgr.breakers[name]
	if !ok {
		b = &Breaker{
			name:    name,
			metrics: mgr.metrics,
		}
		b.state.Store(int32(Closed))
		mgr.breakers[name] = b
	}
	b.threshold = threshold
	b.timeout = timeout
}

func (b *Breaker) State() State {
	return State(b.state.Load())
}

func (b *Breaker) Allow() (bool, error) {
	current := State(b.state.Load())

	switch current {
	case Closed:
		return true, nil
	case Open:
		lastFail := time.Unix(0, b.lastFailure.Load())
		if time.Since(lastFail) >= b.timeout {
			if b.state.CompareAndSwap(int32(Open), int32(HalfOpen)) {
				b.halfOpenProbe.Store(false)
				b.metrics.SetBreakerState(b.name, 2)
				return true, nil
			}
			return false, fmt.Errorf("circuit breaker open for %s", b.name)
		}
		return false, fmt.Errorf("circuit breaker open for %s", b.name)
	case HalfOpen:
		if b.halfOpenProbe.CompareAndSwap(false, true) {
			return true, nil
		}
		return false, fmt.Errorf("circuit breaker half-open, probe in progress for %s", b.name)
	default:
		return false, fmt.Errorf("unknown circuit breaker state for %s", b.name)
	}
}

func (b *Breaker) IsHalfOpenProbe() bool {
	return State(b.state.Load()) == HalfOpen && b.halfOpenProbe.Load()
}

func (b *Breaker) RecordSuccess() {
	current := State(b.state.Load())
	if current == HalfOpen {
		b.state.Store(int32(Closed))
		b.failures.Store(0)
		b.halfOpenProbe.Store(false)
		b.metrics.SetBreakerState(b.name, 0)
		return
	}
	b.failures.Store(0)
}

func (b *Breaker) RecordFailure() {
	b.lastFailure.Store(time.Now().UnixNano())
	newCount := b.failures.Add(1)

	current := State(b.state.Load())
	if current == HalfOpen {
		b.state.Store(int32(Open))
		b.halfOpenProbe.Store(false)
		b.metrics.SetBreakerState(b.name, 1)
		return
	}

	if newCount >= int64(b.threshold) {
		if b.state.CompareAndSwap(int32(Closed), int32(Open)) {
			b.metrics.SetBreakerState(b.name, 1)
		}
	}
}
