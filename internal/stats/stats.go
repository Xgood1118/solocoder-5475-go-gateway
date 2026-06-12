package stats

import (
	"sort"
	"sync"
	"time"
)

type SlowRequest struct {
	RequestID  string    `json:"request_id"`
	Method     string    `json:"method"`
	Path       string    `json:"path"`
	Upstream   string    `json:"upstream"`
	DurationMs float64   `json:"duration_ms"`
	Timestamp  time.Time `json:"timestamp"`
}

type UpstreamStats struct {
	Upstream          string `json:"upstream"`
	QPSMinute         int64  `json:"qps_minute"`
	QPSHour           int64  `json:"qps_hour"`
	QPSDay            int64  `json:"qps_day"`
	BreakerTriggers   int64  `json:"breaker_triggers"`
	RateLimitRejects  int64  `json:"rate_limit_rejects"`
	TotalRequests     int64  `json:"total_requests"`
	ErrorCount        int64  `json:"error_count"`
}

type Stats struct {
	slowReqs       []SlowRequest
	slowReqMu      sync.Mutex
	maxSlowReqs    int

	breakerTriggers   map[string]int64
	breakerMu         sync.Mutex

	rateLimitRejects  map[string]int64
	rateLimitMu       sync.Mutex

	totalRequests     map[string]int64
	totalMu          sync.Mutex

	errorCounts       map[string]int64
	errorMu          sync.Mutex
}

func New() *Stats {
	return &Stats{
		slowReqs:         make([]SlowRequest, 0, 10),
		maxSlowReqs:      10,
		breakerTriggers:  make(map[string]int64),
		rateLimitRejects: make(map[string]int64),
		totalRequests:    make(map[string]int64),
		errorCounts:      make(map[string]int64),
	}
}

func (s *Stats) RecordSlowRequest(req SlowRequest) {
	s.slowReqMu.Lock()
	defer s.slowReqMu.Unlock()

	s.slowReqs = append(s.slowReqs, req)
	sort.Slice(s.slowReqs, func(i, j int) bool {
		return s.slowReqs[i].DurationMs > s.slowReqs[j].DurationMs
	})
	if len(s.slowReqs) > s.maxSlowReqs {
		s.slowReqs = s.slowReqs[:s.maxSlowReqs]
	}
}

func (s *Stats) GetSlowRequests() []SlowRequest {
	s.slowReqMu.Lock()
	defer s.slowReqMu.Unlock()
	result := make([]SlowRequest, len(s.slowReqs))
	copy(result, s.slowReqs)
	return result
}

func (s *Stats) RecordBreakerTrigger(upstream string) {
	s.breakerMu.Lock()
	s.breakerTriggers[upstream]++
	s.breakerMu.Unlock()
}

func (s *Stats) GetBreakerTriggers() map[string]int64 {
	s.breakerMu.Lock()
	defer s.breakerMu.Unlock()
	result := make(map[string]int64, len(s.breakerTriggers))
	for k, v := range s.breakerTriggers {
		result[k] = v
	}
	return result
}

func (s *Stats) RecordRateLimitReject(upstream string) {
	s.rateLimitMu.Lock()
	s.rateLimitRejects[upstream]++
	s.rateLimitMu.Unlock()
}

func (s *Stats) GetRateLimitRejects() map[string]int64 {
	s.rateLimitMu.Lock()
	defer s.rateLimitMu.Unlock()
	result := make(map[string]int64, len(s.rateLimitRejects))
	for k, v := range s.rateLimitRejects {
		result[k] = v
	}
	return result
}

func (s *Stats) RecordRequest(upstream string) {
	s.totalMu.Lock()
	s.totalRequests[upstream]++
	s.totalMu.Unlock()
}

func (s *Stats) GetTotalRequests() map[string]int64 {
	s.totalMu.Lock()
	defer s.totalMu.Unlock()
	result := make(map[string]int64, len(s.totalRequests))
	for k, v := range s.totalRequests {
		result[k] = v
	}
	return result
}

func (s *Stats) RecordError(upstream string) {
	s.errorMu.Lock()
	s.errorCounts[upstream]++
	s.errorMu.Unlock()
}

func (s *Stats) GetErrorCounts() map[string]int64 {
	s.errorMu.Lock()
	defer s.errorMu.Unlock()
	result := make(map[string]int64, len(s.errorCounts))
	for k, v := range s.errorCounts {
		result[k] = v
	}
	return result
}
