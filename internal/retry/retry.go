package retry

import (
	"io"
	"net/http"
	"time"

	"github.com/api-gateway/internal/config"
	"github.com/api-gateway/internal/metrics"
)

type Retryer struct {
	cfg     config.RetryConfig
	metrics *metrics.Metrics
}

func New(cfg config.RetryConfig, m *metrics.Metrics) *Retryer {
	return &Retryer{
		cfg:     cfg,
		metrics: m,
	}
}

func (r *Retryer) ShouldRetry(method string, statusCode int) bool {
	if !r.cfg.Enabled {
		return false
	}

	isRetryable := false
	for _, s := range r.cfg.RetryableStatus {
		if s == statusCode {
			isRetryable = true
			break
		}
	}
	if !isRetryable {
		return false
	}

	isIdempotent := false
	for _, m := range r.cfg.IdempotentMethods {
		if m == method {
			isIdempotent = true
			break
		}
	}
	return isIdempotent
}

func (r *Retryer) MaxRetries() int {
	return r.cfg.MaxRetries
}

func (r *Retryer) RetryInterval() time.Duration {
	return r.cfg.RetryInterval
}

func (r *Retryer) RecordRetry(upstream, method string, statusCode int) {
	r.metrics.RecordRetry(upstream, method, statusCode)
}

func CloneRequest(orig *http.Request) (*http.Request, error) {
	clone := orig.Clone(orig.Context())

	if orig.Body != nil {
		bodyBytes, err := io.ReadAll(orig.Body)
		if err != nil {
			return nil, err
		}
		orig.Body = io.NopCloser(io.MultiReader(
			stringsReader(string(bodyBytes)),
		))
		clone.Body = io.NopCloser(stringsReader(string(bodyBytes)))
		clone.ContentLength = int64(len(bodyBytes))
	}

	return clone, nil
}

type stringReader struct {
	s string
	i int64
}

func stringsReader(s string) *stringReader { return &stringReader{s: s} }

func (r *stringReader) Read(b []byte) (int, error) {
	if r.i >= int64(len(r.s)) {
		return 0, io.EOF
	}
	n := copy(b, r.s[r.i:])
	r.i += int64(n)
	return n, nil
}

func (r *stringReader) Close() error { return nil }
