package proxy

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/api-gateway/internal/metrics"
	"github.com/api-gateway/internal/retry"
)

type Proxy struct {
	client  *http.Client
	metrics *metrics.Metrics
}

func New(m *metrics.Metrics) *Proxy {
	return &Proxy{
		client: &http.Client{
			Timeout: 60 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		metrics: m,
	}
}

type Result struct {
	StatusCode int
	Duration   time.Duration
	Body       []byte
	Headers    http.Header
	IsRetry    bool
}

func (p *Proxy) Forward(r *http.Request, target string, upstream string, retryer *retry.Retryer) (*Result, error) {
	result, err := p.doForward(r, target, upstream)
	if err != nil {
		return nil, err
	}

	if retryer != nil && retryer.ShouldRetry(r.Method, result.StatusCode) {
		p.metrics.RecordRetry(upstream, r.Method, result.StatusCode)

		time.Sleep(retryer.RetryInterval())

		cloneReq, err := retry.CloneRequest(r)
		if err != nil {
			return result, nil
		}

		retryResult, retryErr := p.doForward(cloneReq, target, upstream)
		if retryErr != nil {
			return result, nil
		}
		retryResult.IsRetry = true
		return retryResult, nil
	}

	return result, nil
}

func (p *Proxy) doForward(r *http.Request, target string, upstream string) (*Result, error) {
	url := target + r.URL.Path
	if r.URL.RawQuery != "" {
		url += "?" + r.URL.RawQuery
	}

	var bodyReader io.Reader
	if r.Body != nil {
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read request body: %w", err)
		}
		r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		bodyReader = bytes.NewReader(bodyBytes)
	}

	proxyReq, err := http.NewRequest(r.Method, url, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create proxy request: %w", err)
	}

	for k, vv := range r.Header {
		for _, v := range vv {
			proxyReq.Header.Add(k, v)
		}
	}

	proxyReq.Host = stripScheme(target)

	start := time.Now()
	resp, err := p.client.Do(proxyReq)
	duration := time.Since(start)
	if err != nil {
		return &Result{
			StatusCode: 502,
			Duration:   duration,
		}, fmt.Errorf("upstream request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return &Result{
			StatusCode: resp.StatusCode,
			Duration:   duration,
			Headers:    resp.Header,
		}, fmt.Errorf("failed to read response body: %w", err)
	}

	return &Result{
		StatusCode: resp.StatusCode,
		Duration:   duration,
		Body:       respBody,
		Headers:    resp.Header,
	}, nil
}

func stripScheme(target string) string {
	if len(target) > 7 && target[:7] == "http://" {
		return target[7:]
	}
	if len(target) > 8 && target[:8] == "https://" {
		return target[8:]
	}
	return target
}
