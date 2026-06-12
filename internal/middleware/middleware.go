package middleware

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/api-gateway/internal/auth"
	"github.com/api-gateway/internal/breaker"
	"github.com/api-gateway/internal/canary"
	"github.com/api-gateway/internal/config"
	"github.com/api-gateway/internal/ipblacklist"
	"github.com/api-gateway/internal/logger"
	"github.com/api-gateway/internal/metrics"
	"github.com/api-gateway/internal/proxy"
	"github.com/api-gateway/internal/ratelimit"
	"github.com/api-gateway/internal/retry"
	"github.com/api-gateway/internal/router"
	"github.com/api-gateway/internal/stats"
	"github.com/api-gateway/internal/tracing"
	upstreamPkg "github.com/api-gateway/internal/upstream"
	wsPkg "github.com/api-gateway/internal/websocket"
)

type Gateway struct {
	configMgr  *config.ConfigManager
	rt         *router.Router
	upstreams  *upstreamPkg.Manager
	breakers   *breaker.Manager
	rateLimits *ratelimit.Manager
	authMgr    *auth.Manager
	blacklist  *ipblacklist.IPBlacklist
	metrics    *metrics.Metrics
	st         *stats.Stats
	px         *proxy.Proxy
	zapLogger  *zap.Logger
}

func NewGateway(
	cfgMgr *config.ConfigManager,
	rt *router.Router,
	upstreams *upstreamPkg.Manager,
	breakers *breaker.Manager,
	rl *ratelimit.Manager,
	authMgr *auth.Manager,
	bl *ipblacklist.IPBlacklist,
	m *metrics.Metrics,
	st *stats.Stats,
	px *proxy.Proxy,
	l *zap.Logger,
) *Gateway {
	return &Gateway{
		configMgr:  cfgMgr,
		rt:         rt,
		upstreams:  upstreams,
		breakers:   breakers,
		rateLimits: rl,
		authMgr:    authMgr,
		blacklist:  bl,
		metrics:    m,
		st:         st,
		px:         px,
		zapLogger:  l,
	}
}

func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		reqID := c.GetHeader("X-Request-ID")
		if reqID == "" {
			reqID = newUUID()
		}
		c.Set("request_id", reqID)
		c.Header("X-Request-ID", reqID)
		c.Request.Header.Set("X-Request-ID", reqID)
		c.Next()
	}
}

func (g *Gateway) IPBlacklist() gin.HandlerFunc {
	return func(c *gin.Context) {
		if g.blacklist.CheckRequest(c.Request) {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "IP address blocked"})
			return
		}
		c.Next()
	}
}

func (g *Gateway) Tracing() gin.HandlerFunc {
	return func(c *gin.Context) {
		cfg := g.configMgr.Get()
		if !cfg.Tracing.Enabled {
			c.Next()
			return
		}
		spanCtx := tracing.ExtractOrCreate(c.Request)
		spanCtx.Inject(c.Request)
		c.Set("trace_id", spanCtx.TraceID)
		c.Set("span_id", spanCtx.SpanID)
		c.Header("traceparent", spanCtx.Traceparent())
		c.Next()
	}
}

func (g *Gateway) ProxyHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		reqID, _ := c.Get("request_id")
		traceID, _ := c.Get("trace_id")

		match := g.rt.Match(c.Request.URL.Path)
		if match == nil {
			c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
				"error": "no route matched", "path": c.Request.URL.Path, "request_id": fmt.Sprintf("%v", reqID),
			})
			return
		}

		upstreamName := match.Upstream

		if match.Route.Canary.Enabled {
			decision := canary.Decide(c.Request, match.Route.Canary, match.Route.Stain)
			upstreamName = decision.Upstream
			c.Set("is_canary", decision.IsCanary)
			c.Set("canary_variant", decision.Variant)
			if decision.IsCanary {
				g.metrics.RecordCanary(match.Upstream, decision.Variant)
			}
		} else {
			c.Set("is_canary", false)
		}

		u := g.upstreams.Get(upstreamName)
		if u == nil {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{
				"error": "upstream not found", "upstream": upstreamName, "request_id": fmt.Sprintf("%v", reqID),
			})
			return
		}

		if u.DryRun {
			g.metrics.RecordDryRun(upstreamName)
			c.AbortWithStatusJSON(http.StatusOK, gin.H{
				"message": "dry-run mode: request matched but not forwarded",
				"upstream": upstreamName, "path": c.Request.URL.Path, "request_id": fmt.Sprintf("%v", reqID),
			})
			return
		}

		if u.BodySizeLimit > 0 && c.Request.ContentLength > u.BodySizeLimit {
			g.metrics.RecordBodySizeExceed(upstreamName)
			c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{
				"error": "request body too large", "limit": u.BodySizeLimit, "request_id": fmt.Sprintf("%v", reqID),
			})
			return
		}

		if c.Request.ContentLength > 0 && u.BodySizeLimit > 0 {
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, u.BodySizeLimit)
		}

		if match.Route.Breaker.Enabled {
			brk := g.breakers.Get(upstreamName)
			allowed, err := brk.Allow()
			if !allowed {
				g.st.RecordBreakerTrigger(upstreamName)
				g.metrics.RecordError(upstreamName, "circuit_open")
				c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{
					"error": err.Error(), "upstream": upstreamName, "request_id": fmt.Sprintf("%v", reqID),
				})
				return
			}
		}

		rl := g.rateLimits.Get(upstreamName)
		brk := g.breakers.Get(upstreamName)
		var rateLimitErr error
		if brk.IsHalfOpenProbe() {
			rateLimitErr = rl.AllowBypassRPS(c.Request)
		} else {
			rateLimitErr = rl.Allow(c.Request)
		}
		if rateLimitErr != nil {
			g.st.RecordRateLimitReject(upstreamName)
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": rateLimitErr.Error(), "upstream": upstreamName, "request_id": fmt.Sprintf("%v", reqID),
			})
			return
		}
		defer rl.Release()

		if err := g.authMgr.Apply(upstreamName, c.Request); err != nil {
			c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
				"error": "auth hook failed: " + err.Error(), "upstream": upstreamName,
			})
			return
		}

		g.metrics.IncConn(upstreamName)

		c.Request.URL.Path = match.PathRewrite

		if match.WebSocket && wsPkg.IsWebSocketUpgrade(c.Request) {
			g.metrics.DecConn(upstreamName)
			if err := wsPkg.Proxy(c.Writer, c.Request, u.Target); err != nil {
				c.AbortWithStatusJSON(http.StatusBadGateway, gin.H{
					"error": "websocket proxy failed: " + err.Error(), "upstream": upstreamName,
				})
			}
			return
		}

		var retryerInst *retry.Retryer
		if u.Retry.Enabled {
			retryerInst = retry.New(u.Retry, g.metrics)
		}

		result, err := g.px.Forward(c.Request, u.Target, upstreamName, retryerInst)
		duration := time.Since(start)

		g.metrics.DecConn(upstreamName)

		if err != nil {
			if match.Route.Breaker.Enabled {
				g.breakers.Get(upstreamName).RecordFailure()
			}
			g.metrics.RecordError(upstreamName, "upstream_error")
			g.st.RecordError(upstreamName)
			g.metrics.RecordRequest(upstreamName, c.Request.Method, 502, duration)
			g.st.RecordRequest(upstreamName)

			durationMs := float64(duration.Nanoseconds()) / 1e6
			isCanary, _ := c.Get("is_canary")
			logger.LogAccess(g.zapLogger, logger.AccessLogEntry{
				Method: c.Request.Method, Path: c.Request.URL.Path, Upstream: upstreamName,
				StatusCode: 502, DurationMs: durationMs, RequestID: fmt.Sprintf("%v", reqID),
				TraceID: fmt.Sprintf("%v", traceID), ClientIP: c.ClientIP(),
				UserAgent: c.Request.UserAgent(), IsCanary: isCanary.(bool),
			})

			c.Header("X-Response-Time", strconv.FormatFloat(durationMs, 'f', 2, 64)+"ms")
			c.Header("X-Request-ID", fmt.Sprintf("%v", reqID))
			c.AbortWithStatusJSON(http.StatusBadGateway, gin.H{
				"error": "upstream error", "upstream": upstreamName, "request_id": fmt.Sprintf("%v", reqID),
			})
			return
		}

		if result.StatusCode >= 500 {
			if match.Route.Breaker.Enabled {
				g.breakers.Get(upstreamName).RecordFailure()
			}
			g.st.RecordError(upstreamName)
		} else {
			if match.Route.Breaker.Enabled {
				g.breakers.Get(upstreamName).RecordSuccess()
			}
		}

		g.metrics.RecordRequest(upstreamName, c.Request.Method, result.StatusCode, result.Duration)
		g.st.RecordRequest(upstreamName)

		durationMs := float64(duration.Nanoseconds()) / 1e6

		if durationMs > 1000 {
			g.st.RecordSlowRequest(stats.SlowRequest{
				RequestID: fmt.Sprintf("%v", reqID), Method: c.Request.Method,
				Path: c.Request.URL.Path, Upstream: upstreamName,
				DurationMs: durationMs, Timestamp: time.Now(),
			})
		}

		isCanary, _ := c.Get("is_canary")
		logger.LogAccess(g.zapLogger, logger.AccessLogEntry{
			Method: c.Request.Method, Path: c.Request.URL.Path, Upstream: upstreamName,
			StatusCode: result.StatusCode, DurationMs: durationMs, RequestID: fmt.Sprintf("%v", reqID),
			TraceID: fmt.Sprintf("%v", traceID), ClientIP: c.ClientIP(),
			UserAgent: c.Request.UserAgent(), IsRetry: result.IsRetry, IsCanary: isCanary.(bool),
		})

		for k, vv := range result.Headers {
			for _, v := range vv {
				c.Writer.Header().Add(k, v)
			}
		}

		c.Header("X-Response-Time", strconv.FormatFloat(durationMs, 'f', 2, 64)+"ms")
		c.Header("X-Request-ID", fmt.Sprintf("%v", reqID))

		c.Data(result.StatusCode, result.Headers.Get("Content-Type"), result.Body)
	}
}

func newUUID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
