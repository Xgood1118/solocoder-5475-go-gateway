package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/api-gateway/internal/auth"
	"github.com/api-gateway/internal/breaker"
	"github.com/api-gateway/internal/config"
	"github.com/api-gateway/internal/ipblacklist"
	"github.com/api-gateway/internal/logger"
	"github.com/api-gateway/internal/metrics"
	"github.com/api-gateway/internal/middleware"
	"github.com/api-gateway/internal/openapi"
	"github.com/api-gateway/internal/proxy"
	"github.com/api-gateway/internal/ratelimit"
	"github.com/api-gateway/internal/router"
	"github.com/api-gateway/internal/stats"
	upstreamPkg "github.com/api-gateway/internal/upstream"
)

func main() {
	cfgPath := "config.yaml"
	if p := os.Getenv("CONFIG_PATH"); p != "" {
		cfgPath = p
	}

	zapLogger := logger.New("info")
	defer zapLogger.Sync()

	cfgMgr, err := config.NewConfigManager(cfgPath, zapLogger)
	if err != nil {
		zapLogger.Fatal("failed to load config", zap.Error(err))
		os.Exit(1)
	}

	initialCfg := cfgMgr.Get()
	if lvl := initialCfg.Logging.Level; lvl != "" {
		zapLogger = logger.New(lvl)
	}

	m := metrics.New()
	st := stats.New()
	rt := router.New()
	upstreamMgr := upstreamPkg.NewManager(zapLogger)
	breakerMgr := breaker.NewManager(m)
	rlMgr := ratelimit.NewManager(m)
	authMgr := auth.NewManager()
	bl := ipblacklist.New()
	px := proxy.New(m)
	apiGen := openapi.NewGenerator(initialCfg.OpenAPI)

	applyConfig(initialCfg, rt, upstreamMgr, breakerMgr, rlMgr, authMgr, bl, m, apiGen)

	cfgMgr.OnChange(func(cfg *config.Config) {
		zapLogger.Info("applying new configuration")
		applyConfig(cfg, rt, upstreamMgr, breakerMgr, rlMgr, authMgr, bl, m, apiGen)
	})

	upstreamMgr.StartHealthChecks()

	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())

	gw := middleware.NewGateway(cfgMgr, rt, upstreamMgr, breakerMgr, rlMgr, authMgr, bl, m, st, px, zapLogger)

	engine.Use(middleware.RequestID())
	engine.Use(gw.IPBlacklist())
	engine.Use(gw.Tracing())

	engine.GET("/health", healthHandler(upstreamMgr))
	engine.GET("/metrics", gin.WrapH(promhttp.Handler()))

	engine.GET("/openapi", openAPIListHandler(apiGen, rt))
	engine.GET("/openapi/:upstream", openAPIHandler(apiGen, rt))

	engine.GET("/stats", statsHandler(st, m))
	engine.GET("/stats/slow", slowRequestsHandler(st))
	engine.GET("/stats/qps", qpsHandler(m))

	engine.NoRoute(gw.ProxyHandler())

	port := initialCfg.Server.Port
	if p := os.Getenv("PORT"); p != "" {
		fmt.Sscanf(p, "%d", &port)
	}

	zapLogger.Info(fmt.Sprintf("starting API gateway on :%d", port))
	if err := engine.Run(fmt.Sprintf(":%d", port)); err != nil {
		zapLogger.Fatal("failed to start server", zap.Error(err))
		os.Exit(1)
	}
}

func applyConfig(
	cfg *config.Config,
	rt *router.Router,
	upstreamMgr *upstreamPkg.Manager,
	breakerMgr *breaker.Manager,
	rlMgr *ratelimit.Manager,
	authMgr *auth.Manager,
	bl *ipblacklist.IPBlacklist,
	m *metrics.Metrics,
	apiGen *openapi.Generator,
) {
	for _, u := range cfg.Upstreams {
		hc := upstreamPkg.HealthCheckConfig{
			Enabled:  u.HealthCheck.Enabled,
			Interval: u.HealthCheck.Interval,
		}
		dryRun := u.DryRun
		upstreamMgr.Register(u.Name, u.Target, dryRun, hc, u.BodySizeLimit, u.Retry)
		m.EnsureUpstream(u.Name)
	}

	for _, route := range cfg.Routes {
		breakerMgr.Update(route.Upstream, route.Breaker.Threshold, route.Breaker.Timeout)
		rlMgr.Update(route.Upstream, route.RateLimit.RPS, route.RateLimit.MaxConcurrent, route.RateLimit.Mode)

		authMgr.RebuildFromConfig(route.Upstream, route.AuthHooks)
	}

	if len(cfg.IPBlacklist) > 0 {
		_ = bl.Load(cfg.IPBlacklist)
	}

	rt.Load(cfg.Routes)
}

func healthHandler(upstreamMgr *upstreamPkg.Manager) gin.HandlerFunc {
	return func(c *gin.Context) {
		allHealthy, statuses := upstreamMgr.AllHealthy()
		if allHealthy {
			c.JSON(http.StatusOK, gin.H{
				"status":    "healthy",
				"upstreams": statuses,
			})
			return
		}
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":    "unhealthy",
			"upstreams": statuses,
		})
	}
}

func openAPIListHandler(apiGen *openapi.Generator, rt *router.Router) gin.HandlerFunc {
	return func(c *gin.Context) {
		routes := rt.Routes()
		docs := apiGen.Generate(routes)
		list := openapi.UpstreamList(docs)
		c.JSON(http.StatusOK, gin.H{
			"upstreams": list,
			"hint":      "GET /openapi/:upstream for specific docs",
		})
	}
}

func openAPIHandler(apiGen *openapi.Generator, rt *router.Router) gin.HandlerFunc {
	return func(c *gin.Context) {
		upstream := c.Param("upstream")
		routes := rt.Routes()
		docs := apiGen.Generate(routes)
		if err := openapi.WriteDocs(docs, c.Writer, upstream); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
	}
}

func statsHandler(st *stats.Stats, m *metrics.Metrics) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"total_requests":     st.GetTotalRequests(),
			"error_counts":       st.GetErrorCounts(),
			"breaker_triggers":   st.GetBreakerTriggers(),
			"rate_limit_rejects": st.GetRateLimitRejects(),
		})
	}
}

func slowRequestsHandler(st *stats.Stats) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"slow_requests": st.GetSlowRequests(),
		})
	}
}

func qpsHandler(m *metrics.Metrics) gin.HandlerFunc {
	return func(c *gin.Context) {
		granularity := strings.ToLower(c.DefaultQuery("granularity", "minute"))
		if granularity != "minute" && granularity != "hour" && granularity != "day" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "granularity must be minute, hour, or day"})
			return
		}
		ranking := m.GetQPSRanking(granularity)
		c.JSON(http.StatusOK, gin.H{
			"granularity": granularity,
			"ranking":     ranking,
		})
	}
}

var _ = fmt.Sprintf
var _ = strings.ToLower
var _ = zap.L
