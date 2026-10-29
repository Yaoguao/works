package http_server

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"medtech-practice/internal/http-server/handlers/analytics"
	"medtech-practice/internal/http-server/handlers/appointment"
	"medtech-practice/internal/http-server/handlers/document"
	"medtech-practice/internal/http-server/handlers/hold"
	"medtech-practice/internal/http-server/handlers/ml"
	"medtech-practice/internal/http-server/handlers/reschedule"
	"medtech-practice/internal/http-server/handlers/stats"
	"medtech-practice/internal/services"
)

func New(
	env string,
	log *slog.Logger,
	booking *services.BookingService,
	documents *services.DocumentsService,
	analyticsSvc *services.AnalyticsService,
	proposalSvc *services.ProposalService,
) *gin.Engine {
	if env == "prod" {
		gin.SetMode(gin.ReleaseMode)
	}

	router := gin.New()
	router.Use(gin.Recovery(), requestLogger(log))

	router.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	router.POST("/v1/slots/:id/hold", hold.New(booking, log))
	router.POST("/v1/appointments", appointment.New(booking, log))
	router.POST("/v1/appointments/race-demo", appointment.NewRaceDemo(booking, log))
	router.GET("/v1/stats", stats.New(booking))

	router.POST("/v1/documents/upload", document.NewUpload(documents, log))
	router.GET("/v1/documents/:id/download-url", document.NewDownloadURL(documents))
	router.DELETE("/v1/documents/:id", document.NewDelete(documents))

	router.GET("/v1/analytics/no-show", analytics.NoShow(analyticsSvc))
	router.GET("/v1/analytics/reconcile", analytics.Reconcile(analyticsSvc))

	// ML stub — заглушка внешнего сервиса ноу-шоу скоринга.
	router.POST("/ml/no-show/score", ml.NewScore(log))

	// Механизм переноса записи (no-show / reschedule).
	router.POST("/v1/appointments/:id/reschedule-proposals", reschedule.NewCreateProposal(proposalSvc, log))
	router.POST("/v1/reschedule-proposals/:id/accept", reschedule.NewAcceptProposal(proposalSvc, log))
	router.POST("/v1/reschedule-proposals/:id/decline", reschedule.NewDeclineProposal(proposalSvc, log))

	return router
}

func requestLogger(log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		c.Next()

		log.InfoContext(c.Request.Context(), "http request",
			slog.String("method", c.Request.Method),
			slog.String("path", c.Request.URL.Path),
			slog.Int("status", c.Writer.Status()),
			slog.Duration("latency", time.Since(start)),
			slog.String("client_ip", c.ClientIP()),
		)
	}
}
