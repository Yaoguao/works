package recommendations

import (
	"log/slog"
	"net/http"
	"strconv"

	"tankard_workdir/internal/ml"
	"tankard_workdir/internal/service"

	"github.com/gin-gonic/gin"
)

// New возвращает обработчик GET /v1/recommendations. Он формирует рекомендации в пределах заданного бюджета задержки и всегда отвечает кодом 200 благодаря цепочке кэша и резервных стратегий.
func New(log *slog.Logger, svc *service.RecommendationService) gin.HandlerFunc {
	return func(c *gin.Context) {
		userID, err := strconv.ParseInt(c.Query("user_id"), 10, 64)
		if err != nil || userID <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "user_id must be a positive integer"})
			return
		}

		surface := c.DefaultQuery("surface", "home")

		limit := 0
		if raw := c.Query("limit"); raw != "" {
			parsed, parseErr := strconv.Atoi(raw)
			if parseErr != nil || parsed <= 0 {
				c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be a positive integer"})
				return
			}
			limit = parsed
		}

		mode := c.GetHeader(ml.ModeHeader)
		if mode == "" {
			mode = c.Query("ml_mode")
		}

		result, err := svc.Recommend(c.Request.Context(), service.RecommendParams{
			UserID:  userID,
			Surface: surface,
			Limit:   limit,
			MLMode:  mode,
		})
		if err != nil {
			log.Error("failed to resolve recommendations", "user_id", userID, "surface", surface, "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to resolve recommendations"})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"request_id":   result.RequestID,
			"user_id":      result.UserID,
			"surface":      result.Surface,
			"strategy_key": result.StrategyKey,
			"experiment":   result.Experiment,
			"source":       result.Source,
			"latency_ms":   result.LatencyMs,
			"items":        result.Items,
		})
	}
}
