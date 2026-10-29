package mlrank

import (
	"log/slog"
	"net/http"
	"time"

	"tankard_workdir/internal/ml"

	"github.com/gin-gonic/gin"
)

// New возвращает обработчик POST /ml/rank — заглушку ранжировщика для проверки сценариев деградации рекомендаций. Режим выбирается заголовком X-ML-Mode или параметром ?mode=: normal (по умолчанию), slow (задержка) или fail (ошибка 500).
func New(log *slog.Logger, slowDelay time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req ml.RankRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json: " + err.Error()})
			return
		}

		mode := c.GetHeader(ml.ModeHeader)
		if mode == "" {
			mode = c.DefaultQuery("mode", ml.ModeNormal)
		}

		switch mode {
		case ml.ModeFail:
			log.Warn("ml stub forced failure", "user_id", req.UserID, "surface", req.Surface)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "ml stub failure"})
			return
		case ml.ModeSlow:
			timer := time.NewTimer(slowDelay)
			defer timer.Stop()

			select {
			case <-c.Request.Context().Done():
				c.JSON(http.StatusGatewayTimeout, gin.H{"error": "ml stub timed out"})
				return
			case <-timer.C:
			}
		}

		c.JSON(http.StatusOK, ml.RankResponse{
			RankedProductIDs: ml.Rank(req.UserID, req.CandidateIDs),
		})
	}
}
