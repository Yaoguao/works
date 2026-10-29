package ml

import (
	"log/slog"
	"math"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// ModelVersion — версия стаб-модели ноу-шоу скоринга.
const ModelVersion = "stub-xgb-v1"

// scoreRequest — вход скоринга (то, что реальный ML-сервис получал бы от backend).
type scoreRequest struct {
	PatientID     int64     `json:"patient_id"`
	ProviderID    int64     `json:"provider_id"`
	StartsAt      time.Time `json:"starts_at"`
	LeadTimeHours float64   `json:"lead_time_hours"`
}

// scoreResponse — выход скоринга.
type scoreResponse struct {
	Score        float64 `json:"score"`
	ModelVersion string  `json:"model_version"`
}

// NewScore возвращает gin-хендлер POST /ml/no-show/score — заглушку внешнего ML-сервиса.
//
// Эмуляция отказов (для проверки, что запись пациента не ломается при падении ML):
//   - ?fail=1            -> 503 (сервис недоступен)
//   - заголовок X-ML-Fail: 1 -> 503
//   - ?delay=5s          -> задержка перед ответом (эмуляция timeout)
func NewScore(log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		const op = "handlers.ml.NewScore"

		if d, err := time.ParseDuration(c.Query("delay")); err == nil && d > 0 {
			time.Sleep(d)
		}

		var req scoreRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
			return
		}

		if c.Query("fail") == "1" || c.GetHeader("X-ML-Fail") == "1" {
			log.WarnContext(c.Request.Context(), "ml stub simulated failure",
				slog.String("op", op), slog.Int64("patient_id", req.PatientID))
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "ml unavailable (simulated)"})
			return
		}

		c.JSON(http.StatusOK, scoreResponse{
			Score:        stubScore(req),
			ModelVersion: ModelVersion,
		})
	}
}

// stubScore — детерминированная заглушка: чем больше lead time, тем выше риск неявки.
func stubScore(req scoreRequest) float64 {
	base := 0.15
	if req.LeadTimeHours > 72 {
		base += 0.35
	}
	base += float64((req.PatientID+req.ProviderID)%5) * 0.07
	return math.Round(math.Min(base, 0.98)*1e4) / 1e4
}
