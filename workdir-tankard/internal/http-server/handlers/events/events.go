package events

import (
	"errors"
	"log/slog"
	"net/http"
	"time"

	"tankard_workdir/internal/service"

	"github.com/gin-gonic/gin"
)

type request struct {
	EventID        string    `json:"event_id"`
	UserExternalID string    `json:"user_external_id"`
	EventType      string    `json:"event_type"`
	ProductID      int64     `json:"product_id"`
	TS             time.Time `json:"ts"`
	Surface        string    `json:"surface"`
	ExperimentID   int64     `json:"experiment_id"`
	StrategyKey    string    `json:"strategy_key"`
}

// New возвращает обработчик POST /v1/events. Он проверяет данные и публикует событие в Kafka, отвечая кодом 202 Accepted без обновления профиля в рамках запроса.
func New(log *slog.Logger, svc *service.EventService) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req request
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json: " + err.Error()})
			return
		}

		event := service.Event{
			EventID:        req.EventID,
			UserExternalID: req.UserExternalID,
			EventType:      req.EventType,
			ProductID:      req.ProductID,
			TS:             req.TS,
			Surface:        req.Surface,
			ExperimentID:   req.ExperimentID,
			StrategyKey:    req.StrategyKey,
		}

		if err := svc.Publish(c.Request.Context(), event); err != nil {
			if isValidationError(err) {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}

			log.Error("failed to publish event", "event_id", req.EventID, "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to accept event"})
			return
		}

		c.JSON(http.StatusAccepted, gin.H{"status": "accepted", "event_id": req.EventID})
	}
}

func isValidationError(err error) bool {
	return errors.Is(err, service.ErrMissingEventID) ||
		errors.Is(err, service.ErrMissingUserID) ||
		errors.Is(err, service.ErrMissingEventType) ||
		errors.Is(err, service.ErrUnknownEventType)
}
