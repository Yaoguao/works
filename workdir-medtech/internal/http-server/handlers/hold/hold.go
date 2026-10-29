package hold

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"medtech-practice/internal/services"
)

const (
	defaultTTLSeconds = 120
	maxTTLSeconds     = 900
)

// Holder — интерфейс сервисного слоя для hold'а слота.
type Holder interface {
	Hold(ctx context.Context, slotID int64, token string, ttl time.Duration) error
}

type request struct {
	Token      string `json:"token"`
	TTLSeconds int    `json:"ttl_seconds"`
}

// New возвращает gin-хендлер POST /v1/slots/:id/hold.
func New(holder Holder, log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		const op = "handlers.hold.New"

		slotID, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid slot id"})
			return
		}

		var req request
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
			return
		}
		if req.Token == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "token is required"})
			return
		}

		ttlSeconds := req.TTLSeconds
		if ttlSeconds <= 0 {
			ttlSeconds = defaultTTLSeconds
		}
		if ttlSeconds > maxTTLSeconds {
			ttlSeconds = maxTTLSeconds
		}

		err = holder.Hold(c.Request.Context(), slotID, req.Token, time.Duration(ttlSeconds)*time.Second)
		switch {
		case err == nil:
			c.JSON(http.StatusOK, gin.H{
				"slot_id":     slotID,
				"token":       req.Token,
				"ttl_seconds": ttlSeconds,
				"hold_key":    "slot:hold:" + strconv.FormatInt(slotID, 10),
			})
		case errors.Is(err, services.ErrSlotNotFound):
			c.JSON(http.StatusNotFound, gin.H{"error": "slot not found"})
		case errors.Is(err, services.ErrSlotNotAvailable):
			c.JSON(http.StatusConflict, gin.H{"error": "slot is booked"})
		case errors.Is(err, services.ErrHoldConflict):
			c.JSON(http.StatusConflict, gin.H{"error": "slot is held by another token"})
		default:
			log.ErrorContext(c.Request.Context(), "hold failed",
				slog.String("op", op), slog.Int64("slot_id", slotID), slog.Any("error", err))
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "hold storage unavailable"})
		}
	}
}
