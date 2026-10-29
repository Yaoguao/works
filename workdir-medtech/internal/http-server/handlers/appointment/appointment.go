package appointment

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"medtech-practice/internal/models"
	"medtech-practice/internal/services"
)

// Creator — интерфейс сервисного слоя для создания записи.
type Creator interface {
	CreateAppointment(ctx context.Context, req services.CreateAppointmentRequest) (*models.Appointment, bool, error)
}

type createRequest struct {
	ClinicID       int64  `json:"clinic_id"`
	PatientID      int64  `json:"patient_id"`
	ProviderID     int64  `json:"provider_id"`
	SlotID         int64  `json:"slot_id"`
	IdempotencyKey string `json:"idempotency_key"`
	HoldToken      string `json:"hold_token"`
}

type appointmentResponse struct {
	ID             int64     `json:"id"`
	ClinicID       int64     `json:"clinic_id"`
	PatientID      int64     `json:"patient_id"`
	ProviderID     int64     `json:"provider_id"`
	SlotID         int64     `json:"slot_id"`
	Status         string    `json:"status"`
	StartsAt       time.Time `json:"starts_at"`
	EndsAt         time.Time `json:"ends_at"`
	IdempotencyKey *string   `json:"idempotency_key"`
	CreatedAt      time.Time `json:"created_at"`
}

func toResponse(a *models.Appointment) appointmentResponse {
	return appointmentResponse{
		ID:             a.ID,
		ClinicID:       a.ClinicID,
		PatientID:      a.PatientID,
		ProviderID:     a.ProviderID,
		SlotID:         a.SlotID,
		Status:         a.Status,
		StartsAt:       a.StartsAt,
		EndsAt:         a.EndsAt,
		IdempotencyKey: a.IdempotencyKey,
		CreatedAt:      a.CreatedAt,
	}
}

// New возвращает gin-хендлер POST /v1/appointments.
// 201 — запись создана; 200 — идемпотентный повтор (та же запись);
// 409 — слот занят или удержан чужим hold'ом.
func New(creator Creator, log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		const op = "handlers.appointment.New"

		var req createRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
			return
		}
		switch {
		case req.ClinicID <= 0, req.PatientID <= 0, req.ProviderID <= 0, req.SlotID <= 0:
			c.JSON(http.StatusBadRequest, gin.H{"error": "clinic_id, patient_id, provider_id and slot_id are required"})
			return
		case req.IdempotencyKey == "":
			c.JSON(http.StatusBadRequest, gin.H{"error": "idempotency_key is required"})
			return
		}

		apt, created, err := creator.CreateAppointment(c.Request.Context(), services.CreateAppointmentRequest{
			ClinicID:       req.ClinicID,
			PatientID:      req.PatientID,
			ProviderID:     req.ProviderID,
			SlotID:         req.SlotID,
			IdempotencyKey: req.IdempotencyKey,
			HoldToken:      req.HoldToken,
		})
		switch {
		case err == nil && created:
			c.JSON(http.StatusCreated, toResponse(apt))
		case err == nil:
			c.Header("X-Idempotent-Replay", "true")
			c.JSON(http.StatusOK, toResponse(apt))
		case errors.Is(err, services.ErrSlotNotFound):
			c.JSON(http.StatusNotFound, gin.H{"error": "slot not found"})
		case errors.Is(err, services.ErrSlotNotAvailable):
			c.JSON(http.StatusConflict, gin.H{"error": "slot is not available"})
		case errors.Is(err, services.ErrHoldConflict):
			c.JSON(http.StatusConflict, gin.H{"error": "slot is held by another token"})
		default:
			log.ErrorContext(c.Request.Context(), "create appointment failed",
				slog.String("op", op), slog.Any("request", req), slog.Any("error", err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		}
	}
}

// Racer — интерфейс сервисного слоя для демо-гонки.
type Racer interface {
	RaceDemo(ctx context.Context, slotID int64, attempts int) (*services.RaceDemoResult, error)
}

type raceDemoRequest struct {
	SlotID   int64 `json:"slot_id"`
	Attempts int   `json:"attempts"`
}

// NewRaceDemo возвращает gin-хендлер POST /v1/appointments/race-demo:
// параллельно attempts горутин бронируют один слот, ожидание — ровно 1 success.
// Тело опционально: {"slot_id": 5004, "attempts": 20}.
func NewRaceDemo(racer Racer, log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req raceDemoRequest
		if err := c.ShouldBindJSON(&req); err != nil && !errors.Is(err, io.EOF) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
			return
		}
		if req.SlotID <= 0 {
			req.SlotID = 5004
		}
		if req.Attempts <= 0 {
			req.Attempts = services.RaceDemoAttempts
		}

		res, err := racer.RaceDemo(c.Request.Context(), req.SlotID, req.Attempts)
		switch {
		case err == nil:
			c.JSON(http.StatusOK, res)
		case errors.Is(err, services.ErrSlotNotFound):
			c.JSON(http.StatusNotFound, gin.H{"error": "slot not found"})
		case errors.Is(err, services.ErrSlotNotAvailable):
			c.JSON(http.StatusConflict, gin.H{
				"error":   "slot is already booked; re-seed the DB or pick another free slot",
				"slot_id": req.SlotID,
			})
		default:
			log.ErrorContext(c.Request.Context(), "race demo failed",
				slog.String("op", "handlers.appointment.NewRaceDemo"),
				slog.Int64("slot_id", req.SlotID), slog.Any("error", err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		}
	}
}
