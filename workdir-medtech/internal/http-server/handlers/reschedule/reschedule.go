package reschedule

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"medtech-practice/internal/models"
	"medtech-practice/internal/services"
)

// Proposer — сервисный слой механизма переноса записи.
type Proposer interface {
	CreateProposal(ctx context.Context, appointmentID, proposedSlotID, proposedBy int64) (*models.RescheduleProposal, error)
	AcceptProposal(ctx context.Context, proposalID int64) (*models.Appointment, error)
	DeclineProposal(ctx context.Context, proposalID int64) (*models.RescheduleProposal, error)
}

type createRequest struct {
	ProposedSlotID int64 `json:"proposed_slot_id"`
	ProposedBy     int64 `json:"proposed_by"`
}

type proposalResponse struct {
	ID             int64     `json:"id"`
	AppointmentID  int64     `json:"appointment_id"`
	OldSlotID      int64     `json:"old_slot_id"`
	ProposedSlotID int64     `json:"proposed_slot_id"`
	Status         string    `json:"status"`
	ExpiresAt      time.Time `json:"expires_at"`
	CreatedAt      time.Time `json:"created_at"`
}

func toProposalResponse(p *models.RescheduleProposal) proposalResponse {
	return proposalResponse{
		ID:             p.ID,
		AppointmentID:  p.AppointmentID,
		OldSlotID:      p.OldSlotID,
		ProposedSlotID: p.ProposedSlotID,
		Status:         p.Status,
		ExpiresAt:      p.ExpiresAt,
		CreatedAt:      p.CreatedAt,
	}
}

type appointmentResponse struct {
	ID        int64     `json:"id"`
	SlotID    int64     `json:"slot_id"`
	Status    string    `json:"status"`
	StartsAt  time.Time `json:"starts_at"`
	EndsAt    time.Time `json:"ends_at"`
	Version   int32     `json:"version"`
	UpdatedAt time.Time `json:"updated_at"`
}

// NewCreateProposal — gin-хендлер POST /v1/appointments/:id/reschedule-proposals.
// Врач предлагает перенос: новый слот уходит в held, создаётся pending-proposal.
func NewCreateProposal(proposer Proposer, log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		const op = "handlers.reschedule.NewCreateProposal"

		appointmentID, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil || appointmentID <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid appointment id"})
			return
		}

		var req createRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body: " + err.Error()})
			return
		}
		if req.ProposedSlotID <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "proposed_slot_id is required"})
			return
		}

		p, err := proposer.CreateProposal(c.Request.Context(), appointmentID, req.ProposedSlotID, req.ProposedBy)
		switch {
		case err == nil:
			c.JSON(http.StatusCreated, toProposalResponse(p))
		case errors.Is(err, services.ErrAppointmentNotFound):
			c.JSON(http.StatusNotFound, gin.H{"error": "appointment not found"})
		case errors.Is(err, services.ErrSlotNotFound):
			c.JSON(http.StatusNotFound, gin.H{"error": "proposed slot not found"})
		case errors.Is(err, services.ErrAppointmentNotActive):
			c.JSON(http.StatusConflict, gin.H{"error": "appointment is not active"})
		case errors.Is(err, services.ErrSlotNotAvailable):
			c.JSON(http.StatusConflict, gin.H{"error": "proposed slot is not available"})
		case errors.Is(err, services.ErrProposalExists):
			c.JSON(http.StatusConflict, gin.H{"error": "pending proposal already exists for appointment"})
		default:
			log.ErrorContext(c.Request.Context(), "create proposal failed",
				slog.String("op", op), slog.Int64("appointment_id", appointmentID), slog.Any("error", err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		}
	}
}

// NewAcceptProposal — gin-хендлер POST /v1/reschedule-proposals/:id/accept.
// Пациент принимает перенос: слоты меняются местами, запись обновляется.
func NewAcceptProposal(proposer Proposer, log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		const op = "handlers.reschedule.NewAcceptProposal"

		proposalID, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil || proposalID <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid proposal id"})
			return
		}

		apt, err := proposer.AcceptProposal(c.Request.Context(), proposalID)
		switch {
		case err == nil:
			c.JSON(http.StatusOK, appointmentResponse{
				ID: apt.ID, SlotID: apt.SlotID, Status: apt.Status,
				StartsAt: apt.StartsAt, EndsAt: apt.EndsAt,
				Version: apt.Version, UpdatedAt: apt.UpdatedAt,
			})
		case errors.Is(err, services.ErrProposalNotFound):
			c.JSON(http.StatusNotFound, gin.H{"error": "proposal not found"})
		case errors.Is(err, services.ErrAppointmentNotFound):
			c.JSON(http.StatusNotFound, gin.H{"error": "appointment not found"})
		case errors.Is(err, services.ErrProposalNotPending):
			c.JSON(http.StatusConflict, gin.H{"error": "proposal is not pending"})
		case errors.Is(err, services.ErrProposalExpired):
			c.JSON(http.StatusConflict, gin.H{"error": "proposal expired"})
		case errors.Is(err, services.ErrProposalConflict):
			c.JSON(http.StatusConflict, gin.H{"error": "proposed slot is no longer available"})
		default:
			log.ErrorContext(c.Request.Context(), "accept proposal failed",
				slog.String("op", op), slog.Int64("proposal_id", proposalID), slog.Any("error", err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		}
	}
}

// NewDeclineProposal — gin-хендлер POST /v1/reschedule-proposals/:id/decline.
// Пациент отклоняет перенос: hold нового слота снимается.
func NewDeclineProposal(proposer Proposer, log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		const op = "handlers.reschedule.NewDeclineProposal"

		proposalID, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil || proposalID <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid proposal id"})
			return
		}

		p, err := proposer.DeclineProposal(c.Request.Context(), proposalID)
		switch {
		case err == nil:
			c.JSON(http.StatusOK, toProposalResponse(p))
		case errors.Is(err, services.ErrProposalNotFound):
			c.JSON(http.StatusNotFound, gin.H{"error": "proposal not found"})
		case errors.Is(err, services.ErrProposalNotPending):
			c.JSON(http.StatusConflict, gin.H{"error": "proposal is not pending"})
		default:
			log.ErrorContext(c.Request.Context(), "decline proposal failed",
				slog.String("op", op), slog.Int64("proposal_id", proposalID), slog.Any("error", err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		}
	}
}
