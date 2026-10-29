package services

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	pgx_driver "medtech-practice/lib/pgx-driver"
	"medtech-practice/lib/pgx-driver/transaction"

	"medtech-practice/internal/models"
	"medtech-practice/internal/storage"
)

var (
	ErrAppointmentNotFound  = errors.New("appointment not found")
	ErrAppointmentNotActive = errors.New("appointment is not active")
	ErrProposalNotFound     = errors.New("proposal not found")
	ErrProposalNotPending   = errors.New("proposal is not pending")
	ErrProposalExpired      = errors.New("proposal expired")
	ErrProposalExists       = errors.New("pending proposal already exists for appointment")
	ErrProposalConflict     = errors.New("proposed slot is no longer available")
)

const (
	// ProposalStatus* — статусная модель предложения переноса.
	ProposalStatusPending  = "pending"
	ProposalStatusAccepted = "accepted"
	ProposalStatusDeclined = "declined"
	ProposalStatusExpired  = "expired"

	// AppointmentStatusRescheduled — статус записи после успешного переноса.
	AppointmentStatusRescheduled = "rescheduled"

	// ProposalTTL — сколько живёт предложение до истечения.
	ProposalTTL = 15 * time.Minute

	// EventRescheduleProposed — событие outbox о создании предложения.
	EventRescheduleProposed = "reschedule.proposed"
	// EventAppointmentRescheduled — событие outbox о состоявшемся переносе.
	EventAppointmentRescheduled = "appointment.rescheduled"
)

// ProposalService реализует механизм переноса записи: врач предлагает новый слот,
// пациент принимает/отклоняет, истёкшие предложения закрывает воркер.
type ProposalService struct {
	pg        *pgx_driver.Postgres
	txManager transaction.Manager
	storage   *storage.ProposalStorage
	appt      *storage.AppointmentStorage
	log       *slog.Logger
}

func NewProposalService(
	pg *pgx_driver.Postgres,
	txManager transaction.Manager,
	st *storage.ProposalStorage,
	appt *storage.AppointmentStorage,
	log *slog.Logger,
) *ProposalService {
	return &ProposalService{pg: pg, txManager: txManager, storage: st, appt: appt, log: log}
}

// CreateProposal создаёт предложение переноса: ставит hold на новый слот,
// создаёт pending-proposal с TTL 15 минут. Старый слот не трогаем до accept.
func (s *ProposalService) CreateProposal(
	ctx context.Context,
	appointmentID int64,
	proposedSlotID int64,
	proposedBy int64,
) (*models.RescheduleProposal, error) {
	const op = "services.proposal.CreateProposal"

	var proposal *models.RescheduleProposal

	err := s.txManager.ExecuteInTransaction(ctx, "proposal.create", func(qe pgx_driver.QueryExecuter) error {
		apt, err := s.storage.LockAppointment(ctx, qe, appointmentID)
		if errors.Is(err, storage.ErrNotFound) {
			return ErrAppointmentNotFound
		}
		if err != nil {
			return err
		}
		if apt.Status != AppointmentStatusConfirmed && apt.Status != AppointmentStatusRescheduled {
			return ErrAppointmentNotActive
		}
		if apt.SlotID == proposedSlotID {
			return ErrSlotNotAvailable
		}

		slot, err := s.storage.LockSlot(ctx, qe, proposedSlotID)
		if errors.Is(err, storage.ErrNotFound) {
			return ErrSlotNotFound
		}
		if err != nil {
			return err
		}
		if slot.Status != "free" {
			return ErrSlotNotAvailable
		}

		expiresAt := time.Now().Add(ProposalTTL)
		p := &models.RescheduleProposal{
			AppointmentID:  appointmentID,
			OldSlotID:      apt.SlotID,
			ProposedSlotID: proposedSlotID,
			Status:         ProposalStatusPending,
			ProposedBy:     optionalID(proposedBy),
			ExpiresAt:      expiresAt,
		}
		if _, err := s.storage.InsertProposal(ctx, qe, p); err != nil {
			return err
		}

		holdToken := strconv.FormatInt(p.ID, 10)
		ok, err := s.storage.HoldSlot(ctx, qe, proposedSlotID, holdToken, expiresAt)
		if err != nil {
			return err
		}
		if !ok {
			return ErrSlotNotAvailable
		}

		payload := map[string]any{
			"proposal_id":      p.ID,
			"appointment_id":   appointmentID,
			"old_slot_id":      apt.SlotID,
			"proposed_slot_id": proposedSlotID,
			"expires_at":       expiresAt.Format(time.RFC3339),
		}
		if err := s.appt.InsertOutboxEvent(ctx, qe, EventRescheduleProposed, "reschedule_proposal", p.ID, payload); err != nil {
			return err
		}

		proposal = p
		return nil
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "uq_one_pending_proposal" {
			return nil, ErrProposalExists
		}
		return nil, err
	}

	s.log.InfoContext(ctx, "reschedule proposal created",
		slog.String("op", op),
		slog.Int64("proposal_id", proposal.ID),
		slog.Int64("appointment_id", appointmentID),
		slog.Int64("proposed_slot_id", proposedSlotID),
	)
	return proposal, nil
}

// AcceptProposal применяет перенос в одной транзакции:
// новый слот -> booked, старый -> free, appointment -> новый слот (+version),
// proposal -> accepted, пишется история и outbox appointment.rescheduled.
func (s *ProposalService) AcceptProposal(ctx context.Context, proposalID int64) (*models.Appointment, error) {
	const op = "services.proposal.AcceptProposal"

	var rescheduled *models.Appointment

	err := s.txManager.ExecuteInTransaction(ctx, "proposal.accept", func(qe pgx_driver.QueryExecuter) error {
		p, err := s.storage.LockProposal(ctx, qe, proposalID)
		if errors.Is(err, storage.ErrNotFound) {
			return ErrProposalNotFound
		}
		if err != nil {
			return err
		}
		if p.Status != ProposalStatusPending {
			return ErrProposalNotPending
		}
		if !p.ExpiresAt.After(time.Now()) {
			return ErrProposalExpired
		}

		apt, err := s.storage.LockAppointment(ctx, qe, p.AppointmentID)
		if errors.Is(err, storage.ErrNotFound) {
			return ErrAppointmentNotFound
		}
		if err != nil {
			return err
		}

		holdToken := strconv.FormatInt(p.ID, 10)
		newSlot, err := s.storage.BookHeldSlot(ctx, qe, p.ProposedSlotID, holdToken)
		if err != nil {
			return err
		}
		if newSlot == nil {
			return ErrProposalConflict
		}

		updated, err := s.storage.RescheduleAppointment(
			ctx, qe, p.AppointmentID, newSlot.ID, newSlot.StartsAt, newSlot.EndsAt, apt.Version,
		)
		if err != nil {
			return err
		}
		if updated == nil {
			return ErrProposalConflict
		}

		if _, err := s.storage.ReleaseSlot(ctx, qe, p.OldSlotID); err != nil {
			return err
		}

		if _, err := s.storage.CloseProposal(ctx, qe, p.ID, ProposalStatusAccepted, time.Now()); err != nil {
			return err
		}

		fromStatus := apt.Status
		if err := s.appt.InsertStatusHistory(ctx, qe, p.AppointmentID, &fromStatus, AppointmentStatusRescheduled, "reschedule accepted"); err != nil {
			return err
		}

		payload := map[string]any{
			"proposal_id":    p.ID,
			"appointment_id": p.AppointmentID,
			"old_slot_id":    p.OldSlotID,
			"new_slot_id":    newSlot.ID,
			"starts_at":      newSlot.StartsAt.Format(time.RFC3339),
			"ends_at":        newSlot.EndsAt.Format(time.RFC3339),
		}
		if err := s.appt.InsertOutboxEvent(ctx, qe, EventAppointmentRescheduled, "appointment", p.AppointmentID, payload); err != nil {
			return err
		}

		rescheduled = updated
		return nil
	})
	if err != nil {
		return nil, err
	}

	s.log.InfoContext(ctx, "reschedule proposal accepted",
		slog.String("op", op),
		slog.Int64("proposal_id", proposalID),
		slog.Int64("appointment_id", rescheduled.ID),
	)
	return rescheduled, nil
}

// DeclineProposal отклоняет предложение и снимает hold с нового слота.
func (s *ProposalService) DeclineProposal(ctx context.Context, proposalID int64) (*models.RescheduleProposal, error) {
	const op = "services.proposal.DeclineProposal"

	var declined *models.RescheduleProposal

	err := s.txManager.ExecuteInTransaction(ctx, "proposal.decline", func(qe pgx_driver.QueryExecuter) error {
		p, err := s.storage.LockProposal(ctx, qe, proposalID)
		if errors.Is(err, storage.ErrNotFound) {
			return ErrProposalNotFound
		}
		if err != nil {
			return err
		}
		if p.Status != ProposalStatusPending {
			return ErrProposalNotPending
		}

		holdToken := strconv.FormatInt(p.ID, 10)
		if _, err := s.storage.ReleaseHeldSlot(ctx, qe, p.ProposedSlotID, holdToken); err != nil {
			return err
		}

		decidedAt := time.Now()
		ok, err := s.storage.CloseProposal(ctx, qe, p.ID, ProposalStatusDeclined, decidedAt)
		if err != nil {
			return err
		}
		if !ok {
			return ErrProposalNotPending
		}

		p.Status = ProposalStatusDeclined
		p.DecidedAt = &decidedAt
		declined = p
		return nil
	})
	if err != nil {
		return nil, err
	}

	s.log.InfoContext(ctx, "reschedule proposal declined",
		slog.String("op", op), slog.Int64("proposal_id", proposalID))
	return declined, nil
}

// ExpireProposals закрывает просроченные pending-proposal и снимает hold со слотов.
// Возвращает число истёкших предложений (для логов/метрик воркера).
func (s *ProposalService) ExpireProposals(ctx context.Context) (int, error) {
	const op = "services.proposal.ExpireProposals"

	var count int

	err := s.txManager.ExecuteInTransaction(ctx, "proposal.expire", func(qe pgx_driver.QueryExecuter) error {
		expired, err := s.storage.ExpirePending(ctx, qe, time.Now())
		if err != nil {
			return err
		}

		for _, e := range expired {
			holdToken := strconv.FormatInt(e.ID, 10)
			if _, err := s.storage.ReleaseHeldSlot(ctx, qe, e.ProposedSlotID, holdToken); err != nil {
				return err
			}
		}

		count = len(expired)
		return nil
	})
	if err != nil {
		return 0, err
	}

	if count > 0 {
		s.log.InfoContext(ctx, "reschedule proposals expired",
			slog.String("op", op), slog.Int("count", count))
	}
	return count, nil
}

// optionalID возвращает nil для нулевого id (неизвестный автор).
func optionalID(id int64) *int64 {
	if id <= 0 {
		return nil
	}
	return &id
}
