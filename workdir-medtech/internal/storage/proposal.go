package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"

	pgx_driver "medtech-practice/lib/pgx-driver"

	"medtech-practice/internal/models"
)

// ProposalStorage — доступ к данным механизма переноса записи:
// блокировки appointment/слота, hold/relase/book слота, proposal и optimistic lock.
type ProposalStorage struct {
	db *pgx_driver.Postgres
}

func NewProposalStorage(db *pgx_driver.Postgres) *ProposalStorage {
	return &ProposalStorage{db: db}
}

const proposalColumns = `id, appointment_id, old_slot_id, proposed_slot_id, status,
	proposed_by, expires_at, created_at, decided_at`

func scanProposal(row pgx.Row) (*models.RescheduleProposal, error) {
	var p models.RescheduleProposal
	err := row.Scan(
		&p.ID, &p.AppointmentID, &p.OldSlotID, &p.ProposedSlotID, &p.Status,
		&p.ProposedBy, &p.ExpiresAt, &p.CreatedAt, &p.DecidedAt,
	)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// LockAppointment читает запись с блокировкой строки (SELECT ... FOR UPDATE).
func (s *ProposalStorage) LockAppointment(
	ctx context.Context,
	qe pgx_driver.QueryExecuter,
	appointmentID int64,
) (*models.Appointment, error) {
	const op = "storage.proposal.LockAppointment"

	sql, args, err := s.db.Select(appointmentColumns).
		From("appointments").
		Where(squirrel.Eq{"id": appointmentID}).
		Suffix("FOR UPDATE").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("%s: build query: %w", op, err)
	}

	apt, err := scanAppointment(qe.QueryRow(ctx, sql, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("%s: execute query: %w", op, err)
	}

	return apt, nil
}

// LockSlot читает слот с блокировкой строки (SELECT ... FOR UPDATE).
func (s *ProposalStorage) LockSlot(
	ctx context.Context,
	qe pgx_driver.QueryExecuter,
	slotID int64,
) (*models.Slot, error) {
	const op = "storage.proposal.LockSlot"

	sql, args, err := s.db.Select(slotColumns).
		From("slots").
		Where(squirrel.Eq{"id": slotID}).
		Suffix("FOR UPDATE").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("%s: build query: %w", op, err)
	}

	slot, err := scanSlot(qe.QueryRow(ctx, sql, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("%s: execute query: %w", op, err)
	}

	return slot, nil
}

// HoldSlot переводит свободный слот в held с указанным hold_token (id proposal)
// и hold_until. Возвращает false, если слот уже не свободен (гонка).
func (s *ProposalStorage) HoldSlot(
	ctx context.Context,
	qe pgx_driver.QueryExecuter,
	slotID int64,
	holdToken string,
	holdUntil time.Time,
) (bool, error) {
	const op = "storage.proposal.HoldSlot"

	sql, args, err := s.db.Update("slots").
		Set("status", "held").
		Set("hold_token", holdToken).
		Set("hold_until", holdUntil).
		Where(squirrel.Eq{"id": slotID, "status": "free"}).
		ToSql()
	if err != nil {
		return false, fmt.Errorf("%s: build query: %w", op, err)
	}

	tag, err := qe.Exec(ctx, sql, args...)
	if err != nil {
		return false, fmt.Errorf("%s: execute query: %w", op, err)
	}

	return tag.RowsAffected() > 0, nil
}

// BookHeldSlot бронирует слот, удерживаемый данным proposal (hold_token = proposalID).
// Возвращает nil, если слот уже забронирован/удержан кем-то другим (конфликт гонки).
func (s *ProposalStorage) BookHeldSlot(
	ctx context.Context,
	qe pgx_driver.QueryExecuter,
	slotID int64,
	holdToken string,
) (*models.Slot, error) {
	const op = "storage.proposal.BookHeldSlot"

	sql, args, err := s.db.Update("slots").
		Set("status", "booked").
		Set("hold_token", nil).
		Set("hold_until", nil).
		Where(squirrel.Eq{"id": slotID, "status": "held", "hold_token": holdToken}).
		Suffix("RETURNING " + slotColumns).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("%s: build query: %w", op, err)
	}

	slot, err := scanSlot(qe.QueryRow(ctx, sql, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%s: execute query: %w", op, err)
	}

	return slot, nil
}

// ReleaseHeldSlot освобождает слот, удерживаемый данным proposal.
// Возвращает false, если слот уже не в том hold'е (освобождать нечего).
func (s *ProposalStorage) ReleaseHeldSlot(
	ctx context.Context,
	qe pgx_driver.QueryExecuter,
	slotID int64,
	holdToken string,
) (bool, error) {
	const op = "storage.proposal.ReleaseHeldSlot"

	sql, args, err := s.db.Update("slots").
		Set("status", "free").
		Set("hold_token", nil).
		Set("hold_until", nil).
		Where(squirrel.Eq{"id": slotID, "status": "held", "hold_token": holdToken}).
		ToSql()
	if err != nil {
		return false, fmt.Errorf("%s: build query: %w", op, err)
	}

	tag, err := qe.Exec(ctx, sql, args...)
	if err != nil {
		return false, fmt.Errorf("%s: execute query: %w", op, err)
	}

	return tag.RowsAffected() > 0, nil
}

// ReleaseSlot освобождает слот (held/booked -> free), снимая hold. Возвращает false,
// если слот уже свободен/в другом состоянии.
func (s *ProposalStorage) ReleaseSlot(
	ctx context.Context,
	qe pgx_driver.QueryExecuter,
	slotID int64,
) (bool, error) {
	const op = "storage.proposal.ReleaseSlot"

	sql, args, err := s.db.Update("slots").
		Set("status", "free").
		Set("hold_token", nil).
		Set("hold_until", nil).
		Where(squirrel.Eq{"id": slotID}).
		Where(squirrel.Eq{"status": []string{"held", "booked"}}).
		ToSql()
	if err != nil {
		return false, fmt.Errorf("%s: build query: %w", op, err)
	}

	tag, err := qe.Exec(ctx, sql, args...)
	if err != nil {
		return false, fmt.Errorf("%s: execute query: %w", op, err)
	}

	return tag.RowsAffected() > 0, nil
}

// InsertProposal создаёт предложение переноса в статусе pending и возвращает его id.
func (s *ProposalStorage) InsertProposal(
	ctx context.Context,
	qe pgx_driver.QueryExecuter,
	p *models.RescheduleProposal,
) (int64, error) {
	const op = "storage.proposal.InsertProposal"

	sql, args, err := s.db.Insert("reschedule_proposals").
		Columns("appointment_id", "old_slot_id", "proposed_slot_id", "status", "proposed_by", "expires_at").
		Values(p.AppointmentID, p.OldSlotID, p.ProposedSlotID, p.Status, p.ProposedBy, p.ExpiresAt).
		Suffix("RETURNING id, created_at").
		ToSql()
	if err != nil {
		return 0, fmt.Errorf("%s: build query: %w", op, err)
	}

	if err := qe.QueryRow(ctx, sql, args...).Scan(&p.ID, &p.CreatedAt); err != nil {
		return 0, fmt.Errorf("%s: execute query: %w", op, err)
	}

	return p.ID, nil
}

// LockProposal читает proposal с блокировкой строки.
func (s *ProposalStorage) LockProposal(
	ctx context.Context,
	qe pgx_driver.QueryExecuter,
	proposalID int64,
) (*models.RescheduleProposal, error) {
	const op = "storage.proposal.LockProposal"

	sql, args, err := s.db.Select(proposalColumns).
		From("reschedule_proposals").
		Where(squirrel.Eq{"id": proposalID}).
		Suffix("FOR UPDATE").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("%s: build query: %w", op, err)
	}

	p, err := scanProposal(qe.QueryRow(ctx, sql, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("%s: execute query: %w", op, err)
	}

	return p, nil
}

// CloseProposal переводит pending-proposal в терминальный статус (accepted/declined/expired).
// Возвращает false, если предложение уже не pending (гонка accept/decline/expire).
func (s *ProposalStorage) CloseProposal(
	ctx context.Context,
	qe pgx_driver.QueryExecuter,
	proposalID int64,
	status string,
	decidedAt time.Time,
) (bool, error) {
	const op = "storage.proposal.CloseProposal"

	sql, args, err := s.db.Update("reschedule_proposals").
		Set("status", status).
		Set("decided_at", decidedAt).
		Where(squirrel.Eq{"id": proposalID, "status": "pending"}).
		ToSql()
	if err != nil {
		return false, fmt.Errorf("%s: build query: %w", op, err)
	}

	tag, err := qe.Exec(ctx, sql, args...)
	if err != nil {
		return false, fmt.Errorf("%s: execute query: %w", op, err)
	}

	return tag.RowsAffected() > 0, nil
}

// RescheduleAppointment переводит запись на новый слот с optimistic lock по version.
// Возвращает nil, если версия изменилась (запись параллельно обновили).
func (s *ProposalStorage) RescheduleAppointment(
	ctx context.Context,
	qe pgx_driver.QueryExecuter,
	appointmentID int64,
	newSlotID int64,
	startsAt time.Time,
	endsAt time.Time,
	expectedVersion int32,
) (*models.Appointment, error) {
	const op = "storage.proposal.RescheduleAppointment"

	sql, args, err := s.db.Update("appointments").
		Set("slot_id", newSlotID).
		Set("starts_at", startsAt).
		Set("ends_at", endsAt).
		Set("status", "rescheduled").
		Set("version", squirrel.Expr("version + 1")).
		Set("updated_at", squirrel.Expr("now()")).
		Where(squirrel.Eq{"id": appointmentID, "version": expectedVersion}).
		Suffix("RETURNING " + appointmentColumns).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("%s: build query: %w", op, err)
	}

	apt, err := scanAppointment(qe.QueryRow(ctx, sql, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%s: execute query: %w", op, err)
	}

	return apt, nil
}

// ExpiredProposal — данные истёкшего proposal для снятия hold'а.
type ExpiredProposal struct {
	ID             int64
	AppointmentID  int64
	ProposedSlotID int64
}

// ExpirePending помечает просроченные pending-proposal как expired и возвращает их,
// чтобы вызывающий снял hold с соответствующих слотов.
func (s *ProposalStorage) ExpirePending(
	ctx context.Context,
	qe pgx_driver.QueryExecuter,
	now time.Time,
) ([]ExpiredProposal, error) {
	const op = "storage.proposal.ExpirePending"

	sql, args, err := s.db.Update("reschedule_proposals").
		Set("status", "expired").
		Set("decided_at", now).
		Where(squirrel.Eq{"status": "pending"}).
		Where(squirrel.Lt{"expires_at": now}).
		Suffix("RETURNING id, appointment_id, proposed_slot_id").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("%s: build query: %w", op, err)
	}

	rows, err := qe.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: execute query: %w", op, err)
	}
	defer rows.Close()

	var expired []ExpiredProposal
	for rows.Next() {
		var e ExpiredProposal
		if err := rows.Scan(&e.ID, &e.AppointmentID, &e.ProposedSlotID); err != nil {
			return nil, fmt.Errorf("%s: scan row: %w", op, err)
		}
		expired = append(expired, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: iterate rows: %w", op, err)
	}

	return expired, nil
}
