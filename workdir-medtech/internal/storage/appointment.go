package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"

	pgx_driver "medtech-practice/lib/pgx-driver"

	"medtech-practice/internal/models"
)

var ErrNotFound = errors.New("entity not found")

type AppointmentStorage struct {
	db *pgx_driver.Postgres
}

func New(db *pgx_driver.Postgres) *AppointmentStorage {
	return &AppointmentStorage{db: db}
}

const slotColumns = "id, clinic_id, provider_id, starts_at, ends_at, status"

func scanSlot(row pgx.Row) (*models.Slot, error) {
	var s models.Slot
	err := row.Scan(&s.ID, &s.ClinicID, &s.ProviderID, &s.StartsAt, &s.EndsAt, &s.Status)
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// GetSlot возвращает слот по id.
func (s *AppointmentStorage) GetSlot(ctx context.Context, qe pgx_driver.QueryExecuter, slotID int64) (*models.Slot, error) {
	const op = "storage.appointment.GetSlot"

	sql, args, err := s.db.Select(slotColumns).
		From("slots").
		Where(squirrel.Eq{"id": slotID}).
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

// BookSlot атомарно переводит слот в booked. Атомарность даёт сам UPDATE:
// условие status IN ('free','held') перепроверяется на уже заблокированной строке,
// поэтому при конкуренции ровно одна транзакция получает строку, остальные — 0 rows.
func (s *AppointmentStorage) BookSlot(ctx context.Context, qe pgx_driver.QueryExecuter, slotID int64) (*models.Slot, error) {
	const op = "storage.appointment.BookSlot"

	sql, args, err := s.db.Update("slots").
		Set("status", "booked").
		Where(squirrel.Eq{"id": slotID}).
		Where(squirrel.Eq{"status": []string{"free", "held"}}).
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

// GetAppointmentByIdempotencyKey возвращает существующую запись по ключу идемпотентности
// в рамках клиники (см. uq_appointments_idempotency) или ErrNotFound.
func (s *AppointmentStorage) GetAppointmentByIdempotencyKey(
	ctx context.Context,
	qe pgx_driver.QueryExecuter,
	clinicID int64,
	idempotencyKey string,
) (*models.Appointment, error) {
	const op = "storage.appointment.GetAppointmentByIdempotencyKey"

	sql, args, err := s.db.Select(appointmentColumns).
		From("appointments").
		Where(squirrel.Eq{"clinic_id": clinicID, "idempotency_key": idempotencyKey}).
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

const appointmentColumns = `id, clinic_id, patient_id, provider_id, slot_id, status,
	starts_at, ends_at, version, idempotency_key, created_at, updated_at`

func scanAppointment(row pgx.Row) (*models.Appointment, error) {
	var a models.Appointment
	err := row.Scan(
		&a.ID, &a.ClinicID, &a.PatientID, &a.ProviderID, &a.SlotID, &a.Status,
		&a.StartsAt, &a.EndsAt, &a.Version, &a.IdempotencyKey, &a.CreatedAt, &a.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &a, nil
}

// InsertAppointment создаёт запись и возвращает её id.
func (s *AppointmentStorage) InsertAppointment(
	ctx context.Context,
	qe pgx_driver.QueryExecuter,
	a *models.Appointment,
) (int64, error) {
	const op = "storage.appointment.InsertAppointment"

	sql, args, err := s.db.Insert("appointments").
		Columns(
			"clinic_id", "patient_id", "provider_id", "slot_id", "status",
			"starts_at", "ends_at", "idempotency_key",
		).
		Values(
			a.ClinicID, a.PatientID, a.ProviderID, a.SlotID, a.Status,
			a.StartsAt, a.EndsAt, a.IdempotencyKey,
		).
		Suffix("RETURNING id, version, created_at, updated_at").
		ToSql()
	if err != nil {
		return 0, fmt.Errorf("%s: build query: %w", op, err)
	}

	err = qe.QueryRow(ctx, sql, args...).Scan(&a.ID, &a.Version, &a.CreatedAt, &a.UpdatedAt)
	if err != nil {
		return 0, fmt.Errorf("%s: execute query: %w", op, err)
	}

	return a.ID, nil
}

// InsertStatusHistory добавляет запись о переходе статуса appointment'а.
func (s *AppointmentStorage) InsertStatusHistory(
	ctx context.Context,
	qe pgx_driver.QueryExecuter,
	appointmentID int64,
	fromStatus *string,
	toStatus string,
	reason string,
) error {
	const op = "storage.appointment.InsertStatusHistory"

	sql, args, err := s.db.Insert("appointment_status_history").
		Columns("appointment_id", "from_status", "to_status", "reason").
		Values(appointmentID, fromStatus, toStatus, reason).
		ToSql()
	if err != nil {
		return fmt.Errorf("%s: build query: %w", op, err)
	}

	if _, err := qe.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("%s: execute query: %w", op, err)
	}

	return nil
}

// InsertOutboxEvent пишет событие в outbox в той же транзакции, что и бизнес-изменение.
func (s *AppointmentStorage) InsertOutboxEvent(
	ctx context.Context,
	qe pgx_driver.QueryExecuter,
	eventType string,
	aggregateType string,
	aggregateID int64,
	payload any,
) error {
	const op = "storage.appointment.InsertOutboxEvent"

	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("%s: marshal payload: %w", op, err)
	}

	sql, args, err := s.db.Insert("outbox_events").
		Columns("event_type", "aggregate_type", "aggregate_id", "payload").
		Values(eventType, aggregateType, aggregateID, raw).
		ToSql()
	if err != nil {
		return fmt.Errorf("%s: build query: %w", op, err)
	}

	if _, err := qe.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("%s: execute query: %w", op, err)
	}

	return nil
}

// GetAppointmentByID возвращает запись по id или ErrNotFound.
func (s *AppointmentStorage) GetAppointmentByID(
	ctx context.Context,
	qe pgx_driver.QueryExecuter,
	appointmentID int64,
) (*models.Appointment, error) {
	const op = "storage.appointment.GetAppointmentByID"

	sql, args, err := s.db.Select(appointmentColumns).
		From("appointments").
		Where(squirrel.Eq{"id": appointmentID}).
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
