package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/Masterminds/squirrel"

	pgx_driver "medtech-practice/lib/pgx-driver"
)

// UnscoredAppointment — запись, ожидающая ноу-шоу скоринга.
type UnscoredAppointment struct {
	ID            int64
	PatientID     int64
	ProviderID    int64
	StartsAt      time.Time
	LeadTimeHours float64
}

// ListUnscored возвращает батч активных записей без оценки (scored_at IS NULL).
// lead_time_hours считаем как разницу между началом визита и созданием записи.
func (s *AppointmentStorage) ListUnscored(
	ctx context.Context,
	qe pgx_driver.QueryExecuter,
	limit int,
) ([]UnscoredAppointment, error) {
	const op = "storage.appointment.ListUnscored"

	if limit <= 0 {
		limit = 50
	}

	sql, args, err := s.db.Select(
		"id", "patient_id", "provider_id", "starts_at",
		"GREATEST(EXTRACT(EPOCH FROM (starts_at - created_at)) / 3600, 0) AS lead_time_hours",
	).
		From("appointments").
		Where(squirrel.Eq{"scored_at": nil}).
		Where(squirrel.Eq{"status": []string{"confirmed", "rescheduled"}}).
		OrderBy("id").
		Limit(uint64(limit)).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("%s: build query: %w", op, err)
	}

	rows, err := qe.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: execute query: %w", op, err)
	}
	defer rows.Close()

	var result []UnscoredAppointment
	for rows.Next() {
		var a UnscoredAppointment
		if err := rows.Scan(&a.ID, &a.PatientID, &a.ProviderID, &a.StartsAt, &a.LeadTimeHours); err != nil {
			return nil, fmt.Errorf("%s: scan row: %w", op, err)
		}
		result = append(result, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: iterate rows: %w", op, err)
	}

	return result, nil
}

// SaveScore записывает оценку в запись. Условие scored_at IS NULL делает запись
// идемпотентной: повторный воркер не перезапишет уже проставленный score.
func (s *AppointmentStorage) SaveScore(
	ctx context.Context,
	qe pgx_driver.QueryExecuter,
	appointmentID int64,
	score float64,
	modelVersion string,
) error {
	const op = "storage.appointment.SaveScore"

	now := time.Now()
	sql, args, err := s.db.Update("appointments").
		Set("no_show_score", score).
		Set("scored_at", now).
		Set("model_version", modelVersion).
		Set("updated_at", now).
		Where(squirrel.Eq{"id": appointmentID}).
		Where(squirrel.Expr("scored_at IS NULL")).
		ToSql()
	if err != nil {
		return fmt.Errorf("%s: build query: %w", op, err)
	}

	if _, err := qe.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("%s: execute query: %w", op, err)
	}

	return nil
}
