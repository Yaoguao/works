package storage

import (
	"context"
	"fmt"
	"time"

	pgx_driver "medtech-practice/lib/pgx-driver"
)

// AppointmentFact — строка выгрузки appointments вместе со specialty провайдера.
type AppointmentFact struct {
	AppointmentID int64
	ClinicID      int64
	ProviderID    int64
	PatientID     int64
	Specialty     string
	Status        string
	StartsAt      time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
	NoShowScore   float64
}

type AnalyticsStorage struct {
	db *pgx_driver.Postgres
}

func NewAnalyticsStorage(db *pgx_driver.Postgres) *AnalyticsStorage {
	return &AnalyticsStorage{db: db}
}

// ListAppointmentsFacts читает записи с specialty провайдера — источник экспорта в ClickHouse.
// Specialty джойним из providers (см. HINT_03).
func (s *AnalyticsStorage) ListAppointmentsFacts(
	ctx context.Context,
	qe pgx_driver.QueryExecuter,
) ([]AppointmentFact, error) {
	const op = "storage.analytics.ListAppointmentsFacts"

	sql, args, err := s.db.Select(
		"a.id", "a.clinic_id", "a.provider_id", "a.patient_id", "p.specialty", "a.status",
		"a.starts_at", "a.created_at", "a.updated_at", "COALESCE(a.no_show_score, 0)",
	).
		From("appointments a").
		Join("providers p ON p.id = a.provider_id").
		OrderBy("a.id").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("%s: build query: %w", op, err)
	}

	rows, err := qe.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: execute query: %w", op, err)
	}
	defer rows.Close()

	var facts []AppointmentFact
	for rows.Next() {
		var f AppointmentFact
		if err := rows.Scan(
			&f.AppointmentID, &f.ClinicID, &f.ProviderID, &f.PatientID, &f.Specialty,
			&f.Status, &f.StartsAt, &f.CreatedAt, &f.UpdatedAt, &f.NoShowScore,
		); err != nil {
			return nil, fmt.Errorf("%s: scan row: %w", op, err)
		}
		facts = append(facts, f)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: iterate rows: %w", op, err)
	}

	return facts, nil
}

// CountAppointmentsByDay считает appointments в Postgres за календарный день (Europe/Moscow),
// чтобы сверять с ClickHouse на одной и той же таймзоне.
func (s *AnalyticsStorage) CountAppointmentsByDay(
	ctx context.Context,
	qe pgx_driver.QueryExecuter,
	day time.Time,
) (int64, error) {
	const op = "storage.analytics.CountAppointmentsByDay"

	sql, args, err := s.db.Select("count(*)").
		From("appointments").
		Where("(starts_at AT TIME ZONE 'Europe/Moscow')::date = ?", day.Format("2006-01-02")).
		ToSql()
	if err != nil {
		return 0, fmt.Errorf("%s: build query: %w", op, err)
	}

	var count int64
	if err := qe.QueryRow(ctx, sql, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("%s: execute query: %w", op, err)
	}

	return count, nil
}
