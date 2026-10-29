package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/Masterminds/squirrel"

	pgx_driver "medtech-practice/lib/pgx-driver"

	"medtech-practice/internal/models"
)

// ListNewOutboxEvents возвращает батч непубликованных событий outbox (status='new').
func (s *AppointmentStorage) ListNewOutboxEvents(
	ctx context.Context,
	qe pgx_driver.QueryExecuter,
	limit int,
) ([]models.OutboxEvent, error) {
	const op = "storage.appointment.ListNewOutboxEvents"

	if limit <= 0 {
		limit = 100
	}

	sql, args, err := s.db.Select(
		"id", "event_id", "event_type", "aggregate_type", "aggregate_id",
		"payload", "status", "created_at", "sent_at",
	).
		From("outbox_events").
		Where(squirrel.Eq{"status": "new"}).
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

	var events []models.OutboxEvent
	for rows.Next() {
		var e models.OutboxEvent
		if err := rows.Scan(
			&e.ID, &e.EventID, &e.EventType, &e.AggregateType, &e.AggregateID,
			&e.Payload, &e.Status, &e.CreatedAt, &e.SentAt,
		); err != nil {
			return nil, fmt.Errorf("%s: scan row: %w", op, err)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: iterate rows: %w", op, err)
	}

	return events, nil
}

// MarkOutboxSent помечает событие опубликованным.
func (s *AppointmentStorage) MarkOutboxSent(
	ctx context.Context,
	qe pgx_driver.QueryExecuter,
	eventID int64,
) error {
	const op = "storage.appointment.MarkOutboxSent"

	sql, args, err := s.db.Update("outbox_events").
		Set("status", "sent").
		Set("sent_at", time.Now()).
		Where(squirrel.Eq{"id": eventID}).
		ToSql()
	if err != nil {
		return fmt.Errorf("%s: build query: %w", op, err)
	}

	if _, err := qe.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("%s: execute query: %w", op, err)
	}

	return nil
}
