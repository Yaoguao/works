package services

import (
	"context"

	pgx_driver "medtech-practice/lib/pgx-driver"

	"medtech-practice/internal/models"
	"medtech-practice/internal/storage"
)

// OutboxService отдаёт воркеру неопубликованные события outbox и помечает их отправленными.
type OutboxService struct {
	pg      *pgx_driver.Postgres
	storage *storage.AppointmentStorage
}

func NewOutboxService(pg *pgx_driver.Postgres, st *storage.AppointmentStorage) *OutboxService {
	return &OutboxService{pg: pg, storage: st}
}

// ListNewOutboxEvents возвращает батч событий со статусом 'new' в порядке появления.
func (s *OutboxService) ListNewOutboxEvents(ctx context.Context, limit int) ([]models.OutboxEvent, error) {
	return s.storage.ListNewOutboxEvents(ctx, s.pg, limit)
}

// MarkOutboxSent переводит событие в статус 'sent'.
func (s *OutboxService) MarkOutboxSent(ctx context.Context, eventID int64) error {
	return s.storage.MarkOutboxSent(ctx, s.pg, eventID)
}
