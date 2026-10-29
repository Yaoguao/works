package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"medtech-practice/internal/models"
)

// NotificationQueue — очередь RabbitMQ, в которую публикуются события уведомлений.
const NotificationQueue = "medtech.notifications"

// OutboxStore — доступ к таблице outbox_events.
type OutboxStore interface {
	ListNewOutboxEvents(ctx context.Context, limit int) ([]models.OutboxEvent, error)
	MarkOutboxSent(ctx context.Context, eventID int64) error
}

// Notifier публикует уведомление в шину (RabbitMQ).
type Notifier interface {
	Publish(ctx context.Context, queue string, body []byte) error
}

// notification — конверт уведомления: {type, appointment_id, proposal_id}.
type notification struct {
	Type          string `json:"type"`
	AppointmentID any    `json:"appointment_id,omitempty"`
	ProposalID    any    `json:"proposal_id,omitempty"`
}

// OutboxWorker публикует необработанные события outbox в RabbitMQ-очередь
// уведомлений и помечает их отправленными.
type OutboxWorker struct {
	store    OutboxStore
	notifier Notifier
	queue    string
	interval time.Duration
	batch    int
	log      *slog.Logger
}

func NewOutboxWorker(
	store OutboxStore,
	notifier Notifier,
	queue string,
	interval time.Duration,
	batch int,
	log *slog.Logger,
) *OutboxWorker {
	if queue == "" {
		queue = NotificationQueue
	}
	if interval <= 0 {
		interval = 2 * time.Second
	}
	if batch <= 0 {
		batch = 100
	}
	return &OutboxWorker{store: store, notifier: notifier, queue: queue, interval: interval, batch: batch, log: log}
}

// Run запускает цикл публикации до отмены контекста.
func (w *OutboxWorker) Run(ctx context.Context) {
	const op = "worker.outbox"

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.tick(ctx, op)
		}
	}
}

func (w *OutboxWorker) tick(ctx context.Context, op string) {
	events, err := w.store.ListNewOutboxEvents(ctx, w.batch)
	if err != nil {
		w.log.ErrorContext(ctx, "list outbox events failed", slog.String("op", op), slog.Any("error", err))
		return
	}

	for _, e := range events {
		body, err := buildNotification(e)
		if err != nil {
			w.log.ErrorContext(ctx, "build notification failed",
				slog.String("op", op), slog.Int64("outbox_id", e.ID), slog.Any("error", err))
			continue
		}

		if err := w.notifier.Publish(ctx, w.queue, body); err != nil {
			w.log.WarnContext(ctx, "publish notification failed, will retry",
				slog.String("op", op), slog.Int64("outbox_id", e.ID), slog.Any("error", err))
			continue
		}

		if err := w.store.MarkOutboxSent(ctx, e.ID); err != nil {
			w.log.ErrorContext(ctx, "mark outbox sent failed",
				slog.String("op", op), slog.Int64("outbox_id", e.ID), slog.Any("error", err))
		}
	}
}

// buildNotification собирает JSON-конверт {type, appointment_id, proposal_id}
// из payload события outbox.
func buildNotification(e models.OutboxEvent) ([]byte, error) {
	notif := notification{Type: e.EventType}

	var payload map[string]any
	if err := json.Unmarshal(e.Payload, &payload); err == nil {
		if v, ok := payload["appointment_id"]; ok {
			notif.AppointmentID = v
		}
		if v, ok := payload["proposal_id"]; ok {
			notif.ProposalID = v
		}
	}

	return json.Marshal(notif)
}
