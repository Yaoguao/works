package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"tankard_workdir/internal/repository"
	pgxdriver "tankard_workdir/lib/pgx-driver"
	"tankard_workdir/lib/pgx-driver/transaction"

	"github.com/segmentio/kafka-go"
)

const (
	EventTypeView      = "view"
	EventTypeClick     = "click"
	EventTypePurchase  = "purchase"
	EventTypeAddToCart = "add_to_cart"
)

var (
	ErrMissingEventID   = errors.New("event_id is required")
	ErrMissingUserID    = errors.New("user_external_id is required")
	ErrMissingEventType = errors.New("event_type is required")
	ErrUnknownEventType = errors.New("unknown event_type")
	ErrUserNotFound     = errors.New("user not found")
)

type Event struct {
	EventID        string    `json:"event_id"`
	UserExternalID string    `json:"user_external_id"`
	EventType      string    `json:"event_type"`
	ProductID      int64     `json:"product_id"`
	TS             time.Time `json:"ts"`
	Surface        string    `json:"surface"`
	ExperimentID   int64     `json:"experiment_id,omitempty"`
	StrategyKey    string    `json:"strategy_key,omitempty"`
}

// Validate проверяет обязательные поля и допустимость типа события.
func (e *Event) Validate() error {
	if e.EventID == "" {
		return ErrMissingEventID
	}
	if e.UserExternalID == "" {
		return ErrMissingUserID
	}
	if e.EventType == "" {
		return ErrMissingEventType
	}

	switch e.EventType {
	case EventTypeView, EventTypeClick, EventTypePurchase, EventTypeAddToCart:
	default:
		return fmt.Errorf("%w: %q", ErrUnknownEventType, e.EventType)
	}

	return nil
}

type Publisher interface {
	Send(ctx context.Context, key, value []byte, headers ...kafka.Header) error
}

type EventService struct {
	log       *slog.Logger
	publisher Publisher
	tm        transaction.Manager
	users     *repository.UserRepository
	products  *repository.ProductRepository
	profiles  *repository.ProfileRepository
	analytics EventRecorder

	processed  atomic.Int64
	duplicates atomic.Int64
	failed     atomic.Int64
}

func NewEventService(
	log *slog.Logger,
	publisher Publisher,
	tm transaction.Manager,
	users *repository.UserRepository,
	products *repository.ProductRepository,
	profiles *repository.ProfileRepository,
	analytics EventRecorder,
) *EventService {
	return &EventService{
		log:       log,
		publisher: publisher,
		tm:        tm,
		users:     users,
		products:  products,
		profiles:  profiles,
		analytics: analytics,
	}
}

// Publish проверяет событие и отправляет его в Kafka, используя внешний идентификатор пользователя в качестве ключа, чтобы события одного пользователя попадали в одну партицию. Профиль не обновляется, поэтому приём событий остаётся быстрым.
func (s *EventService) Publish(ctx context.Context, e Event) error {
	if err := e.Validate(); err != nil {
		return err
	}

	payload, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	if err := s.publisher.Send(ctx, []byte(e.UserExternalID), payload); err != nil {
		return fmt.Errorf("publish event: %w", err)
	}

	return nil
}

// Handle обрабатывает одно сообщение Kafka идемпотентно: повторный event_id пропускается без изменения счётчиков. Возврат ошибки запускает повторные попытки, а после их исчерпания сообщение отправляется в DLQ.
func (s *EventService) Handle(ctx context.Context, msg kafka.Message) error {
	var event Event
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("unmarshal event: %w", err)
	}
	if err := event.Validate(); err != nil {
		return fmt.Errorf("validate event: %w", err)
	}

	userID, err := s.users.GetIDByExternalID(ctx, event.UserExternalID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return fmt.Errorf("%w: %s", ErrUserNotFound, event.UserExternalID)
		}
		return fmt.Errorf("resolve user: %w", err)
	}

	category := s.resolveCategory(ctx, event.ProductID)

	duplicate, err := s.process(ctx, event, userID, category)
	if err != nil {
		s.failed.Add(1)
		return err
	}

	if duplicate {
		s.duplicates.Add(1)
		s.log.Info("duplicate event skipped",
			"event_id", event.EventID,
			"duplicates_total", s.duplicates.Load(),
		)
		return nil
	}

	s.processed.Add(1)
	s.log.Info("event processed",
		"event_id", event.EventID,
		"user_id", userID,
		"event_type", event.EventType,
		"processed_total", s.processed.Load(),
	)

	s.recordAnalytics(event, userID)

	return nil
}

// recordAnalytics передаёт клик или покупку в конвейер OLAP. Ошибки не должны прерывать обработку событий, поэтому функция лишь помещает запись в буфер, где она может быть отброшена при переполнении.
func (s *EventService) recordAnalytics(event Event, userID int64) {
	if s.analytics == nil {
		return
	}
	if event.EventType != EventTypeClick && event.EventType != EventTypePurchase {
		return
	}

	s.analytics.RecordEvent(FunnelEvent{
		EventID:      event.EventID,
		UserID:       userID,
		EventType:    event.EventType,
		Surface:      event.Surface,
		ProductID:    event.ProductID,
		ExperimentID: event.ExperimentID,
		StrategyKey:  event.StrategyKey,
		At:           event.TS,
	})
}

// process в одной транзакции записывает идентификатор события для идемпотентности и обновляет профиль.
func (s *EventService) process(ctx context.Context, event Event, userID int64, category string) (bool, error) {
	err := s.tm.ExecuteInTransaction(ctx, "process_event", func(tx pgxdriver.QueryExecuter) error {
		if err := s.profiles.InsertProcessedEvent(ctx, tx, event.EventID, userID, event.EventType); err != nil {
			return err
		}

		return s.profiles.ApplyEvent(ctx, tx, userID, event.EventType, category, event.TS)
	})
	if err != nil {
		if errors.Is(err, transaction.ErrConflictingData) {
			return true, nil
		}
		return false, fmt.Errorf("process event: %w", err)
	}

	return false, nil
}

// resolveCategory пытается определить категорию товара, но ошибка не должна мешать обновлению счётчиков профиля; для неизвестного товара возвращается пустая категория.
func (s *EventService) resolveCategory(ctx context.Context, productID int64) string {
	if productID <= 0 {
		return ""
	}

	category, err := s.products.GetCategory(ctx, productID)
	if err != nil {
		if !errors.Is(err, repository.ErrNotFound) {
			s.log.Warn("failed to resolve product category", "product_id", productID, "error", err)
		}
		return ""
	}

	return category
}
