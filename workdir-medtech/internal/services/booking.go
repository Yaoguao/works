package services

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	pgx_driver "medtech-practice/lib/pgx-driver"
	"medtech-practice/lib/pgx-driver/transaction"
	redislib "medtech-practice/lib/redis"

	"medtech-practice/internal/models"
	"medtech-practice/internal/storage"
)

var (
	ErrSlotNotFound     = errors.New("slot not found")
	ErrSlotNotAvailable = errors.New("slot is not available")
	ErrHoldConflict     = errors.New("slot is held by another token")
)

const (
	// SlotStatusBooked — целевой статус слота после успешной брони.
	SlotStatusBooked = "booked"
	// AppointmentStatusConfirmed — статус, который получает запись при создании.
	AppointmentStatusConfirmed = "confirmed"
	// EventAppointmentCreated — тип события в outbox.
	EventAppointmentCreated = "appointment.created"

	// RaceDemoPatientID — пациент из seed-данных для демо-гонки.
	RaceDemoPatientID = int64(1001)
	// RaceDemoAttempts — число параллельных попыток по умолчанию.
	RaceDemoAttempts = 20
)

// BookingService реализует атомарную бронь слота:
// Redis hold — только UX-слой (быстрый отказ чужих запросов),
// PostgreSQL — источник истины (атомарный UPDATE слота + уникальные индексы).
type BookingService struct {
	pg        *pgx_driver.Postgres
	txManager transaction.Manager
	storage   *storage.AppointmentStorage
	redis     *redislib.Client
	log       *slog.Logger

	conflictCount atomic.Int64
}

func NewBookingService(
	pg *pgx_driver.Postgres,
	txManager transaction.Manager,
	st *storage.AppointmentStorage,
	rdb *redislib.Client,
	log *slog.Logger,
) *BookingService {
	return &BookingService{
		pg:        pg,
		txManager: txManager,
		storage:   st,
		redis:     rdb,
		log:       log,
	}
}

// ConflictCount — суммарное число конфликтов брони/hold'а с момента старта сервиса.
func (s *BookingService) ConflictCount() int64 {
	return s.conflictCount.Load()
}

func slotHoldKey(slotID int64) string {
	return fmt.Sprintf("slot:hold:%d", slotID)
}

// Hold ставит hold на слот в Redis: SET NX EX slot:hold:{id} = token.
// Повторный hold тем же токеном продлевает TTL, чужой токен — конфликт.
func (s *BookingService) Hold(ctx context.Context, slotID int64, token string, ttl time.Duration) error {
	const op = "services.booking.Hold"

	slot, err := s.storage.GetSlot(ctx, s.pg, slotID)
	if err != nil {
		return err
	}
	if slot.Status == SlotStatusBooked {
		s.bumpConflicts(ctx, op, slotID)
		return ErrSlotNotAvailable
	}

	key := slotHoldKey(slotID)
	set, err := s.redis.SetNX(ctx, key, token, ttl)
	if err != nil {
		// Redis — не источник истины: без него запись всё равно возможна,
		// но взять hold мы не можем, поэтому отдаём ошибку наверх.
		return fmt.Errorf("%s: redis setnx: %w", op, err)
	}
	if set {
		return nil
	}

	holder, err := s.redis.Get(ctx, key)
	if err == nil && holder == token {
		if err := s.redis.Expire(ctx, key, ttl); err != nil {
			return fmt.Errorf("%s: redis expire: %w", op, err)
		}
		return nil
	}

	s.bumpConflicts(ctx, op, slotID)
	return ErrHoldConflict
}

type CreateAppointmentRequest struct {
	ClinicID       int64
	PatientID      int64
	ProviderID     int64
	SlotID         int64
	IdempotencyKey string
	HoldToken      string
}

// CreateAppointment бронирует слот. Вся запись (слот + appointment + история + outbox)
// выполняется в ОДНОЙ транзакции Postgres. Повтор с тем же idempotency_key
// возвращает существующую запись, не создавая дубль.
// Второе возвращаемое значение — создана ли запись (false = идемпотентный повтор).
func (s *BookingService) CreateAppointment(
	ctx context.Context,
	req CreateAppointmentRequest,
) (*models.Appointment, bool, error) {
	const op = "services.booking.CreateAppointment"

	if req.IdempotencyKey != "" {
		apt, err := s.storage.GetAppointmentByIdempotencyKey(ctx, s.pg, req.ClinicID, req.IdempotencyKey)
		if err == nil {
			return apt, false, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, false, fmt.Errorf("%s: idempotency lookup: %w", op, err)
		}
	}

	var created *models.Appointment

	err := s.txManager.ExecuteInTransaction(ctx, "booking.create_appointment", func(qe pgx_driver.QueryExecuter) error {
		if err := s.checkHold(ctx, req.SlotID, req.HoldToken); err != nil {
			return err
		}

		slot, err := s.storage.BookSlot(ctx, qe, req.SlotID)
		if err != nil {
			return err
		}
		if slot == nil {
			if _, gerr := s.storage.GetSlot(ctx, qe, req.SlotID); gerr != nil {
				if errors.Is(gerr, storage.ErrNotFound) {
					return ErrSlotNotFound
				}
				return gerr
			}
			s.bumpConflicts(ctx, op, req.SlotID)
			return ErrSlotNotAvailable
		}

		apt := &models.Appointment{
			ClinicID:       req.ClinicID,
			PatientID:      req.PatientID,
			ProviderID:     slot.ProviderID,
			SlotID:         slot.ID,
			Status:         AppointmentStatusConfirmed,
			StartsAt:       slot.StartsAt,
			EndsAt:         slot.EndsAt,
			IdempotencyKey: &req.IdempotencyKey,
		}
		aptID, err := s.storage.InsertAppointment(ctx, qe, apt)
		if err != nil {
			return err
		}
		apt.ID = aptID

		if err := s.storage.InsertStatusHistory(ctx, qe, aptID, nil, AppointmentStatusConfirmed, "created via API"); err != nil {
			return err
		}

		payload := map[string]any{
			"appointment_id": aptID,
			"clinic_id":      slot.ClinicID,
			"provider_id":    slot.ProviderID,
			"patient_id":     req.PatientID,
			"slot_id":        slot.ID,
			"status":         AppointmentStatusConfirmed,
			"starts_at":      slot.StartsAt.Format(time.RFC3339),
			"ends_at":        slot.EndsAt.Format(time.RFC3339),
		}
		if err := s.storage.InsertOutboxEvent(ctx, qe, EventAppointmentCreated, "appointment", aptID, payload); err != nil {
			return err
		}

		created = apt
		return nil
	})
	if err != nil {

		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			switch pgErr.ConstraintName {
			case "uq_appointments_idempotency":
				apt, gerr := s.storage.GetAppointmentByIdempotencyKey(ctx, s.pg, req.ClinicID, req.IdempotencyKey)
				if gerr == nil {
					return apt, false, nil
				}
				return nil, false, fmt.Errorf("%s: idempotency relookup: %w", op, gerr)
			default:
				s.bumpConflicts(ctx, op, req.SlotID)
				return nil, false, ErrSlotNotAvailable
			}
		}

		if errors.Is(err, ErrSlotNotAvailable) || errors.Is(err, ErrHoldConflict) {
			if req.IdempotencyKey != "" {
				apt, aerr := s.storage.GetAppointmentByIdempotencyKey(ctx, s.pg, req.ClinicID, req.IdempotencyKey)
				if aerr == nil {
					return apt, false, nil
				}
				if !errors.Is(aerr, storage.ErrNotFound) {
					return nil, false, fmt.Errorf("%s: idempotency relookup: %w", op, aerr)
				}
			}
		}
		return nil, false, err
	}

	if err := s.redis.Del(ctx, slotHoldKey(req.SlotID)); err != nil {
		s.log.WarnContext(ctx, "failed to release redis hold",
			slog.String("op", op), slog.Int64("slot_id", req.SlotID), slog.Any("error", err))
	}

	return created, true, nil
}

// checkHold проверяет Redis-hold: hold есть и токен не наш — конфликт.
// Redis недоступен или hold истёк — не мешаем брони: истину знает только Postgres.
func (s *BookingService) checkHold(ctx context.Context, slotID int64, holdToken string) error {
	const op = "services.booking.checkHold"

	holder, err := s.redis.Get(ctx, slotHoldKey(slotID))
	switch {
	case errors.Is(err, redislib.NoMatches):
		return nil
	case err != nil:
		s.log.WarnContext(ctx, "redis unavailable, skipping hold check",
			slog.String("op", op), slog.Int64("slot_id", slotID), slog.Any("error", err))
		return nil
	}

	if holdToken != "" && holder == holdToken {
		return nil
	}

	s.bumpConflicts(ctx, op, slotID)
	return ErrHoldConflict
}

type RaceDemoResult struct {
	SlotID   int64    `json:"slot_id"`
	Attempts int      `json:"attempts"`
	Success  int      `json:"success"`
	Conflict int      `json:"conflict"`
	Other    int      `json:"other"`
	Errors   []string `json:"errors,omitempty"`
}

// RaceDemo параллельно пытается забронировать один слот attempts горутинами.
// Ожидаемый итог: ровно одна успешная бронь, остальные — конфликт.
func (s *BookingService) RaceDemo(ctx context.Context, slotID int64, attempts int) (*RaceDemoResult, error) {
	const op = "services.booking.RaceDemo"

	slot, err := s.storage.GetSlot(ctx, s.pg, slotID)
	if err != nil {
		return nil, err
	}
	if slot.Status == SlotStatusBooked {
		return nil, ErrSlotNotAvailable
	}

	res := &RaceDemoResult{SlotID: slotID, Attempts: attempts}

	var (
		mu    sync.Mutex
		wg    sync.WaitGroup
		start = make(chan struct{})
	)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			<-start

			_, created, err := s.CreateAppointment(ctx, CreateAppointmentRequest{
				ClinicID:       slot.ClinicID,
				PatientID:      RaceDemoPatientID,
				ProviderID:     slot.ProviderID,
				SlotID:         slotID,
				IdempotencyKey: fmt.Sprintf("race-%d-%d", slotID, n),
			})

			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil && created:
				res.Success++
			case errors.Is(err, ErrSlotNotAvailable), errors.Is(err, ErrHoldConflict):
				res.Conflict++
			case errors.Is(err, ErrSlotNotFound):
				res.Other++
				res.Errors = append(res.Errors, fmt.Sprintf("attempt %d: slot not found", n))
			default:
				res.Other++
				res.Errors = append(res.Errors, fmt.Sprintf("attempt %d: %v", n, err))
			}
		}(i)
	}
	close(start)
	wg.Wait()

	s.log.InfoContext(ctx, "race demo finished",
		slog.String("op", op),
		slog.Int64("slot_id", slotID),
		slog.Int("success", res.Success),
		slog.Int("conflict", res.Conflict),
		slog.Int("other", res.Other),
	)

	return res, nil
}

// bumpConflicts считает и логирует конфликты (метрика conflict_count).
func (s *BookingService) bumpConflicts(ctx context.Context, op string, slotID int64) {
	count := s.conflictCount.Add(1)
	s.log.WarnContext(ctx, "booking conflict",
		slog.String("op", op),
		slog.Int64("slot_id", slotID),
		slog.Int64("conflict_count", count),
	)
}
