package services

import (
	"context"
	"log/slog"

	"medtech-practice/lib/mlclient"
	pgx_driver "medtech-practice/lib/pgx-driver"

	"medtech-practice/internal/storage"
)

// Scorer — источник ноу-шоу оценки (внешний ML, в практике — stub).
type Scorer interface {
	Score(ctx context.Context, req mlclient.ScoreRequest) (*mlclient.ScoreResult, error)
}

// ScoringService проставляет ноу-шоу оценку асинхронно, отдельно от создания записи.
// Создание appointment не ждёт ML: если ML недоступен, запись живёт, а оценка
// доезжает позже при следующем проходе воркера (retry).
type ScoringService struct {
	pg      *pgx_driver.Postgres
	storage *storage.AppointmentStorage
	ml      Scorer
	log     *slog.Logger
}

func NewScoringService(
	pg *pgx_driver.Postgres,
	st *storage.AppointmentStorage,
	ml Scorer,
	log *slog.Logger,
) *ScoringService {
	return &ScoringService{pg: pg, storage: st, ml: ml, log: log}
}

// ScorePending обрабатывает батч неоценённых активных записей.
// Возвращает число успешно проставленных оценок. Ошибка по конкретной записи
// не прерывает обработку батча — она будет повторена на следующем проходе.
func (s *ScoringService) ScorePending(ctx context.Context, batch int) (int, error) {
	const op = "services.scoring.ScorePending"

	pending, err := s.storage.ListUnscored(ctx, s.pg, batch)
	if err != nil {
		return 0, err
	}

	scored := 0
	for _, apt := range pending {
		result, err := s.ml.Score(ctx, mlclient.ScoreRequest{
			PatientID:     apt.PatientID,
			ProviderID:    apt.ProviderID,
			StartsAt:      apt.StartsAt,
			LeadTimeHours: apt.LeadTimeHours,
		})
		if err != nil {
			s.log.WarnContext(ctx, "ml scoring failed, will retry",
				slog.String("op", op),
				slog.Int64("appointment_id", apt.ID),
				slog.Any("error", err),
			)
			continue
		}

		if err := s.storage.SaveScore(ctx, s.pg, apt.ID, result.Score, result.ModelVersion); err != nil {
			s.log.ErrorContext(ctx, "failed to persist no-show score",
				slog.String("op", op),
				slog.Int64("appointment_id", apt.ID),
				slog.Any("error", err),
			)
			continue
		}

		scored++
		s.log.InfoContext(ctx, "appointment scored",
			slog.String("op", op),
			slog.Int64("appointment_id", apt.ID),
			slog.Float64("score", result.Score),
			slog.String("model_version", result.ModelVersion),
		)
	}

	return scored, nil
}
