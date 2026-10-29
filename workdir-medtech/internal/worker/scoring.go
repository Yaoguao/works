package worker

import (
	"context"
	"log/slog"
	"time"
)

// ScoreProcessor — обработчик батча неоценённых записей.
type ScoreProcessor interface {
	ScorePending(ctx context.Context, batch int) (int, error)
}

// ScoringWorker периодически проставляет ноу-шоу оценки вне запроса пользователя:
// создание appointment не ждёт ML, оценка доезжает позже (в т.ч. ретраи при падении ML).
type ScoringWorker struct {
	processor ScoreProcessor
	interval  time.Duration
	batch     int
	log       *slog.Logger
}

func NewScoringWorker(p ScoreProcessor, interval time.Duration, batch int, log *slog.Logger) *ScoringWorker {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if batch <= 0 {
		batch = 50
	}
	return &ScoringWorker{processor: p, interval: interval, batch: batch, log: log}
}

// Run запускает цикл скоринга до отмены контекста. Делает проход сразу, затем по тикеру.
func (w *ScoringWorker) Run(ctx context.Context) {
	const op = "worker.scoring"

	w.tick(ctx, op)

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

func (w *ScoringWorker) tick(ctx context.Context, op string) {
	n, err := w.processor.ScorePending(ctx, w.batch)
	if err != nil {
		w.log.ErrorContext(ctx, "scoring pass failed", slog.String("op", op), slog.Any("error", err))
		return
	}
	if n > 0 {
		w.log.InfoContext(ctx, "scored appointments", slog.String("op", op), slog.Int("count", n))
	}
}
