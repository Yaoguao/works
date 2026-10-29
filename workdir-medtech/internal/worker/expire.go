package worker

import (
	"context"
	"log/slog"
	"time"
)

// Expirer — обработчик истечения pending-proposal.
type Expirer interface {
	ExpireProposals(ctx context.Context) (int, error)
}

// ExpireWorker периодически закрывает просроченные предложения переноса
// и снимает hold с их слотов.
type ExpireWorker struct {
	expirer  Expirer
	interval time.Duration
	log      *slog.Logger
}

func NewExpireWorker(e Expirer, interval time.Duration, log *slog.Logger) *ExpireWorker {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &ExpireWorker{expirer: e, interval: interval, log: log}
}

// Run запускает цикл истечения до отмены контекста.
func (w *ExpireWorker) Run(ctx context.Context) {
	const op = "worker.expire"

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := w.expirer.ExpireProposals(ctx)
			if err != nil {
				w.log.ErrorContext(ctx, "expire pass failed", slog.String("op", op), slog.Any("error", err))
				continue
			}
			if n > 0 {
				w.log.InfoContext(ctx, "expired proposals", slog.String("op", op), slog.Int("count", n))
			}
		}
	}
}
