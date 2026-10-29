package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"medtech-practice/internal/config"
	"medtech-practice/internal/services"
	"medtech-practice/internal/storage"
	"medtech-practice/lib/clickhouse"
	"medtech-practice/lib/logger/sl"
	pgx_driver "medtech-practice/lib/pgx-driver"
)

const syncTimeout = 2 * time.Minute

// cmd/sync_analytics — exporter + aggregator из TASK_03.
// Прогон: appointments (PG) -> fact_appointments (CH) -> daily_provider_stats (CH).
// Запускается повторно безопасно: дедуп держит event_id + ReplacingMergeTree,
// а агрегат пересобирается целиком (TRUNCATE + INSERT SELECT).
func main() {
	cfg := config.MustLoad()
	log := sl.InitLogger(cfg.Env, os.Stdout)

	ctx, cancel := context.WithTimeout(context.Background(), syncTimeout)
	defer cancel()

	pg, err := pgx_driver.New(postgresDSN(cfg.Postgres), log,
		pgx_driver.MaxPoolSize(int32(cfg.Postgres.MaxOpenConns)),
		pgx_driver.MinConns(int32(cfg.Postgres.MaxIdleConns)),
	)
	if err != nil {
		log.Error("failed to connect to postgres", sl.Err(err))
		os.Exit(1)
	}
	defer pg.Close()

	ch := clickhouse.New(cfg.ClickHouse.Host, cfg.ClickHouse.Port, cfg.ClickHouse.User, cfg.ClickHouse.Password)

	svc := services.NewAnalyticsService(pg, storage.NewAnalyticsStorage(pg), ch, log)

	exported, err := svc.SyncFacts(ctx)
	if err != nil {
		log.Error("failed to sync facts to clickhouse", sl.Err(err))
		os.Exit(1)
	}
	log.Info("facts exported", slog.Int("rows", exported))

	if err := svc.BuildDailyStats(ctx); err != nil {
		log.Error("failed to rebuild daily_provider_stats", sl.Err(err))
		os.Exit(1)
	}
	log.Info("daily_provider_stats rebuilt")

	fmt.Printf("sync done: exported=%d rows to %s\n", exported, "fact_appointments")
}

func postgresDSN(cfg config.Postgres) string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database, cfg.SSLMode,
	)
}
