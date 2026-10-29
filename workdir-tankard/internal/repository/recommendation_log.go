package repository

import (
	"context"
	"fmt"

	"tankard_workdir/internal/models"
	pgxdriver "tankard_workdir/lib/pgx-driver"
)

type RecommendationLogRepository struct {
	db *pgxdriver.Postgres
}

func NewRecommendationLogRepository(db *pgxdriver.Postgres) *RecommendationLogRepository {
	return &RecommendationLogRepository{db: db}
}

// Insert сохраняет одну запись о выданных рекомендациях.
func (r *RecommendationLogRepository) Insert(ctx context.Context, log *models.RecommendationLog) error {
	const query = `
INSERT INTO recommendation_logs
    (request_id, user_id, surface, experiment_id, strategy_key, product_ids, source, latency_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

	if _, err := r.db.Exec(
		ctx, query,
		log.RequestID, log.UserID, log.Surface, log.ExperimentID,
		log.StrategyKey, log.ProductIDs, log.Source, log.LatencyMs,
	); err != nil {
		return fmt.Errorf("insert recommendation_log: %w", err)
	}

	return nil
}
