package models

import "time"

type RecommendationLog struct {
	ID           int64     `db:"id"`
	RequestID    string    `db:"request_id"`
	UserID       int64     `db:"user_id"`
	Surface      string    `db:"surface"`
	ExperimentID *int64    `db:"experiment_id"`
	StrategyKey  string    `db:"strategy_key"`
	ProductIDs   []int64   `db:"product_ids"`
	Source       string    `db:"source"`
	LatencyMs    int32     `db:"latency_ms"`
	CreatedAt    time.Time `db:"created_at"`
}
