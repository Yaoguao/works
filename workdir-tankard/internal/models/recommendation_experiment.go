package models

type RecommendationExperiment struct {
	ID          int64  `db:"id"`
	Name        string `db:"name"`
	StrategyKey string `db:"strategy_key"`
	Weight      int32  `db:"weight"`
	Enabled     bool   `db:"enabled"`
}
