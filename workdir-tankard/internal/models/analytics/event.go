package analytics

import "time"

type RawEvent struct {
	EventID      string    `ch:"event_id"`
	EventTime    time.Time `ch:"event_time"`
	UserID       uint64    `ch:"user_id"`
	EventType    string    `ch:"event_type"`
	Surface      string    `ch:"surface"`
	ProductID    uint64    `ch:"product_id"`
	ExperimentID uint64    `ch:"experiment_id"`
	StrategyKey  string    `ch:"strategy_key"`
}
