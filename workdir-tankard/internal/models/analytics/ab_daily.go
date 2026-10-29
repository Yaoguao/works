package analytics

import "time"

type ABDaily struct {
	Day          time.Time `ch:"day"`
	Surface      string    `ch:"surface"`
	ExperimentID uint64    `ch:"experiment_id"`
	StrategyKey  string    `ch:"strategy_key"`
	Impressions  uint64    `ch:"impressions"`
	Clicks       uint64    `ch:"clicks"`
	Conversions  uint64    `ch:"conversions"`
}
