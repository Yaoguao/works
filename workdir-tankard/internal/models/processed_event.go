package models

import "time"

type ProcessedEvent struct {
	EventID     string    `db:"event_id"`
	UserID      int64     `db:"user_id"`
	EventType   string    `db:"event_type"`
	ProcessedAt time.Time `db:"processed_at"`
}
