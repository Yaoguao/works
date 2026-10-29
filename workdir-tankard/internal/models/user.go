package models

import "time"

type User struct {
	ID         int64     `db:"id"`
	ExternalID string    `db:"external_id"`
	Segment    string    `db:"segment"`
	CreatedAt  time.Time `db:"created_at"`
}
