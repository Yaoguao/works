package models

import "time"

type UserProfile struct {
	UserID        int64      `db:"user_id"`
	ViewCount     int32      `db:"view_count"`
	ClickCount    int32      `db:"click_count"`
	PurchaseCount int32      `db:"purchase_count"`
	TopCategories []string   `db:"top_categories"`
	LastEventAt   *time.Time `db:"last_event_at"`
	UpdatedAt     time.Time  `db:"updated_at"`
}
