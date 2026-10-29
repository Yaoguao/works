package models

import "time"

type Product struct {
	ID         int64     `db:"id"`
	SKU        string    `db:"sku"`
	Title      string    `db:"title"`
	Category   string    `db:"category"`
	PriceCents int32     `db:"price_cents"`
	InStock    bool      `db:"in_stock"`
	MarginBand string    `db:"margin_band"`
	CreatedAt  time.Time `db:"created_at"`
}
