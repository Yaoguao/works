package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	pgxdriver "tankard_workdir/lib/pgx-driver"

	"github.com/jackc/pgx/v5"
)

const maxTopCategories = 3

type ProfileRepository struct {
	db *pgxdriver.Postgres
}

func NewProfileRepository(db *pgxdriver.Postgres) *ProfileRepository {
	return &ProfileRepository{db: db}
}

// GetTopCategories возвращает наиболее популярные категории пользователя. Если профиль отсутствует или список категорий пуст, возвращается пустой срез.
func (r *ProfileRepository) GetTopCategories(ctx context.Context, userID int64) ([]string, error) {
	const query = `SELECT top_categories FROM user_profiles WHERE user_id = $1`

	var categories []string
	if err := r.db.QueryRow(ctx, query, userID).Scan(&categories); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("query top_categories: %w", err)
	}

	return categories, nil
}

// InsertProcessedEvent записывает идентификатор события для обеспечения идемпотентности. Повторный идентификатор нарушает ограничение уникальности; уровень транзакций преобразует эту ошибку в transaction.ErrConflictingData.
func (r *ProfileRepository) InsertProcessedEvent(
	ctx context.Context,
	q pgxdriver.QueryExecuter,
	eventID string,
	userID int64,
	eventType string,
) error {
	const query = `INSERT INTO processed_events (event_id, user_id, event_type) VALUES ($1, $2, $3)`

	if _, err := q.Exec(ctx, query, eventID, userID, eventType); err != nil {
		return fmt.Errorf("insert processed_event: %w", err)
	}

	return nil
}

// ApplyEvent увеличивает счётчики профиля и обновляет top_categories для одного события.
func (r *ProfileRepository) ApplyEvent(
	ctx context.Context,
	q pgxdriver.QueryExecuter,
	userID int64,
	eventType string,
	category string,
	eventTime time.Time,
) error {
	existing, err := r.lockTopCategories(ctx, q, userID)
	if err != nil {
		return err
	}

	newTop := updateTopCategories(existing, category, maxTopCategories)

	viewInc, clickInc, purchaseInc := counterDeltas(eventType)

	const query = `
INSERT INTO user_profiles (user_id, view_count, click_count, purchase_count, top_categories, last_event_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, now())
ON CONFLICT (user_id) DO UPDATE SET
    view_count     = user_profiles.view_count + EXCLUDED.view_count,
    click_count    = user_profiles.click_count + EXCLUDED.click_count,
    purchase_count = user_profiles.purchase_count + EXCLUDED.purchase_count,
    top_categories = EXCLUDED.top_categories,
    last_event_at  = EXCLUDED.last_event_at,
    updated_at     = now()`

	if _, err := q.Exec(ctx, query, userID, viewInc, clickInc, purchaseInc, newTop, eventTime); err != nil {
		return fmt.Errorf("upsert user_profile: %w", err)
	}

	return nil
}

// lockTopCategories читает и блокирует строку профиля, сериализуя параллельные обновления одного пользователя. Если строки нет, возвращается пустой срез; строка будет создана при обновлении через upsert.
func (r *ProfileRepository) lockTopCategories(
	ctx context.Context,
	q pgxdriver.QueryExecuter,
	userID int64,
) ([]string, error) {
	const query = `SELECT top_categories FROM user_profiles WHERE user_id = $1 FOR UPDATE`

	var categories []string
	if err := q.QueryRow(ctx, query, userID).Scan(&categories); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("lock user_profile: %w", err)
	}

	return categories, nil
}

// updateTopCategories перемещает категорию в начало списка, удаляет дубликаты и ограничивает список заданной длиной. Пустая категория не изменяет список.
func updateTopCategories(existing []string, category string, limit int) []string {
	if category == "" {
		return existing
	}

	out := make([]string, 0, limit)
	out = append(out, category)

	for _, c := range existing {
		if c == "" || c == category {
			continue
		}
		if len(out) >= limit {
			break
		}
		out = append(out, c)
	}

	return out
}

// counterDeltas сопоставляет тип события с приращениями счётчиков профиля.
func counterDeltas(eventType string) (view, click, purchase int) {
	switch eventType {
	case "view":
		return 1, 0, 0
	case "click":
		return 0, 1, 0
	case "purchase":
		return 0, 0, 1
	default:
		return 0, 0, 0
	}
}
