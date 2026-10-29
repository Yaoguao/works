package repository

import (
	"context"
	"fmt"
	"time"

	pgxdriver "tankard_workdir/lib/pgx-driver"
)

type AnalyticsRepository struct {
	db *pgxdriver.Postgres
}

func NewAnalyticsRepository(db *pgxdriver.Postgres) *AnalyticsRepository {
	return &AnalyticsRepository{db: db}
}

// ExperimentByStrategy сопоставляет strategy_key с идентификатором включённого эксперимента. Если одному ключу соответствуют несколько экспериментов, выбирается эксперимент с наибольшим весом.
func (r *AnalyticsRepository) ExperimentByStrategy(ctx context.Context) (map[string]int64, error) {
	const query = `
SELECT strategy_key, id
FROM recommendation_experiments
WHERE enabled = true
ORDER BY weight DESC, id`

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query experiments: %w", err)
	}
	defer rows.Close()

	experiments := make(map[string]int64)
	for rows.Next() {
		var strategyKey string
		var id int64
		if err := rows.Scan(&strategyKey, &id); err != nil {
			return nil, fmt.Errorf("scan experiment: %w", err)
		}
		if _, exists := experiments[strategyKey]; !exists {
			experiments[strategyKey] = id
		}
	}

	return experiments, rows.Err()
}

// RecommendationLogsByDay возвращает число запросов рекомендаций и общее число рекомендованных товаров за календарный день по московскому времени. Второе значение используется для сверки с PostgreSQL: модель событий создаёт одну запись показа на товар, и число таких записей должно совпадать с ClickHouse.
func (r *AnalyticsRepository) RecommendationLogsByDay(ctx context.Context, day time.Time) (requests, impressions int64, err error) {
	const query = `
SELECT count(*), COALESCE(sum(cardinality(product_ids)), 0)
FROM recommendation_logs
WHERE (created_at AT TIME ZONE 'Europe/Moscow')::date = $1`

	if err := r.db.QueryRow(ctx, query, day.Format("2006-01-02")).Scan(&requests, &impressions); err != nil {
		return 0, 0, fmt.Errorf("query recommendation_logs: %w", err)
	}

	return requests, impressions, nil
}
