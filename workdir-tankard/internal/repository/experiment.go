package repository

import (
	"context"
	"fmt"

	"tankard_workdir/internal/models"
	pgxdriver "tankard_workdir/lib/pgx-driver"
)

type ExperimentRepository struct {
	db *pgxdriver.Postgres
}

func NewExperimentRepository(db *pgxdriver.Postgres) *ExperimentRepository {
	return &ExperimentRepository{db: db}
}

// List возвращает все экспериментальные группы в порядке идентификаторов.
func (r *ExperimentRepository) List(ctx context.Context) ([]models.RecommendationExperiment, error) {
	const query = `
SELECT id, name, strategy_key, weight, enabled
FROM recommendation_experiments
ORDER BY id`

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query experiments: %w", err)
	}
	defer rows.Close()

	var experiments []models.RecommendationExperiment
	for rows.Next() {
		var e models.RecommendationExperiment
		if err := rows.Scan(&e.ID, &e.Name, &e.StrategyKey, &e.Weight, &e.Enabled); err != nil {
			return nil, fmt.Errorf("scan experiment: %w", err)
		}
		experiments = append(experiments, e)
	}

	return experiments, rows.Err()
}

// Update частично изменяет экспериментальную группу: поля со значением nil остаются без изменений. Если группа с указанным идентификатором не найдена, возвращается ErrNotFound.
func (r *ExperimentRepository) Update(ctx context.Context, id int64, weight *int32, enabled *bool) error {
	const query = `
UPDATE recommendation_experiments
SET weight  = COALESCE($2, weight),
    enabled = COALESCE($3, enabled)
WHERE id = $1`

	tag, err := r.db.Exec(ctx, query, id, weight, enabled)
	if err != nil {
		return fmt.Errorf("update experiment: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("experiment %d: %w", id, ErrNotFound)
	}

	return nil
}
