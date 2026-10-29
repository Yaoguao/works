package repository

import (
	"context"
	"errors"
	"fmt"

	pgxdriver "tankard_workdir/lib/pgx-driver"

	"github.com/jackc/pgx/v5"
)

type UserRepository struct {
	db *pgxdriver.Postgres
}

func NewUserRepository(db *pgxdriver.Postgres) *UserRepository {
	return &UserRepository{db: db}
}

// GetIDByExternalID находит внутренний идентификатор пользователя по внешнему. Если пользователь не найден, возвращается ErrNotFound.
func (r *UserRepository) GetIDByExternalID(ctx context.Context, externalID string) (int64, error) {
	const query = `SELECT id FROM users WHERE external_id = $1`

	var id int64
	if err := r.db.QueryRow(ctx, query, externalID).Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("user %q: %w", externalID, ErrNotFound)
		}
		return 0, fmt.Errorf("query user by external_id: %w", err)
	}

	return id, nil
}
