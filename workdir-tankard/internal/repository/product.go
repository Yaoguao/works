package repository

import (
	"context"
	"errors"
	"fmt"

	"tankard_workdir/internal/models"
	pgxdriver "tankard_workdir/lib/pgx-driver"

	"github.com/jackc/pgx/v5"
)

type ProductRepository struct {
	db *pgxdriver.Postgres
}

func NewProductRepository(db *pgxdriver.Postgres) *ProductRepository {
	return &ProductRepository{db: db}
}

// GetCategory возвращает категорию товара по его идентификатору. Если товар не найден, возвращается ErrNotFound.
func (r *ProductRepository) GetCategory(ctx context.Context, productID int64) (string, error) {
	const query = `SELECT category FROM products WHERE id = $1`

	var category string
	if err := r.db.QueryRow(ctx, query, productID).Scan(&category); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("product %d: %w", productID, ErrNotFound)
		}
		return "", fmt.Errorf("query product category: %w", err)
	}

	return category, nil
}

// AllIDs возвращает идентификаторы всех товаров в порядке возрастания.
func (r *ProductRepository) AllIDs(ctx context.Context) ([]int64, error) {
	const query = `SELECT id FROM products ORDER BY id`

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query product ids: %w", err)
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan product id: %w", err)
		}
		ids = append(ids, id)
	}

	return ids, rows.Err()
}

// ListByIDs загружает товары по их идентификаторам и возвращает их в виде карты.
func (r *ProductRepository) ListByIDs(ctx context.Context, ids []int64) (map[int64]models.Product, error) {
	if len(ids) == 0 {
		return map[int64]models.Product{}, nil
	}

	const query = `
SELECT id, sku, title, category, price_cents, in_stock, margin_band
FROM products
WHERE id = ANY($1)`

	rows, err := r.db.Query(ctx, query, ids)
	if err != nil {
		return nil, fmt.Errorf("query products by ids: %w", err)
	}

	products, err := scanProducts(rows)
	if err != nil {
		return nil, err
	}

	result := make(map[int64]models.Product, len(products))
	for _, p := range products {
		result[p.ID] = p
	}

	return result, nil
}

// Popular возвращает товары в наличии, отсортированные по убыванию цены.
func (r *ProductRepository) Popular(ctx context.Context, limit int) ([]models.Product, error) {
	const query = `
SELECT id, sku, title, category, price_cents, in_stock, margin_band
FROM products
WHERE in_stock = true
ORDER BY price_cents DESC, id
LIMIT $1`

	rows, err := r.db.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("query popular products: %w", err)
	}

	return scanProducts(rows)
}

// PopularByCategories возвращает товары в наличии из заданных категорий, отсортированные по убыванию цены.
func (r *ProductRepository) PopularByCategories(
	ctx context.Context,
	categories []string,
	limit int,
) ([]models.Product, error) {
	if len(categories) == 0 {
		return r.Popular(ctx, limit)
	}

	const query = `
SELECT id, sku, title, category, price_cents, in_stock, margin_band
FROM products
WHERE in_stock = true AND category = ANY($1)
ORDER BY price_cents DESC, id
LIMIT $2`

	rows, err := r.db.Query(ctx, query, categories, limit)
	if err != nil {
		return nil, fmt.Errorf("query popular products by categories: %w", err)
	}

	return scanProducts(rows)
}

func scanProducts(rows pgx.Rows) ([]models.Product, error) {
	defer rows.Close()

	var products []models.Product
	for rows.Next() {
		var p models.Product
		if err := rows.Scan(
			&p.ID, &p.SKU, &p.Title, &p.Category, &p.PriceCents, &p.InStock, &p.MarginBand,
		); err != nil {
			return nil, fmt.Errorf("scan product: %w", err)
		}
		products = append(products, p)
	}

	return products, rows.Err()
}
