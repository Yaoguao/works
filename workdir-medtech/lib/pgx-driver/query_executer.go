package pgx_driver

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// QueryExecuter defines a unified interface for executing SQL queries and commands.
// It is implemented by both the main Postgres client and transaction wrappers,
// enabling seamless use of the same logic in and outside of transactions.
type QueryExecuter interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)

	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row

	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)

	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults

	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
}

func (p *Postgres) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.Pool.Query(ctx, sql, args...)
}

func (p *Postgres) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return p.Pool.QueryRow(ctx, sql, args...)
}

func (p *Postgres) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return p.Pool.Exec(ctx, sql, args...)
}

func (p *Postgres) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return p.Pool.SendBatch(ctx, b)
}

func (p *Postgres) CopyFrom(
	ctx context.Context,
	tableName pgx.Identifier,
	columnNames []string,
	rowSrc pgx.CopyFromSource,
) (int64, error) {
	return p.Pool.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

type TxQueryExecuter struct {
	Tx pgx.Tx
}

func (t *TxQueryExecuter) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return t.Tx.Query(ctx, sql, args...)
}

func (t *TxQueryExecuter) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return t.Tx.QueryRow(ctx, sql, args...)
}

func (t *TxQueryExecuter) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return t.Tx.Exec(ctx, sql, args...)
}

func (t *TxQueryExecuter) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return t.Tx.SendBatch(ctx, b)
}

func (t *TxQueryExecuter) CopyFrom(
	ctx context.Context,
	tableName pgx.Identifier,
	columnNames []string,
	rowSrc pgx.CopyFromSource,
) (int64, error) {
	return t.Tx.CopyFrom(ctx, tableName, columnNames, rowSrc)
}
