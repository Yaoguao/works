package transaction

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	pgxdriver "tankard_workdir/lib/pgx-driver"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	_defaultMaxAttempts    = 3
	_defaultBaseRetryDelay = 10 * time.Millisecond
	_defaultMaxRetryDelay  = 100 * time.Millisecond

	_backoffMultiplier = 2
)

type Manager interface {
	ExecuteInTransaction(
		ctx context.Context,
		tsName string,
		fn func(tx pgxdriver.QueryExecuter) error,
	) error
}

type manager struct {
	pool   *pgxdriver.Postgres
	logger *slog.Logger

	maxAttempts    int
	baseRetryDelay time.Duration
	maxRetryDelay  time.Duration
}

func NewManager(pool *pgxdriver.Postgres, logger *slog.Logger, opts ...Option) (Manager, error) {
	tm := &manager{
		pool:   pool,
		logger: logger,

		maxAttempts:    _defaultMaxAttempts,
		baseRetryDelay: _defaultBaseRetryDelay,
		maxRetryDelay:  _defaultMaxRetryDelay,
	}

	for _, opt := range opts {
		opt(tm)
	}
	if err := tm.validate(); err != nil {
		return nil, fmt.Errorf("dbpg.pgx-driver.transaction.NewManager: %w", err)
	}

	return tm, nil
}

func (tm *manager) ExecuteInTransaction(
	ctx context.Context,
	tsName string,
	fn func(tx pgxdriver.QueryExecuter) error,
) error {
	const op = "dbpg.pgx-driver.transaction.ExecuteInTransaction"
	var lastErr error
	currentBackoff := tm.baseRetryDelay

	for attempt := 1; attempt <= tm.maxAttempts; attempt++ {
		err := tm.doTransaction(ctx, tsName, fn)
		if err == nil {
			return nil
		}

		lastErr = err

		if !isRetryableError(err) || attempt == tm.maxAttempts {
			return err
		}

		//nolint:gosec
		jitter := min(time.Duration(
			rand.Int64N(int64(currentBackoff*_backoffMultiplier)),
		), tm.maxRetryDelay)

		tm.logger.LogAttrs(ctx, slog.LevelWarn, "retrying transaction",
			slog.String("op", op),
			slog.String("transaction", tsName),
			slog.Int("attempt", attempt),
			slog.Int("max_attempts", tm.maxAttempts),
			slog.String("retry_after", jitter.String()),
			slog.Any("error", lastErr),
		)

		select {
		case <-time.After(jitter):
			currentBackoff = min(currentBackoff*_backoffMultiplier, tm.maxRetryDelay)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("%s: %s: %w", op, tsName, lastErr)
}

func (tm *manager) doTransaction(ctx context.Context, tsName string, fn func(tx pgxdriver.QueryExecuter) error) error {
	tx, err := tm.pool.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tm.safelyRollback(ctx, tx, tsName)

	if err := fn(&pgxdriver.TxQueryExecuter{Tx: tx}); err != nil {
		return HandleError(tsName, "execute", err)
	}

	return tx.Commit(ctx)
}

func (tm *manager) safelyRollback(ctx context.Context, tx pgx.Tx, tsName string) {
	const op = "dbpg.pgx-driver.transaction.safelyRollback"

	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		tm.logger.LogAttrs(ctx, slog.LevelError, "rollback failed",
			slog.String("op", op),
			slog.String("transaction", tsName),
			slog.Any("error", err),
		)
	}
}

func isRetryableError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40P01", "40001", "08000", "08003", "08006", "08001", "08004", "08007", "08P01":
			return true
		}
	}

	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) {
		return false
	}

	if errors.Is(err, pgx.ErrTxClosed) {
		return true
	}

	return false
}
