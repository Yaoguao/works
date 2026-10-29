package pgx_driver

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	_defaultMaxPoolSize    = 100
	_defaultMinConns       = 50
	_defaultMaxIdleTime    = 10 * time.Minute
	_defaultConnAttempts   = 10
	_defaultBaseRetryDelay = 100 * time.Millisecond
	_defaultMaxRetryDelay  = 5 * time.Second

	_backoffMultiplier = 2
)

type Postgres struct {
	Builder squirrel.StatementBuilderType
	Pool    *pgxpool.Pool
	logger  *slog.Logger

	connAttempts   int
	baseRetryDelay time.Duration
	maxRetryDelay  time.Duration
	maxPoolSize    int32
	maxIdleConns   int32
	maxIdleTime    time.Duration
}

func New(dsn string, logger *slog.Logger, opts ...Option) (*Postgres, error) {
	const op = "storage.postgres.New"

	pg := &Postgres{
		logger:         logger,
		connAttempts:   _defaultConnAttempts,
		baseRetryDelay: _defaultBaseRetryDelay,
		maxRetryDelay:  _defaultMaxRetryDelay,
		maxPoolSize:    _defaultMaxPoolSize,
		maxIdleConns:   _defaultMinConns,
		maxIdleTime:    _defaultMaxIdleTime,
	}

	for _, opt := range opts {
		opt(pg)
	}
	if err := pg.validate(); err != nil {
		return nil, fmt.Errorf("%s: validation: %w", op, err)
	}

	pg.Builder = squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)

	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("%s: parse pool config: %w", op, err)
	}

	poolConfig.MaxConns = pg.maxPoolSize
	poolConfig.MinConns = pg.maxIdleConns
	poolConfig.MaxConnIdleTime = pg.maxIdleTime

	currentBackoff := pg.baseRetryDelay
	for attemptCount := 1; attemptCount <= pg.connAttempts; attemptCount++ {
		pg.Pool, err = pgxpool.NewWithConfig(context.Background(), poolConfig)
		if err == nil {
			return pg, nil
		}

		jitter := min(time.Duration(
			rand.Int64N(int64(currentBackoff*_backoffMultiplier)),
		), pg.maxRetryDelay)

		pg.logger.Info("postgresql connection attempt failed",
			"operation", op,
			"attempt", attemptCount,
			"retry_after", jitter.String(),
			"error", err,
		)

		time.Sleep(jitter)

		nextBackoff := min(currentBackoff*_backoffMultiplier, pg.maxRetryDelay)
		currentBackoff = nextBackoff
	}
	if err != nil {
		return nil, fmt.Errorf("%s: create new pool: %w", op, err)
	}

	pg.logger.Info("postgresql connection successful")

	return pg, nil
}

func (p *Postgres) Ping(ctx context.Context) error {
	return p.Pool.Ping(ctx)
}

func (p *Postgres) Close() {
	if p.Pool != nil {
		p.logger.Info("closing postgresql connection pool...")
		p.Pool.Close()
		p.logger.Info("postgresql connection pool closed")
	}
}

func (p *Postgres) Select(columns ...string) squirrel.SelectBuilder {
	return p.Builder.Select(columns...)
}

func (p *Postgres) Insert(into string) squirrel.InsertBuilder {
	return p.Builder.Insert(into)
}

func (p *Postgres) Update(table string) squirrel.UpdateBuilder {
	return p.Builder.Update(table)
}

func (p *Postgres) Delete(from string) squirrel.DeleteBuilder {
	return p.Builder.Delete(from)
}
