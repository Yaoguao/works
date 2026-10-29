package pgx_driver

import (
	"errors"
	"time"
)

var (
	ErrInvalidMaxPoolSize    = errors.New("invalid maxPoolSize: must be > 0")
	ErrInvalidConnAttempts   = errors.New("invalid connAttempts: must be > 0")
	ErrInvalidBaseRetryDelay = errors.New("invalid base retry delay: must be > 0")
	ErrInvalidMaxRetryDelay  = errors.New("invalid max retry delay: must be > 0")
	ErrBaseExceedsMaxDelay   = errors.New("baseRetryDelay cannot exceed maxRetryDelay")
)

type Option func(*Postgres)

func MaxPoolSize(size int32) Option {
	return func(p *Postgres) {
		p.maxPoolSize = size
	}
}

func MinConns(n int32) Option {
	return func(p *Postgres) {
		p.maxIdleConns = n
	}
}

func MaxConnIdleTime(d time.Duration) Option {
	return func(p *Postgres) {
		p.maxIdleTime = d
	}
}

func MaxConnAttempts(attempts int) Option {
	return func(p *Postgres) {
		p.connAttempts = attempts
	}
}

func BaseRetryDelay(delay time.Duration) Option {
	return func(p *Postgres) {
		p.baseRetryDelay = delay
	}
}

func MaxRetryDelay(delay time.Duration) Option {
	return func(p *Postgres) {
		p.maxRetryDelay = delay
	}
}

func (p *Postgres) validate() error {
	if p.maxPoolSize <= 0 {
		return ErrInvalidMaxPoolSize
	}

	if p.connAttempts <= 0 {
		return ErrInvalidConnAttempts
	}

	if p.baseRetryDelay <= 0 {
		return ErrInvalidBaseRetryDelay
	}

	if p.maxRetryDelay <= 0 {
		return ErrInvalidMaxRetryDelay
	}

	if p.baseRetryDelay > p.maxRetryDelay {
		return ErrBaseExceedsMaxDelay
	}
	return nil
}
