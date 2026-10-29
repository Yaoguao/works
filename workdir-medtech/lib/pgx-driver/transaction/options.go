package transaction

import (
	"errors"
	"time"
)

var (
	ErrInvalidMaxAttempts    = errors.New("invalid maxPoolAttempts: must be > 0")
	ErrInvalidBaseRetryDelay = errors.New("invalid base retry delay: must be > 0")
	ErrInvalidMaxRetryDelay  = errors.New("invalid max retry delay: must be > 0")
	ErrBaseExceedsMaxDelay   = errors.New("baseRetryDelay cannot exceed maxRetryDelay")
)

type Option func(*manager)

func MaxAttempts(attempts int) Option {
	return func(m *manager) {
		m.maxAttempts = attempts
	}
}

func BaseRetryDelay(delay time.Duration) Option {
	return func(m *manager) {
		m.baseRetryDelay = delay
	}
}

func MaxRetryDelay(delay time.Duration) Option {
	return func(m *manager) {
		m.maxRetryDelay = delay
	}
}

func (tm *manager) validate() error {
	if tm.maxAttempts <= 0 {
		return ErrInvalidMaxAttempts
	}

	if tm.baseRetryDelay <= 0 {
		return ErrInvalidBaseRetryDelay
	}

	if tm.maxRetryDelay <= 0 {
		return ErrInvalidMaxRetryDelay
	}

	if tm.baseRetryDelay > tm.maxRetryDelay {
		return ErrBaseExceedsMaxDelay
	}
	return nil
}
