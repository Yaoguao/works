package kafka

import (
	"errors"
	"time"
)

var (
	ErrInvalidMaxAttempts = errors.New("invalid maxPoolAttempts: must be > 0")

	ErrInvalidBaseRetryDelay = errors.New("invalid base retry delay: must be > 0")

	ErrInvalidMaxRetryDelay = errors.New("invalid max retry delay: must be > 0")

	ErrBaseExceedsMaxDelay = errors.New("baseRetryDelay cannot exceed maxRetryDelay")
)

type ProcessorOption func(*Processor)

// MaxAttempts задаёт максимальное число попыток обработки одного сообщения Kafka, включая первую. Значение должно быть больше нуля.
func MaxAttempts(attempts int) ProcessorOption {
	return func(m *Processor) {
		m.maxAttempts = attempts
	}
}

// BaseRetryDelay задаёт начальную задержку между попытками обработки при экспоненциальном увеличении интервала. Значение должно быть больше нуля.
func BaseRetryDelay(delay time.Duration) ProcessorOption {
	return func(m *Processor) {
		m.baseRetryDelay = delay
	}
}

// MaxRetryDelay задаёт верхнюю границу задержки между попытками. Задержка не превысит это значение; оно должно быть положительным и не меньше BaseRetryDelay.
func MaxRetryDelay(delay time.Duration) ProcessorOption {
	return func(m *Processor) {
		m.maxRetryDelay = delay
	}
}

// validate проверяет корректность всех параметров Processor и возвращает ошибку, если какое-либо ограничение нарушено.
func (m *Processor) validate() error {
	if m.maxAttempts <= 0 {
		return ErrInvalidMaxAttempts
	}

	if m.baseRetryDelay <= 0 {
		return ErrInvalidBaseRetryDelay
	}

	if m.maxRetryDelay <= 0 {
		return ErrInvalidMaxRetryDelay
	}

	if m.baseRetryDelay > m.maxRetryDelay {
		return ErrBaseExceedsMaxDelay
	}
	return nil
}
