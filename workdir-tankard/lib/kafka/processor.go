package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"tankard_workdir/lib/kafka/dlq"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	_defaultMaxAttempts    = 3
	_defaultBaseRetryDelay = 10 * time.Millisecond
	_defaultMaxRetryDelay  = 100 * time.Millisecond

	_backoffMultiplier = 2
)

type Handler func(ctx context.Context, msg kafka.Message) error

type Processor struct {
	consumer *Consumer
	dlq      *dlq.DLQ
	logger   slog.Logger

	maxAttempts    int
	baseRetryDelay time.Duration
	maxRetryDelay  time.Duration
}

// NewProcessor создаёт обработчик сообщений с заданными потребителем, клиентом DLQ и журналом. Он применяет дополнительные параметры и проверяет полученную конфигурацию; при ошибке проверки возвращается ошибка.
func NewProcessor(c *Consumer, d *dlq.DLQ, logger slog.Logger, opts ...ProcessorOption) (*Processor, error) {
	p := &Processor{
		consumer:       c,
		dlq:            d,
		logger:         logger,
		maxAttempts:    _defaultMaxAttempts,
		baseRetryDelay: _defaultBaseRetryDelay,
		maxRetryDelay:  _defaultMaxRetryDelay,
	}

	for _, opt := range opts {
		opt(p)
	}

	if err := p.validate(); err != nil {
		return nil, fmt.Errorf("kafka.kafka-v2.NewProcessor: validation: %w", err)
	}

	return p, nil
}

// Start запускает фоновую горутину, которая получает и обрабатывает сообщения Kafka. Обработка учитывает контекст остановки. Метод сразу возвращает управление; ошибки обработки записываются в журнал, но не возвращаются вызывающей стороне.
func (p *Processor) Start(ctx context.Context, handler Handler) {
	go func() {
		for {
			msg, err := p.consumer.Fetch(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				p.logger.LogAttrs(ctx, slog.LevelError, "fetch error",
					slog.Any("error", err),
				)
				continue
			}

			p.processWithRetry(ctx, msg, handler)
		}
	}()
}

// processWithRetry вызывает обработчик не более maxAttempts раз, используя экспоненциальную задержку со случайным разбросом. Если все попытки неудачны и настроена DLQ, сообщение отправляется туда. Независимо от результата отправки смещение фиксируется, чтобы избежать бесконечной повторной обработки.
func (p *Processor) processWithRetry(ctx context.Context, msg kafka.Message, handler Handler) {
	var lastErr error

	currentBackoff := p.baseRetryDelay

	for attempt := 1; attempt <= p.maxAttempts; attempt++ {
		lastErr = handler(ctx, msg)
		if lastErr == nil {
			if err := p.consumer.Commit(ctx, msg); err != nil {
				p.logger.LogAttrs(ctx, slog.LevelError, "failed to commit message offset",
					slog.Int64("offset", msg.Offset),
					slog.String("topic", msg.Topic),
					slog.Any("error", err),
				)
			}
			return
		}

		p.logger.LogAttrs(ctx, slog.LevelWarn, "retryable error",
			slog.Int("attempt", attempt),
			slog.Any("err", lastErr),
		)

		if attempt >= p.maxAttempts {
			break
		}

		//nolint:gosec
		jitter := min(time.Duration(
			rand.Int64N(int64(currentBackoff*_backoffMultiplier)),
		), p.maxRetryDelay)

		select {
		case <-time.After(jitter):
		case <-ctx.Done():
			return
		}

		nextBackoff := min(currentBackoff*_backoffMultiplier, p.maxRetryDelay)
		currentBackoff = nextBackoff
	}

	if p.dlq != nil {
		if err := p.dlq.PublishError(ctx, msg, lastErr, p.maxAttempts); err != nil {
			p.logger.LogAttrs(ctx, slog.LevelError, "DLQ unavailable, skipping commit to prevent data loss",
				slog.Any("err", err),
			)
			return
		}
	}

	if err := p.consumer.Commit(ctx, msg); err != nil {
		p.logger.LogAttrs(ctx, slog.LevelError, "final commit error",
			slog.Any("err", err),
		)
	}
}
