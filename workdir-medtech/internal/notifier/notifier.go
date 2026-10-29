// Package notifier публикует события outbox в RabbitMQ.
package notifier

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/rabbitmq/amqp091-go"

	"medtech-practice/lib/rabbitmq"
)

// DefaultExchange — topic-exchange, через который уходят уведомления о событиях записей.
const DefaultExchange = "medtech.events"

// Notifier реализует интерфейс worker.Notifier поверх lib/rabbitmq:
// публикует JSON-конверт уведомления в exchange с routing key, равным имени очереди.
type Notifier struct {
	publisher *rabbitmq.Publisher
	log       *slog.Logger
}

// New объявляет exchange, очередь (durable) и binding с routing key = имя очереди,
// после чего возвращает готовый Notifier.
func New(client *rabbitmq.RabbitClient, queue string, log *slog.Logger) (*Notifier, error) {
	if err := client.DeclareExchange(DefaultExchange, amqp091.ExchangeTopic, true, false, false, nil); err != nil {
		return nil, fmt.Errorf("declare exchange %q: %w", DefaultExchange, err)
	}

	if err := client.DeclareQueue(queue, DefaultExchange, queue, true, false, true, nil); err != nil {
		return nil, fmt.Errorf("declare queue %q: %w", queue, err)
	}

	return &Notifier{
		publisher: rabbitmq.NewPublisher(client, DefaultExchange, "application/json"),
		log:       log,
	}, nil
}

// Publish отправляет тело уведомления в очередь queue.
func (n *Notifier) Publish(ctx context.Context, queue string, body []byte) error {
	if err := n.publisher.Publish(ctx, body, queue); err != nil {
		return fmt.Errorf("publish notification to %q: %w", queue, err)
	}

	n.log.DebugContext(ctx, "notification published",
		slog.String("queue", queue),
		slog.Int("bytes", len(body)),
	)

	return nil
}
