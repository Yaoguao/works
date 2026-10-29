package kafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
	log    slog.Logger
}

// NewProducer создаёт производителя Kafka для указанных брокеров и топика. Ключ сообщения определяет партицию, поэтому сообщения одного пользователя попадают в одну партицию. Требуются подтверждения от всех синхронизированных реплик; тайм-аут записи — 10 секунд. Внутренние записи направляются в переданный структурированный журнал.
func NewProducer(brokers []string, topic string, log slog.Logger) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			Logger: kafka.LoggerFunc(func(msg string, args ...any) {
				log.LogAttrs(context.Background(), slog.LevelInfo, "producer info",
					slog.String("message", fmt.Sprintf(msg, args...)),
				)
			}),
			ErrorLogger: kafka.LoggerFunc(func(msg string, args ...any) {
				log.LogAttrs(context.Background(), slog.LevelError, "producer error",
					slog.String("error", fmt.Sprintf(msg, args...)),
				)
			}),
		},
		log: log,
	}
}

// Send публикует одно сообщение в топик Kafka, добавляя к ошибке клиента поясняющий контекст. Операция учитывает отмену контекста и тайм-ауты.
func (p *Producer) Send(ctx context.Context, key, value []byte, headers ...kafka.Header) error {
	err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:     key,
		Value:   value,
		Headers: headers,
	})
	if err != nil {
		return fmt.Errorf("kafkav2.Producer.Send: %w", err)
	}
	return nil
}

// Close корректно останавливает производителя и отправляет ожидающие сообщения. Метод можно вызывать несколько раз.
func (p *Producer) Close() error {
	return p.writer.Close()
}
