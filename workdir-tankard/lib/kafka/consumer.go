package kafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/segmentio/kafka-go"
)

type contextKey string

const kafkaMetadataKey contextKey = "kafka_metadata"

type Consumer struct {
	reader *kafka.Reader
	log    slog.Logger
}

// NewConsumer создаёт потребителя Kafka для заданных брокеров, топика и группы. Он настраивает структурированное логирование и добавляет к каждой записи метаданные Kafka.
func NewConsumer(brokers []string, topic, groupID string, log slog.Logger) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
		Logger: kafka.LoggerFunc(func(msg string, args ...any) {
			ctx := context.WithValue(context.Background(), kafkaMetadataKey, map[string]string{
				"topic":    topic,
				"group_id": groupID,
			})
			log.LogAttrs(ctx, slog.LevelInfo, "consumer info",
				slog.String("message", fmt.Sprintf(msg, args...)),
			)
		}),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...any) {
			ctx := context.WithValue(context.Background(), kafkaMetadataKey, map[string]string{
				"topic":    topic,
				"group_id": groupID,
			})
			log.LogAttrs(ctx, slog.LevelError, "consumer error",
				slog.String("error", fmt.Sprintf(msg, args...)),
			)
		}),
	})

	return &Consumer{
		reader: reader,
		log:    log,
	}
}

// Fetch получает следующее сообщение из топика Kafka, добавляя к ошибке клиента контекст. Операция учитывает отмену контекста и тайм-ауты.
func (c *Consumer) Fetch(ctx context.Context) (kafka.Message, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return kafka.Message{}, fmt.Errorf("kafkav2.Consumer.Fetch: %w", err)
	}
	return msg, nil
}

// Commit подтверждает успешную обработку сообщения фиксацией его смещения и добавляет контекст к ошибкам клиента. Вызывать метод следует только после полной обработки сообщения.
func (c *Consumer) Commit(ctx context.Context, msg kafka.Message) error {
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafkav2.Consumer.Commit: %w", err)
	}
	return nil
}

// Close останавливает потребителя Kafka и освобождает связанные ресурсы. Метод можно вызывать несколько раз.
func (c *Consumer) Close() error {
	return c.reader.Close()
}
