package dlq

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

type Publisher interface {
	Send(ctx context.Context, key, value []byte, headers ...kafka.Header) error
}

type DLQ struct {
	producer Publisher
	logger   slog.Logger
}

// New создаёт клиент DLQ с заданными отправителем и журналом. Отправитель должен быть настроен на публикацию в топик DLQ.
func New(producer Publisher, logger slog.Logger) *DLQ {
	return &DLQ{producer: producer, logger: logger}
}

// PublishError сериализует исходное сообщение Kafka, ошибку и метаданные в JSON и отправляет их в топик DLQ. Данные сообщения кодируются в base64 для поддержки двоичного содержимого. Если сериализация JSON не удалась, используется резервный текстовый формат. При ошибке отправки возвращается ошибка.
func (d *DLQ) PublishError(ctx context.Context, msg kafka.Message, err error, attempt int) error {
	const op = "dlq.PublishError"

	payload := map[string]any{
		"original_topic": msg.Topic,
		"error":          err.Error(),
		"attempt":        attempt,
		"timestamp":      time.Now().UTC(),
		"data_base64":    base64.StdEncoding.EncodeToString(msg.Value),
	}

	val, errMarshal := json.Marshal(payload)
	if errMarshal != nil {
		d.logger.LogAttrs(ctx, slog.LevelError, "failed to marshal dlq payload",
			slog.String("op", op),
			slog.Any("err", errMarshal),
		)

		fallbackData := fmt.Sprintf(`{"status":"marshal_error","raw_data":"%s","error":"%s"}`,
			string(msg.Value), err.Error())
		val = []byte(fallbackData)
	}

	if errSend := d.producer.Send(ctx, msg.Key, val); errSend != nil {
		return fmt.Errorf("%s: send to kafka: %w", op, errSend)
	}

	return nil
}
