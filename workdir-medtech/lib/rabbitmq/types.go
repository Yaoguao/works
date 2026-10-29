package rabbitmq

import (
	"context"
	"fmt"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

// ClientConfig — конфигурация клиента RabbitMQ.
type ClientConfig struct {
	URL            string        // Адрес RabbitMQ сервера (обязателен)
	ConnectionName string        // Имя для отображения в RabbitMQ Management
	ConnectTimeout time.Duration // Таймаут установки TCP соединения
	Heartbeat      time.Duration // Интервал heartbeat для поддержания соединения
	ReconnectStrat Strategy      // Стратегия повторных попыток при разрыве соединения
	ProducingStrat Strategy      // Стратегия повторных попыток для публикации сообщений
	ConsumingStrat Strategy      // Стратегия повторных попыток для обработки сообщений
}

// PublishOption — функциональная опция для публикации.
type PublishOption func(*amqp091.Publishing)

// WithExpiration - опция для указания время истечения.
func WithExpiration(d time.Duration) PublishOption {
	return func(p *amqp091.Publishing) {
		if d > 0 {
			p.Expiration = fmt.Sprintf("%d", d.Milliseconds())
		}
	}
}

// WithHeaders - опция для указания заголовков.
func WithHeaders(headers amqp091.Table) PublishOption {
	return func(p *amqp091.Publishing) {
		p.Headers = headers
	}
}

// MessageHandler обрабатывает сообщение. Возвращает ошибку → NACK, nil → ACK.
type MessageHandler func(context.Context, amqp091.Delivery) error

// ConsumerConfig — конфигурация потребителя.
type ConsumerConfig struct {
	Queue         string
	ConsumerTag   string
	AutoAck       bool
	Ask           AskConfig
	Nack          NackConfig
	Args          amqp091.Table
	Workers       int
	PrefetchCount int
}

// AskConfig - настройки Ask.
type AskConfig struct {
	Multiple bool
}

// NackConfig - настройки Nack.
type NackConfig struct {
	Multiple bool
	Requeue  bool
}
