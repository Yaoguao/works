package redis

import (
	"context"
	"errors"
	"strings"
	"tankard_workdir/lib/retry"
	"time"

	"github.com/redis/go-redis/v9"
)

const NoMatches = redis.Nil

var (
	ErrAddressRequired = errors.New("redis address is required")
	ErrInvalidMemory   = errors.New("invalid maxmemory format")
	ErrInvalidPolicy   = errors.New("invalid maxmemory-policy")
)

type Client struct {
	*redis.Client
}

type Options struct {
	Address   string
	Password  string
	MaxMemory string
	Policy    string
}

// New создаёт новый клиент Redis.
func New(addr, password string, db int) *Client {
	return &Client{
		redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
			DB:       db,
		}),
	}
}

// Connect создаёт новый клиент Redis после проверки параметров.
func Connect(options Options) (*Client, error) {
	if err := validateOptions(options); err != nil {
		return nil, err
	}
	client := &Client{
		redis.NewClient(&redis.Options{
			Addr:     options.Address,
			Password: options.Password,
		}),
	}
	ctx := context.Background()
	client.ConfigSet(ctx, "maxmemory", options.MaxMemory)
	client.ConfigSet(ctx, "maxmemory-policy", options.Policy)
	return client, client.Ping(ctx)
}

// validateOptions проверяет параметры подключения к Redis.
func validateOptions(options Options) error {
	if options.Address == "" {
		return ErrAddressRequired
	}
	if options.MaxMemory != "" {
		maxMem := strings.ToLower(options.MaxMemory)
		if !strings.HasSuffix(maxMem, "mb") && !strings.HasSuffix(maxMem, "gb") {
			return ErrInvalidMemory
		}
	}
	if options.Policy != "" {
		switch options.Policy {
		case "noeviction", "allkeys-lru", "volatile-lru",
			"allkeys-random", "volatile-random", "volatile-ttl":
		default:
			return ErrInvalidPolicy
		}
	}
	return nil
}

// Ping проверяет соединение с Redis.
func (c *Client) Ping(ctx context.Context) error {
	return c.Client.Ping(ctx).Err()
}

// Get получает значение по ключу.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	return c.Client.Get(ctx, key).Result()
}

// Set сохраняет значение по ключу.
func (c *Client) Set(ctx context.Context, key string, value any) error {
	return c.Client.Set(ctx, key, value, 0).Err()
}

// SetWithExpiration сохраняет значение с заданным сроком действия.
func (c *Client) SetWithExpiration(ctx context.Context, key string, value any, expiration time.Duration) error {
	return c.Client.Set(ctx, key, value, expiration).Err()
}

// SetNX сохраняет значение, только если ключ ещё не существует (SET NX EX). Возвращает true, если ключ создан, и false, если он уже существовал.
func (c *Client) SetNX(ctx context.Context, key string, value any, expiration time.Duration) (bool, error) {
	return c.Client.SetNX(ctx, key, value, expiration).Result()
}

// SetWithExpirationAndRetry сохраняет значение с заданным сроком действия, используя стратегию повторных попыток.
func (c *Client) SetWithExpirationAndRetry(ctx context.Context, strategy retry.Strategy,
	key string, value any, expiration time.Duration) error {
	return retry.DoContext(ctx, strategy, func() error {
		return c.Client.Set(ctx, key, value, expiration).Err()
	})
}

// Expire задаёт срок действия ключа. При значении 0 ключ не истекает, при отрицательном значении удаляется немедленно. При сбое возвращается ошибка.
func (c *Client) Expire(ctx context.Context, key string, expiration time.Duration) error {
	return c.Client.Expire(ctx, key, expiration).Err()
}

// GetWithRetry получает значение, используя стратегию повторных попыток.
func (c *Client) GetWithRetry(ctx context.Context, strategy retry.Strategy, key string) (string, error) {
	var val string
	err := retry.Do(func() error {
		v, e := c.Get(ctx, key)
		if e == nil {
			val = v
		}
		return e
	}, strategy)
	return val, err
}

// SetWithRetry сохраняет значение, используя стратегию повторных попыток.
func (c *Client) SetWithRetry(ctx context.Context, strategy retry.Strategy, key string, value any) error {
	return retry.Do(func() error {
		return c.Set(ctx, key, value)
	}, strategy)
}

// BatchWriter асинхронно выполняет пакетную запись в Redis.
func (c *Client) BatchWriter(ctx context.Context, in <-chan [2]string) {
	go func() {
		for pair := range in {
			_ = c.Set(ctx, pair[0], pair[1])
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()
}

// Del удаляет ключ.
func (c *Client) Del(ctx context.Context, key string) error {
	return c.Client.Del(ctx, key).Err()
}

// DelWithRetry удаляет ключ, используя стратегию повторных попыток.
func (c *Client) DelWithRetry(ctx context.Context, strategy retry.Strategy, key string) error {
	return retry.Do(func() error {
		return c.Del(ctx, key)
	}, strategy)
}

// Close закрывает клиент и освобождает открытые ресурсы.
func (c *Client) Close() error {
	return c.Client.Close()
}
