package rabbitmq

import (
	"context"
	"time"
)

type Strategy struct {
	Attempts int
	Delay    time.Duration
	Backoff  float64
}

func Do(fn func() error, strategy Strategy) error {
	delay := strategy.Delay
	var err error
	for i := 0; i < strategy.Attempts; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		time.Sleep(delay)
		delay = time.Duration(float64(delay) * strategy.Backoff)
	}
	return err
}

func DoContext(ctx context.Context, strategy Strategy, fn func() error) error {
	delay := strategy.Delay
	var err error
	for i := 0; i < strategy.Attempts; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay = time.Duration(float64(delay) * strategy.Backoff)
	}
	return err
}
