package slogdiscard

import (
	"context"
	"log/slog"
)

func NewDiscardLogger() *slog.Logger {
	return slog.New(NewDiscardHandler())
}

type DiscardHandler struct {
}

func NewDiscardHandler() *DiscardHandler {
	return &DiscardHandler{}
}

func (d DiscardHandler) Handle(_ context.Context, _ slog.Record) error {
	return nil
}

func (d DiscardHandler) WithAttrs(_ []slog.Attr) slog.Handler {
	return d
}

func (d DiscardHandler) WithGroup(_ string) slog.Handler {
	return d
}

func (d DiscardHandler) Enabled(_ context.Context, _ slog.Level) bool {
	return false
}
