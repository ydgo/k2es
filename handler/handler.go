package handler

import (
	"context"
	"github.com/segmentio/kafka-go"
)

type Handler interface {
	Handle(ctx context.Context, msg kafka.Message) error
}

type Handle func(ctx context.Context, msg kafka.Message) error

func (h Handle) Handle(ctx context.Context, msg kafka.Message) error {
	return h(ctx, msg)
}

type drop struct{}

func (h *drop) Handle(_ context.Context, _ kafka.Message) error {
	return nil
}
func DropHandler() Handler {
	return &drop{}
}
