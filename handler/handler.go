package handler

import (
	"context"
	"github.com/segmentio/kafka-go"
	"time"
)

type Handle func(ctx context.Context, msg kafka.Message) error

type Handler interface {
	Handle(ctx context.Context, msg kafka.Message) error
	Duration() time.Duration
}

// DropHandler Drop Everything
type DropHandler struct{}

func (d *DropHandler) Handle(_ context.Context, _ kafka.Message) error {
	return nil
}

func (d *DropHandler) Duration() time.Duration {
	return time.Duration(0)
}

func Drop() Handler {
	return &DropHandler{}
}

// DefaultHandler Default
type DefaultHandler struct {
	handle Handle
}

func (h *DefaultHandler) Handle(ctx context.Context, msg kafka.Message) error {
	return h.handle(ctx, msg)
}

func (h *DefaultHandler) Duration() time.Duration {
	return time.Duration(0)
}

func NewDefaultHandler(handle Handle) *DefaultHandler {
	return &DefaultHandler{handle: handle}
}

type WithTimeHandler struct {
	d      time.Duration
	handle Handle
}

func NewWithTimeHandler(handle Handle) Handler {
	return &WithTimeHandler{
		handle: handle,
	}
}

func (h *WithTimeHandler) Handle(ctx context.Context, msg kafka.Message) error {
	defer h.duration()()
	return h.handle(ctx, msg)
}

func (h *WithTimeHandler) Duration() time.Duration {
	return h.d
}

func (h *WithTimeHandler) duration() func() {
	s := time.Now()
	return func() {
		h.d += time.Since(s)
	}
}
