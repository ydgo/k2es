package consumer

import (
	"context"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/ydgo/k2es/handler"
	"log"
)

type consumer struct {
	id      string
	reader  *kafka.Reader
	handler handler.Handler
}

func newConsumer(groupConfig GroupConfig, clientID string) *consumer {
	return &consumer{
		id:      clientID,
		handler: handler.NewDefaultHandler(groupConfig.Handler),
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:                groupConfig.Brokers,
			GroupID:                groupConfig.GroupID,
			GroupTopics:            groupConfig.GroupTopics,
			Dialer:                 &kafka.Dialer{ClientID: clientID},
			QueueCapacity:          groupConfig.QueueCapacity,
			MinBytes:               groupConfig.MinBytes,
			MaxBytes:               groupConfig.MaxBytes,
			MaxWait:                groupConfig.MaxWait,
			ReadBatchTimeout:       groupConfig.ReadBatchTimeout,
			CommitInterval:         groupConfig.CommitInterval,
			PartitionWatchInterval: groupConfig.PartitionWatchInterval,
			WatchPartitionChanges:  groupConfig.WatchPartitionChanges,
			StartOffset:            groupConfig.StartOffset,
			Logger:                 groupConfig.Logger,
			ErrorLogger:            groupConfig.ErrorLogger,
		}),
	}
}

func (c *consumer) run(ctx context.Context) error {
	defer c.reader.Close()
	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("reader: %w", err)
		}
		err = c.handler.Handle(ctx, msg)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("handle error: %s", err)
		}
	}
}

func (c *consumer) ID() string {
	return c.id
}
