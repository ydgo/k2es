package consumer

import (
	"context"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
)

type consumer struct {
	reader *kafka.Reader
}

func newConsumer(groupConfig GroupConfig, clientID string) *consumer {
	return &consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     groupConfig.Brokers,
			GroupID:     groupConfig.GroupID,
			GroupTopics: groupConfig.GroupTopics,
			Dialer: &kafka.Dialer{
				ClientID: clientID,
			},
			QueueCapacity:          groupConfig.QueueCapacity,
			MinBytes:               groupConfig.MinBytes,
			MaxBytes:               groupConfig.MaxBytes,
			MaxWait:                groupConfig.MaxWait,
			ReadLagInterval:        groupConfig.ReadLagInterval,
			CommitInterval:         groupConfig.CommitInterval,
			PartitionWatchInterval: groupConfig.PartitionWatchInterval,
			WatchPartitionChanges:  groupConfig.WatchPartitionChanges,
			StartOffset:            groupConfig.StartOffset,
			Logger:                 groupConfig.Logger,
			ErrorLogger:            groupConfig.ErrorLogger,
			MaxAttempts:            groupConfig.MaxAttempts,
		}),
	}
}

func (c *consumer) run(ctx context.Context) error {
	defer c.reader.Close()
	for {
		_, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("reader: %w", err)
		}

	}
}
