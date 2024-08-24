package group

import (
	"context"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/ydgo/k2es/indexer"
	"io"
	"log"
	"sync"
	"time"
)

type consumer struct {
	reader  *kafka.Reader
	handler func(ctx context.Context, message kafka.Message) error
}

type Group struct {
	ctx       context.Context
	wg        sync.WaitGroup
	consumers []*consumer
	indexer   *indexer.Mgmt
}

func NewGroup(ctx context.Context, config Config) (*Group, error) {
	err := config.Validate()
	if err != nil {
		return nil, fmt.Errorf("validation: %w", err)
	}
	if config.Consumers <= 0 {
		config.Consumers = 1
	}

	consumers := make([]*consumer, 0)
	for i := 0; i < config.Consumers; i++ {
		consumers = append(consumers, &consumer{
			reader: kafka.NewReader(kafka.ReaderConfig{
				Brokers:                config.Brokers,
				GroupID:                config.GroupID,
				GroupTopics:            config.GroupTopics,
				Dialer:                 &kafka.Dialer{ClientID: fmt.Sprintf("%s-%02d", config.ClientID, i+1)},
				QueueCapacity:          config.QueueCapacity,
				MinBytes:               config.MinBytes,
				MaxBytes:               config.MaxBytes,
				MaxWait:                config.MaxWait,
				ReadBatchTimeout:       config.ReadBatchTimeout,
				CommitInterval:         config.CommitInterval,
				PartitionWatchInterval: config.PartitionWatchInterval,
				WatchPartitionChanges:  config.WatchPartitionChanges,
				StartOffset:            config.StartOffset,
				ErrorLogger:            config.ErrorLogger,
			}),
			handler: config.Indexer.Handle,
		})
	}
	group := &Group{
		ctx:       ctx,
		consumers: consumers,
		indexer:   config.Indexer,
	}
	for _, c := range group.consumers {
		group.wg.Add(1)
		go func(c *consumer) {
			defer group.wg.Done()
			err = c.run(group.ctx)
			if err != nil {
				log.Printf("run: %s", err)
			}
		}(c)
	}
	return group, nil
}

type Config struct {
	Indexer                *indexer.Mgmt
	Consumers              int // Default: 1
	GroupID                string
	GroupTopics            []string
	Brokers                []string
	ClientID               string
	QueueCapacity          int           //  Default: 100
	MinBytes               int           // Default: 1B
	MaxBytes               int           // Default: 1MB
	MaxWait                time.Duration // Default: 10s
	ReadBatchTimeout       time.Duration // 10s
	CommitInterval         time.Duration // Default: 0
	PartitionWatchInterval time.Duration // Default: 5s
	WatchPartitionChanges  bool
	StartOffset            int64 // Default: FirstOffset
	ErrorLogger            kafka.Logger
}

func (config Config) Validate() error {
	if config.Indexer == nil {
		return fmt.Errorf("indexer is required")
	}
	if len(config.GroupID) == 0 {
		return fmt.Errorf("consumer group id is required")
	}
	if len(config.GroupTopics) == 0 {
		return fmt.Errorf("consumer group topics is required")
	}
	if len(config.Brokers) == 0 {
		return fmt.Errorf("consumer brokers is required")
	}
	if len(config.ClientID) == 0 {
		return fmt.Errorf("consumer client id is required")
	}
	if config.MinBytes < 0 {
		return fmt.Errorf("invalid negative minimum batch size (min = %d)", config.MinBytes)
	}

	if config.MaxBytes < 0 {
		return fmt.Errorf("invalid negative maximum batch size (max = %d)", config.MaxBytes)
	}
	if config.MinBytes > config.MaxBytes {
		return fmt.Errorf("minimum batch size greater than the maximum (min = %d, max = %d)", config.MinBytes, config.MaxBytes)
	}
	if config.StartOffset != kafka.LastOffset && config.StartOffset != kafka.FirstOffset {
		return fmt.Errorf("start offset must be either last offset: -1 or first offset: -2")
	}
	return nil
}

// Stop all consumer
func (g *Group) Stop() {
	g.wg.Wait()
}

type Stats struct {
	Readers []kafka.ReaderStats
}

func (g *Group) Stats() Stats {
	readers := make([]kafka.ReaderStats, 0)
	for _, c := range g.consumers {
		readers = append(readers, c.reader.Stats())
	}
	return Stats{
		Readers: readers,
	}
}

func (c *consumer) run(ctx context.Context) error {
	defer c.reader.Close()
	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		err = c.handler(ctx, msg)
		if err != nil {
			log.Printf("handle: %s", err)
		}
	}
}
