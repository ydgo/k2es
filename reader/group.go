package consumer

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"sync"
	"time"
)

type Group struct {
	ctx       context.Context
	wg        sync.WaitGroup
	config    GroupConfig
	consumers []*consumer
}

func NewGroup(ctx context.Context, config GroupConfig) *Group {
	consumers := make([]*consumer, 0)
	if config.Consumers <= 0 {
		config.Consumers = 1
	}
	for i := 0; i < config.Consumers; i++ {
		clientID := fmt.Sprintf("%s-%02d", config.ClientID, i+1)
		consumers = append(consumers, newConsumer(config, clientID))
	}
	group := &Group{
		ctx:       ctx,
		config:    config,
		consumers: consumers,
	}
	for _, c := range group.consumers {
		group.wg.Add(1)
		go func(c *consumer) {
			defer group.wg.Done()
			err := c.run(group.ctx)
			if err != nil {
				log.Printf("consumer run error: %s", err)
			}
		}(c)
	}
	group.wg.Add(1)
	return group
}

type GroupConfig struct {
	Consumers              int // Default: 1
	GroupID                string
	GroupTopics            []string
	Brokers                []string
	ClientID               string
	QueueCapacity          int           //  Default: 100
	MinBytes               int           // Default: 1B
	MaxBytes               int           // Default: 1MB
	MaxWait                time.Duration // Default: 10s
	ReadLagInterval        time.Duration
	CommitInterval         time.Duration // Default: 0
	PartitionWatchInterval time.Duration // Default: 5s
	WatchPartitionChanges  bool
	StartOffset            int64 // Default: FirstOffset
	Logger                 kafka.Logger
	ErrorLogger            kafka.Logger
	MaxAttempts            int  // Default: 3
	HandleDuration         bool // collect handle total duration
}

// Stop all consumer
func (g *Group) Stop() {
	g.wg.Wait()
}

type GroupStats struct {
	TotalLag      int64
	TotalMessages int64
	TotalErrors   int64
	TotalBytes    int64
	ReBalances    int64
}

func (g *Group) Stats() GroupStats {
	stats := GroupStats{}
	for _, c := range g.consumers {
		stat := c.reader.Stats()
		stats.TotalMessages += stat.Messages
		stats.TotalErrors += stat.Errors
		stats.TotalBytes += stat.Bytes
		stats.ReBalances += stat.Rebalances
	}
	return stats

}
