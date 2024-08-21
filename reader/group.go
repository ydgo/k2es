package consumer

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/ydgo/k2es/handler"
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
	ReadBatchTimeout       time.Duration // 10s
	CommitInterval         time.Duration // Default: 0
	PartitionWatchInterval time.Duration // Default: 5s
	WatchPartitionChanges  bool
	StartOffset            int64 // Default: FirstOffset
	Logger                 kafka.Logger
	ErrorLogger            kafka.Logger
	Handler                handler.Handle
}

// Stop all consumer
func (g *Group) Stop() {
	g.wg.Wait()
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
func (g *Group) StatsByClientID(id string) kafka.ReaderStats {
	for _, c := range g.consumers {
		if c.ID() == id {
			return c.reader.Stats()
		}
	}
	return kafka.ReaderStats{}
}

func (g *Group) Clients() []string {
	clients := make([]string, 0)
	for _, c := range g.consumers {
		clients = append(clients, c.ID())
	}
	return clients
}
