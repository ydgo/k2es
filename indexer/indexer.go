package indexer

import (
	"bytes"
	"context"
	"errors"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/segmentio/kafka-go"
	"github.com/ydgo/k2es/data"
	"log"
	"time"
)

type Indexer struct {
	blukIndexer esutil.BulkIndexer
}

func NewIndexer(cfg BlukConfig) *Indexer {
	bi, _ := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		NumWorkers: cfg.Workers,
		Client:     cfg.Client,
		FlushBytes: cfg.FlushBytes,
		OnError: func(ctx context.Context, err error) {
			if !errors.Is(err, context.Canceled) {
				log.Printf("indexer: %s", err)
			}
		},
		FlushInterval: cfg.FlushInterval,
		Timeout:       cfg.Timeout,
	})
	return &Indexer{
		blukIndexer: bi,
	}
}

type BlukConfig struct {
	Client        *elasticsearch.Client
	Workers       int           // Default: 1
	FlushInterval time.Duration // Default: 15s
	Timeout       time.Duration // Default: 9s
	FlushBytes    int           // 5e6 = 5MB
	MaxIdleCount  int           // 最大空闲次数 Default: 3
	IdleInterval  time.Duration // 清除空闲 indexer 的间隔时间 Default: 3 minute
}

func (indexer *Indexer) Handle(ctx context.Context, msg kafka.Message) error {
	item := esutil.BulkIndexerItem{
		Index:     data.TestIndex,
		Action:    "index",
		Body:      bytes.NewReader(msg.Value),
		OnFailure: indexer.onFail,
	}
	return indexer.blukIndexer.Add(ctx, item)

}

func (indexer *Indexer) onFail(_ context.Context, _ esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
	if err != nil {
		log.Println("indexed: ", err)
	} else if res.Error.Type != "" {
		log.Printf("indexed %s:%s", res.Error.Type, res.Error.Reason)
	}
}
