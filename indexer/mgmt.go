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
	"sync"
	"time"
)

type Mgmt struct {
	ctx     context.Context
	cfg     Config
	es      *elasticsearch.Client
	indexer *sync.Map

	// for sync goroutine
	mux             sync.Mutex
	idleBlukIndexer map[string]struct{} // 空闲 indexer
}

func NewIndexerMgmt(ctx context.Context, cfg Config) *Mgmt {
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 15 * time.Second
	}
	if cfg.FlushBytes <= 0 {
		// 5 MB
		cfg.FlushBytes = 5e+6
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 9 * time.Second
	}
	if cfg.MaxIdleCount <= 0 {
		cfg.MaxIdleCount = 3
	}
	if cfg.IdleInterval <= 0 {
		cfg.IdleInterval = 3 * time.Minute
	}
	mgmt := &Mgmt{
		ctx:             ctx,
		cfg:             cfg,
		es:              cfg.Client,
		indexer:         &sync.Map{},
		idleBlukIndexer: make(map[string]struct{}),
	}
	go mgmt.clean()
	return mgmt
}

type Config struct {
	Client        *elasticsearch.Client
	Workers       int           // Default: 1
	FlushInterval time.Duration // Default: 15s
	Timeout       time.Duration // Default: 9s
	FlushBytes    int           // 5e6 = 5MB
	MaxIdleCount  int           // 最大空闲次数 Default: 3
	IdleInterval  time.Duration // 清除空闲 indexer 的间隔时间 Default: 3 minute
}

func (mgmt *Mgmt) Handle(ctx context.Context, msg kafka.Message) error {
	indexer := mgmt.GetIndex(data.TestIndex)
	item := esutil.BulkIndexerItem{
		Index:     data.TestIndex,
		Action:    "index",
		Body:      bytes.NewReader(msg.Value),
		OnFailure: mgmt.onFail,
	}
	return indexer.Add(ctx, item)
}

func (mgmt *Mgmt) onFail(_ context.Context, _ esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
	if err != nil {
		log.Println("indexed: ", err)
	} else if res.Error.Type != "" {
		log.Printf("indexed %s:%s", res.Error.Type, res.Error.Reason)
	}
}

func (mgmt *Mgmt) GetIndex(index string) esutil.BulkIndexer {
	value, ok := mgmt.indexer.Load(index)
	if ok {
		indexer := value.(*blukIndexer)
		return indexer.indexer
	}
	return mgmt.addIndexer(index)

}

// clean all idle indexer
func (mgmt *Mgmt) clean() {
	ticker := time.NewTicker(mgmt.cfg.IdleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			mgmt.indexer.Range(func(k, v interface{}) bool {
				index := k.(string)
				bi, _ := v.(*blukIndexer)
				if bi.idleCount >= mgmt.cfg.MaxIdleCount {
					mgmt.indexer.Delete(index)
					_ = bi.indexer.Close(mgmt.ctx)
					mgmt.idleBlukIndexer[index] = struct{}{}
				} else {
					delete(mgmt.idleBlukIndexer, index)
					numAdded := bi.indexer.Stats().NumAdded
					if bi.numAdded >= numAdded {
						bi.idleCount++
					} else {
						bi.numAdded = numAdded
						bi.idleCount = 0
					}
					log.Printf("%s indexer num_added: %d idle_count: %d\n", index, bi.numAdded, bi.idleCount)
				}
				return true
			})
		case <-mgmt.ctx.Done():
			return
		}
	}

}

type blukIndexer struct {
	numAdded  uint64
	idleCount int
	indexer   esutil.BulkIndexer
}

func (mgmt *Mgmt) newBulkIndexer(index string) *blukIndexer {
	indexer, _ := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		NumWorkers: mgmt.cfg.Workers,
		Client:     mgmt.es,
		Index:      index,
		FlushBytes: mgmt.cfg.FlushBytes,
		OnError: func(ctx context.Context, err error) {
			if !errors.Is(err, context.Canceled) {
				log.Printf("indexer: %s", err)
			}
		},
		FlushInterval: mgmt.cfg.FlushInterval,
		Timeout:       mgmt.cfg.Timeout,
	})
	return &blukIndexer{
		indexer: indexer,
	}

}

func (mgmt *Mgmt) addIndexer(index string) esutil.BulkIndexer {
	indexer := mgmt.newBulkIndexer(index)
	mgmt.indexer.Store(index, indexer)
	return indexer.indexer

}

// Close all indexer
func (mgmt *Mgmt) Close() {
	mgmt.indexer.Range(func(key, value interface{}) bool {
		_ = value.(*blukIndexer).indexer.Close(context.Background())
		mgmt.indexer.Delete(key)
		return true
	})
}

type Stats struct {
	NumAdded    uint64
	NumFlushed  uint64
	NumFailed   uint64
	NumIndexed  uint64
	NumRequests uint64
}

func (mgmt *Mgmt) Stats() Stats {
	stats := Stats{}
	mgmt.indexer.Range(func(key interface{}, value interface{}) bool {
		stat := value.(*blukIndexer).indexer.Stats()
		stats.NumAdded += stat.NumAdded
		stats.NumFlushed += stat.NumFlushed
		stats.NumFailed += stat.NumFailed
		stats.NumIndexed += stat.NumIndexed
		stats.NumRequests += stat.NumRequests
		return true
	})
	return stats
}

func (mgmt *Mgmt) Indices() []string {
	indices := make([]string, 0)
	mgmt.indexer.Range(func(key interface{}, value interface{}) bool {
		indices = append(indices, key.(string))
		return true
	})
	return indices
}
