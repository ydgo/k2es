package writer

import (
	"context"
	"errors"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"log"
	"sync"
	"time"
)

type IndexerMgmt struct {
	ctx     context.Context
	cfg     Config
	es      *elasticsearch.Client
	indexer sync.Map

	// for sync goroutine
	idleBlukIndexer map[string]struct{} // 空闲 indexer
}

func NewIndexerMgmt(ctx context.Context, cfg Config) *IndexerMgmt {
	mgmt := &IndexerMgmt{
		ctx:             ctx,
		es:              cfg.Client,
		cfg:             cfg,
		idleBlukIndexer: make(map[string]struct{}),
	}
	return mgmt
}

type Config struct {
	Client            *elasticsearch.Client
	Workers           int           // bluk indexers workers   Default: 1
	FlushInterval     time.Duration // Default: 30s
	Timeout           time.Duration // Default: 3s
	FlushBytes        int           // 5e6 = 5MB
	Indices           []string      // es index get api 匹配索引的字符串数组
	MaxIdleCount      int
	SyncIndexInterval time.Duration // 从 es 查询所有模型索引的间隔
}

// Close all indexer
func (mgmt *IndexerMgmt) Close() {
	mgmt.indexer.Range(func(key, value interface{}) bool {
		_ = value.(*blukIndexer).indexer.Close(context.Background())
		mgmt.indexer.Delete(key)
		return true
	})
}

func (mgmt *IndexerMgmt) GetIndex(index string) esutil.BulkIndexer {
	value, ok := mgmt.indexer.Load(index)
	if ok {
		indexer := value.(*blukIndexer)
		return indexer.indexer
	}
	return mgmt.addIndexer(index)

}

type blukIndexer struct {
	numAdded  uint64
	idleCount int
	indexer   esutil.BulkIndexer
}

func (mgmt *IndexerMgmt) newBulkIndexer(index string) *blukIndexer {
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

func (mgmt *IndexerMgmt) addIndexer(index string) esutil.BulkIndexer {
	indexer := mgmt.newBulkIndexer(index)
	mgmt.indexer.Store(index, indexer)
	return indexer.indexer

}

type Stats struct {
	NumAdded    uint64
	NumFlushed  uint64
	NumFailed   uint64
	NumIndexed  uint64
	NumRequests uint64
}

func (mgmt *IndexerMgmt) Stats() Stats {
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

func (mgmt *IndexerMgmt) Indices() []string {
	indices := make([]string, 0)
	mgmt.indexer.Range(func(key interface{}, value interface{}) bool {
		indices = append(indices, key.(string))
		return true
	})
	return indices
}
