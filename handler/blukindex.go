package handler

import (
	"bytes"
	"context"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/segmentio/kafka-go"
	"github.com/ydgo/k2es/data"
	"github.com/ydgo/k2es/writer"
	"log"
)

type batchWrite struct {
	mgmt *writer.IndexerMgmt
}

func NewBatchWrite(mgmt *writer.IndexerMgmt) Handler {
	return &batchWrite{mgmt: mgmt}
}

func (b *batchWrite) Handle(ctx context.Context, msg kafka.Message) error {
	indexer := b.mgmt.GetIndex(data.TestIndex)
	item := esutil.BulkIndexerItem{
		Index:     data.TestIndex,
		Action:    "index",
		Body:      bytes.NewReader(msg.Value),
		OnFailure: b.onFail,
	}
	return indexer.Add(ctx, item)
}

func (b *batchWrite) onFail(_ context.Context, _ esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
	if err != nil {
		log.Println("indexed: ", err)
	} else if res.Error.Type != "" {
		log.Printf("indexed %s:%s", res.Error.Type, res.Error.Reason)
	}
}
