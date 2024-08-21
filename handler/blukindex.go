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

func BlukIndex(indexer *writer.IndexerMgmt) Handle {
	return func(ctx context.Context, msg kafka.Message) error {
		blukIndexer := indexer.GetIndex(data.TestIndex)
		item := esutil.BulkIndexerItem{
			Index:  data.TestIndex,
			Action: "index",
			Body:   bytes.NewReader(msg.Value),
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				if err != nil {
					log.Println("Indexed fail: ", err)
				} else {
					if res.Error.Type != "" {
						log.Printf("%s:%s", res.Error.Type, res.Error.Reason)
					} else {
						log.Printf("%s/%s %s (%d)", res.Index, res.DocumentID, res.Result, res.Status)
					}

				}
			},
		}
		return blukIndexer.Add(ctx, item)
	}
}
