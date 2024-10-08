package main

import (
	"context"
	"flag"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
	"github.com/ydgo/k2es/collectors"
	"github.com/ydgo/k2es/config"
	"github.com/ydgo/k2es/group"
	"github.com/ydgo/k2es/indexer"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"
)

var (
	ctx, cancel = context.WithCancel(context.Background())
	configFile  = flag.String("c", "config.yml", "config file")
)

func main() {
	flag.Parse()
	cfg, err := config.Load(*configFile)
	if err != nil {
		log.Printf("load config file failed: %s\n", err)
		return
	}
	// create elasticsearch client
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:           cfg.ES.Hosts,
		DisableRetry:        true,
		CompressRequestBody: true,
	})
	if err != nil {
		log.Printf("create elasticsearch client failed: %s", err)
		return
	}

	// elasticsearch multi indexer management
	mgmt := indexer.NewIndexerMgmt(ctx, indexer.Config{
		Client:        es,
		Workers:       cfg.ES.Workers,
		FlushInterval: cfg.ES.FlushInterval,
		Timeout:       cfg.ES.Timeout,
		FlushBytes:    cfg.ES.FlushBytes,
		MaxIdleCount:  cfg.ES.MaxIdleCount,
		IdleInterval:  cfg.ES.IdleInterval,
	})
	blukIndexer := indexer.NewIndexer(indexer.BlukConfig{
		Client:        es,
		Workers:       cfg.ES.Workers,
		FlushInterval: cfg.ES.FlushInterval,
		Timeout:       cfg.ES.Timeout,
		FlushBytes:    cfg.ES.FlushBytes,
		MaxIdleCount:  cfg.ES.MaxIdleCount,
		IdleInterval:  cfg.ES.IdleInterval,
	})
	groupConfig := group.Config{
		Indexer:                mgmt,
		BlukIndexer:            blukIndexer,
		Consumers:              cfg.Kafka.ConsumerThreads,
		GroupID:                cfg.Kafka.GroupID,
		GroupTopics:            cfg.Kafka.Topics,
		Brokers:                cfg.Kafka.Brokers,
		ClientID:               cfg.Kafka.ClientID,
		QueueCapacity:          cfg.Kafka.QueueCapacity,
		MinBytes:               cfg.Kafka.MinBytes,
		MaxBytes:               cfg.Kafka.MaxBytes,
		MaxWait:                cfg.Kafka.MaxWait,
		CommitInterval:         cfg.Kafka.CommitInterval,
		PartitionWatchInterval: cfg.Kafka.PartitionWatchInterval,
		WatchPartitionChanges:  cfg.Kafka.WatchPartitionChanges,
		StartOffset:            cfg.Kafka.StartOffset,
		ErrorLogger:            kafka.LoggerFunc(func(s string, i ...interface{}) { log.Printf(s, i...) }),
	}
	consumerGroup, err := group.NewGroup(ctx, groupConfig)
	if err != nil {
		log.Printf("create consumer group failed: %s", err)
		return
	}

	// clean all resources
	clean := func() {
		consumerGroup.Stop()
		mgmt.Close()
	}

	// register prometheus collector
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewCounter(consumerGroup))

	go func() {
		http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		_ = http.ListenAndServe(":8080", nil)
	}()

	go func() {
		ticker := time.NewTicker(time.Second * 10)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				for _, index := range mgmt.Indices() {
					// todo indexerMgmt.Collector
					exporter := collectors.NewWriterCollector(index, mgmt)
					_ = reg.Register(exporter)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// 监听退出信号
	done := make(chan os.Signal)
	signal.Notify(done, os.Interrupt)
	<-done
	log.Println("receive interrupt, service stop...")
	cancel()
	clean()
	log.Println("service stopped")

}
