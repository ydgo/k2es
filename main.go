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
	"github.com/ydgo/k2es/handler"
	consumer "github.com/ydgo/k2es/reader"
	"github.com/ydgo/k2es/writer"
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
	log.Printf("config loading...")
	// load config
	cfg, err := config.Load(*configFile)
	if err != nil {
		log.Printf("load config file failed: %s\n", err)
		return
	}
	log.Printf("config loaded")
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

	// 创建测试索引和一条测试数据
	//data.CreateIndex(es)

	// elasticsearch multi indexer management
	indexerMgmt := writer.NewIndexerMgmt(ctx, writer.Config{
		Client:            es,
		Workers:           cfg.ES.Workers,
		FlushInterval:     cfg.ES.FlushInterval,
		Timeout:           cfg.ES.Timeout,
		FlushBytes:        cfg.ES.FlushBytes,
		Indices:           cfg.ES.Indices,
		MaxIdleCount:      cfg.ES.MaxIdleCount,
		SyncIndexInterval: cfg.ES.SyncIndexInterval,
	})
	groupConfig := consumer.GroupConfig{
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
		ErrorLogger: kafka.LoggerFunc(func(s string, i ...interface{}) {
			log.Printf(s, i...)
		}),
		Handler: handler.BlukIndex(indexerMgmt),
	}

	if cfg.Kafka.EnableLogger {
		groupConfig.ErrorLogger = kafka.LoggerFunc(func(s string, i ...interface{}) { log.Printf(s, i...) })
	}
	if cfg.Kafka.EnableErrorLogger {
		groupConfig.ErrorLogger = kafka.LoggerFunc(func(s string, i ...interface{}) { log.Printf(s, i...) })
	}
	consumerGroup := consumer.NewGroup(ctx, groupConfig)

	// clean all resources
	clean := func() {
		consumerGroup.Stop()
		indexerMgmt.Close()
	}

	// register prometheus collector
	reg := prometheus.NewRegistry()
	for _, client := range consumerGroup.Clients() {
		exporter := collectors.NewConsumerCollector(consumerGroup, client)
		reg.MustRegister(exporter)
	}
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
				for _, index := range indexerMgmt.Indices() {
					// todo indexerMgmt.Collector
					exporter := collectors.NewWriterCollector(index, indexerMgmt)
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
