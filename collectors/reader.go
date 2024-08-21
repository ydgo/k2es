package collectors

import (
	"github.com/prometheus/client_golang/prometheus"
	consumer "github.com/ydgo/k2es/reader"
)

type consumerStatCollector struct {
	group    *consumer.Group
	clientID string

	dials    *prometheus.Desc
	fetches  *prometheus.Desc
	messages *prometheus.Desc
	bytes    *prometheus.Desc
	//reBalances *prometheus.Desc
	//timeouts   *prometheus.Desc
	//errors     *prometheus.Desc
	//
	//offset        *prometheus.Desc
	//minBytes      *prometheus.Desc
	//maxBytes      *prometheus.Desc
	//maxWait       *prometheus.Desc
	//queueLength   *prometheus.Desc
	//queueCapacity *prometheus.Desc
	//partition     *prometheus.Desc
	//
	//dailTimeAvg *prometheus.Desc
	//dailTimeMin *prometheus.Desc
	//dailTimeMax *prometheus.Desc
	//dailTimeSum *prometheus.Desc
	//
	//readTimeAvg *prometheus.Desc
	//readTimeMin *prometheus.Desc
	//readTimeMax *prometheus.Desc
	//readTimeSum *prometheus.Desc
	//
	//waitTimeAvg *prometheus.Desc
	//waitTimeMin *prometheus.Desc
	//waitTimeMax *prometheus.Desc
	//waitTimeSum *prometheus.Desc
	//
	//fetchSizeAvg *prometheus.Desc // 消息条数
	//fetchSizeMin *prometheus.Desc
	//fetchSizeMax *prometheus.Desc
	//fetchSizeSum *prometheus.Desc
}

func NewConsumerCollector(group *consumer.Group, clientID string) prometheus.Collector {
	fqName := func(name string) string {
		return "consumer_group_" + name
	}
	return &consumerStatCollector{
		group:    group,
		clientID: clientID,
		dials: prometheus.NewDesc(
			fqName("consumer_dials"),
			"The number of dials ", nil, prometheus.Labels{"client_id": clientID}),
		fetches: prometheus.NewDesc(
			fqName("consumer_fetches"),
			"The number of fetches ", nil, prometheus.Labels{"client_id": clientID}),
		messages: prometheus.NewDesc(
			fqName("consumer_messages"),
			"The number of messages ", nil, prometheus.Labels{"client_id": clientID}),
		bytes: prometheus.NewDesc(
			fqName("consumer_bytes_total"),
			"The number of bytes ", nil, prometheus.Labels{"client_id": clientID}),
		//reBalances: prometheus.NewDesc(
		//	fqName("consumer_rebalances"),
		//	"The number of re balances ", nil, prometheus.Labels{"client_id": clientID}),
		//timeouts: prometheus.NewDesc(
		//	fqName("consumer_timeouts"),
		//	"The number of timeouts ", nil, prometheus.Labels{"client_id": clientID}),
		//errors: prometheus.NewDesc(
		//	fqName("consumer_errors_total"),
		//	"The number of errors ", nil, prometheus.Labels{"client_id": clientID}),
		//offset: prometheus.NewDesc(
		//	fqName("consumer_offsets"),
		//	"The number of offsets ", nil, prometheus.Labels{"client_id": clientID}),
		//minBytes: prometheus.NewDesc(
		//	fqName("consumer_min_bytes"),
		//	"The number of min bytes ", nil, prometheus.Labels{"client_id": clientID}),
		//maxBytes: prometheus.NewDesc(
		//	fqName("consumer_max_bytes"),
		//	"The number of max bytes ", nil, prometheus.Labels{"client_id": clientID}),
		//maxWait: prometheus.NewDesc(
		//	fqName("consumer_max_wait"),
		//	"The number of max wait", nil, prometheus.Labels{"client_id": clientID}),
		//queueLength: prometheus.NewDesc(
		//	fqName("consumer_queue_length"),
		//	"The number of queue length", nil, prometheus.Labels{"client_id": clientID}),
		//queueCapacity: prometheus.NewDesc(
		//	fqName("consumer_queue_capacity"),
		//	"The number of queue capacity", nil, prometheus.Labels{"client_id": clientID}),
		//partition: prometheus.NewDesc(
		//	fqName("consumer_partition"),
		//	"The number of partition", nil, prometheus.Labels{"client_id": clientID}),
		//dailTimeAvg: prometheus.NewDesc(
		//	fqName("consumer_dail_time_avg"),
		//	"Dail time avg", nil, prometheus.Labels{"client_id": clientID}),
		//dailTimeMin: prometheus.NewDesc(
		//	fqName("consumer_dail_time_min"),
		//	"Dail time min", nil, prometheus.Labels{"client_id": clientID}),
		//dailTimeMax: prometheus.NewDesc(
		//	fqName("consumer_dail_time_max"),
		//	"Dail time max", nil, prometheus.Labels{"client_id": clientID}),
		//dailTimeSum: prometheus.NewDesc(
		//	fqName("consumer_dail_time_sum"),
		//	"Dail time sum", nil, prometheus.Labels{"client_id": clientID}),
		//readTimeAvg: prometheus.NewDesc(
		//	fqName("consumer_read_time_avg"),
		//	"Read time avg", nil, prometheus.Labels{"client_id": clientID}),
		//readTimeMin: prometheus.NewDesc(
		//	fqName("consumer_read_time_min"),
		//	"Read time min", nil, prometheus.Labels{"client_id": clientID}),
		//readTimeMax: prometheus.NewDesc(
		//	fqName("consumer_read_time_max"),
		//	"Read time max", nil, prometheus.Labels{"client_id": clientID}),
		//readTimeSum: prometheus.NewDesc(
		//	fqName("consumer_read_time_sum"),
		//	"Read time sum", nil, prometheus.Labels{"client_id": clientID}),
		//waitTimeAvg: prometheus.NewDesc(
		//	fqName("consumer_wait_time_avg"),
		//	"Wait time avg", nil, prometheus.Labels{"client_id": clientID}),
		//waitTimeMin: prometheus.NewDesc(
		//	fqName("consumer_wait_time_min"),
		//	"Wait time min", nil, prometheus.Labels{"client_id": clientID}),
		//waitTimeMax: prometheus.NewDesc(
		//	fqName("consumer_wait_time_max"),
		//	"Wait time max", nil, prometheus.Labels{"client_id": clientID}),
		//waitTimeSum: prometheus.NewDesc(
		//	fqName("consumer_wait_time_sum"),
		//	"Wait time sum", nil, prometheus.Labels{"client_id": clientID}),
		//fetchSizeAvg: prometheus.NewDesc(
		//	fqName("consumer_fetch_size_avg"),
		//	"Fetch size avg", nil, prometheus.Labels{"client_id": clientID}),
		//fetchSizeMin: prometheus.NewDesc(
		//	fqName("consumer_fetch_size_min"),
		//	"Fetch size min", nil, prometheus.Labels{"client_id": clientID}),
		//fetchSizeMax: prometheus.NewDesc(
		//	fqName("consumer_fetch_size_max"),
		//	"Fetch size max", nil, prometheus.Labels{"client_id": clientID}),
		//fetchSizeSum: prometheus.NewDesc(
		//	fqName("consumer_fetch_size_sum"),
		//	"Fetch size sum", nil, prometheus.Labels{"client_id": clientID}),
	}
}

func (c *consumerStatCollector) Collect(ch chan<- prometheus.Metric) {
	stats := c.group.StatsByClientID(c.clientID)
	ch <- prometheus.MustNewConstMetric(c.dials, prometheus.GaugeValue, float64(stats.Dials))
	ch <- prometheus.MustNewConstMetric(c.fetches, prometheus.GaugeValue, float64(stats.Fetches))
	ch <- prometheus.MustNewConstMetric(c.messages, prometheus.GaugeValue, float64(stats.Messages))
	ch <- prometheus.MustNewConstMetric(c.bytes, prometheus.GaugeValue, float64(stats.Bytes))
}

func (c *consumerStatCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.dials
	ch <- c.fetches
	ch <- c.messages
	ch <- c.bytes
	//ch <- c.reBalances
	//ch <- c.timeouts
	//ch <- c.errors
	//ch <- c.offset
	//ch <- c.minBytes
	//ch <- c.maxBytes
	//ch <- c.maxWait
	//ch <- c.queueLength
	//ch <- c.queueCapacity
	//ch <- c.partition
	//ch <- c.dailTimeAvg
	//ch <- c.dailTimeMin
	//ch <- c.dailTimeMax
	//ch <- c.dailTimeSum
	//ch <- c.readTimeAvg
	//ch <- c.readTimeMin
	//ch <- c.readTimeMax
	//ch <- c.readTimeSum
	//ch <- c.waitTimeAvg
	//ch <- c.waitTimeMin
	//ch <- c.waitTimeMax
	//ch <- c.waitTimeSum
	//ch <- c.fetchSizeAvg
	//ch <- c.fetchSizeMin
	//ch <- c.fetchSizeMax
	//ch <- c.fetchSizeSum
}
