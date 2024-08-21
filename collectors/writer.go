package collectors

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ydgo/k2es/writer"
)

type writerCollector struct {
	mgmt     *writer.IndexerMgmt
	index    string
	added    *prometheus.Desc
	flushed  *prometheus.Desc
	indexed  *prometheus.Desc
	failed   *prometheus.Desc
	requests *prometheus.Desc
}

func NewWriterCollector(index string, mgmt *writer.IndexerMgmt) prometheus.Collector {
	fqName := func(name string) string {
		return "es_writer_" + name
	}
	return &writerCollector{
		index: index,
		mgmt:  mgmt,
		added: prometheus.NewDesc(fqName("added"),
			"The number of added", nil, prometheus.Labels{"index": index}),
		flushed: prometheus.NewDesc(
			fqName("flushed"),
			"The number of flushed", nil, prometheus.Labels{"index": index}),
		indexed: prometheus.NewDesc(
			fqName("indexed"),
			"The number of indexed", nil, prometheus.Labels{"index": index}),
		failed: prometheus.NewDesc(
			fqName("failed"),
			"The number of failed", nil, prometheus.Labels{"index": index}),
		requests: prometheus.NewDesc(
			fqName("requests"),
			"The number of requests", nil, prometheus.Labels{"index": index}),
	}
}

func (c *writerCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.added
	ch <- c.flushed
	ch <- c.indexed
	ch <- c.failed
	ch <- c.requests
}

func (c *writerCollector) Collect(ch chan<- prometheus.Metric) {
	stats := c.mgmt.GetIndex(c.index).Stats()
	ch <- prometheus.MustNewConstMetric(c.added, prometheus.GaugeValue, float64(stats.NumAdded))
	ch <- prometheus.MustNewConstMetric(c.flushed, prometheus.GaugeValue, float64(stats.NumFlushed))
	ch <- prometheus.MustNewConstMetric(c.indexed, prometheus.GaugeValue, float64(stats.NumIndexed))
	ch <- prometheus.MustNewConstMetric(c.failed, prometheus.GaugeValue, float64(stats.NumFailed))
	ch <- prometheus.MustNewConstMetric(c.requests, prometheus.GaugeValue, float64(stats.NumRequests))
}
