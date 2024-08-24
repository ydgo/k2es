package collectors

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ydgo/k2es/group"
)

type collector struct {
	group        *group.Group
	bytesCounter prometheus.Counter
}

func NewCounter(group *group.Group) prometheus.Collector {
	t := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "k2es",
		Subsystem: "reader",
		Name:      "bytes_total",
		Help:      "The total number of bytes read",
	})
	return &collector{
		group:        group,
		bytesCounter: t,
	}
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.bytesCounter.Desc()
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	stats := c.group.Stats()
	for _, stat := range stats.Readers {
		c.bytesCounter.Add(float64(stat.Bytes))
	}
	ch <- c.bytesCounter
}
