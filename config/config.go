package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"time"
)

type Config struct {
	Kafka Kafka `yaml:"kafka"` // kafka 配置
	ES    ES    `yaml:"es"`
}

// Kafka config
type Kafka struct {
	Brokers         []string `yaml:"brokers"`
	GroupID         string   `yaml:"group_id"`
	ClientID        string   `yaml:"client_id"`
	ConsumerThreads int      `yaml:"consumer_threads"` // 消费者数量
	Topics          []string `yaml:"topics"`           // 消费者组订阅的 topic

	// reader config
	MinBytes               int           `yaml:"min_bytes"`                // Default: 1B
	MaxBytes               int           `yaml:"max_bytes"`                // Default: 1MB
	MaxWait                time.Duration `yaml:"max_wait"`                 // Default: 10s
	QueueCapacity          int           `yaml:"queue_capacity"`           // Default: 100
	CommitInterval         time.Duration `yaml:"commit_interval"`          // Default: 0
	PartitionWatchInterval time.Duration `yaml:"partition_watch_interval"` // Default: 5s
	WatchPartitionChanges  bool          `yaml:"watch_partition_changes"`  // Default: false
	StartOffset            int64         `yaml:"start_offset"`             // Default: FirstOffset
}

// ES config
type ES struct {
	Hosts         []string      `yaml:"hosts"`          // elasticsearch hosts
	Workers       int           `yaml:"workers"`        // bluk indexers workers   Default: 0
	FlushInterval time.Duration `yaml:"flush_interval"` // Default: 30s
	Timeout       time.Duration `yaml:"timeout"`        // Default: 9s
	FlushBytes    int           `yaml:"flush_bytes"`    // 5e6 = 5MB
	MaxIdleCount  int           `yaml:"max_idle_count"`
	IdleInterval  time.Duration `yaml:"idle_interval"` // 从 es 查询所有模型索引的间隔
}

func Load(file string) (*Config, error) {
	body, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("read: %s", err)
	}
	replaced := os.ExpandEnv(string(body))
	cfg := new(Config)
	if err = yaml.Unmarshal([]byte(replaced), cfg); err != nil {
		return nil, fmt.Errorf("unmarshal: %s", err)
	}
	return cfg, nil
}
