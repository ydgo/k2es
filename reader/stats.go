package consumer

import "github.com/segmentio/kafka-go"

type Stats struct {
	Readers []kafka.ReaderStats
}
