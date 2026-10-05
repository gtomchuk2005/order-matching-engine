package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer struct {
	client *kgo.Client
	topic  string
}

func NewProducer(brokers []string, topic string) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, err
	}
	return &Producer{client: client, topic: topic}, nil
}

// Produce blocks until the broker acks: callers must be guaranteed the record is in the log before proceeding.
func (p *Producer) Produce(ctx context.Context, key string, value []byte) error {
	record := &kgo.Record{Topic: p.topic, Key: []byte(key), Value: value}
	result := p.client.ProduceSync(ctx, record)
	return result.FirstErr()
}

func (p *Producer) Close() {
	p.client.Close()
}
