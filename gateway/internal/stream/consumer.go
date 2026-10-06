package stream

import (
	"context"
	"encoding/json"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Consumer reads the deltas topic with its own groupless client and fans records out via Hub.
type Consumer struct {
	client *kgo.Client
	hub    *Hub
}

func NewConsumer(brokers []string, topic string, hub *Hub) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		// No group: a groupless consumer sees every partition, which fanout needs.
		// A group would shard partitions across gateway instances.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{client: client, hub: hub}, nil
}

func (c *Consumer) Close() {
	c.client.Close()
}

// Run polls fetches until ctx is cancelled, which exits cleanly; any other fetch
// error is logged and polling continues.
func (c *Consumer) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			log.Printf("gateway: fetch error on %s[%d]: %v", topic, partition, err)
		})
		fetches.EachRecord(func(record *kgo.Record) {
			c.handleRecord(record.Value)
		})
	}
}

type recordHeader struct {
	Symbol *string `json:"symbol"`
	Seq    *uint64 `json:"seq"`
}

// handleRecord forwards the original bytes unchanged; only symbol/seq are parsed to route it.
// Pointers distinguish "field absent" from the zero value, since seq 0 is otherwise ambiguous.
func (c *Consumer) handleRecord(value []byte) {
	var hdr recordHeader
	if err := json.Unmarshal(value, &hdr); err != nil {
		log.Printf("gateway: skipping malformed delta record: %v", err)
		return
	}
	if hdr.Symbol == nil || *hdr.Symbol == "" || hdr.Seq == nil {
		log.Printf("gateway: skipping delta record missing symbol or seq")
		return
	}
	c.hub.Publish(*hdr.Symbol, *hdr.Seq, value)
}
