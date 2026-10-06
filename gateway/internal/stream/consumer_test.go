package stream

import (
	"testing"
	"time"
)

func newTestConsumer(hub *Hub) *Consumer {
	return &Consumer{hub: hub}
}

func TestHandleRecordDelta(t *testing.T) {
	h := NewHub()
	c := newTestConsumer(h)
	s := h.Subscribe("AAPL")

	value := []byte(`{"ingress_ts_ns":123,"price":10050,"qty":6,"seq":42,"side":"bid","symbol":"AAPL","type":"delta"}`)
	c.handleRecord(value)

	select {
	case ev := <-s.Events():
		if ev.Seq != 42 {
			t.Fatalf("seq = %d, want 42", ev.Seq)
		}
		if string(ev.Payload) != string(value) {
			t.Fatalf("payload mutated: got %s, want %s", ev.Payload, value)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delta event")
	}
}

func TestHandleRecordTrade(t *testing.T) {
	h := NewHub()
	c := newTestConsumer(h)
	s := h.Subscribe("AAPL")

	value := []byte(`{"ingress_ts_ns":123,"maker_id":"a1","price":10050,"qty":4,"seq":43,"symbol":"AAPL","taker_id":"b2","type":"trade"}`)
	c.handleRecord(value)

	select {
	case ev := <-s.Events():
		if ev.Seq != 43 {
			t.Fatalf("seq = %d, want 43", ev.Seq)
		}
		if string(ev.Payload) != string(value) {
			t.Fatalf("payload mutated: got %s, want %s", ev.Payload, value)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for trade event")
	}
}

func TestHandleRecordMalformedJSONSkipped(t *testing.T) {
	h := NewHub()
	c := newTestConsumer(h)
	s := h.Subscribe("AAPL")

	c.handleRecord([]byte(`not json`))

	select {
	case ev := <-s.Events():
		t.Fatalf("expected no event for malformed record, got %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestHandleRecordMissingSeqSkipped(t *testing.T) {
	h := NewHub()
	c := newTestConsumer(h)
	s := h.Subscribe("AAPL")

	c.handleRecord([]byte(`{"symbol":"AAPL","type":"delta"}`))

	select {
	case ev := <-s.Events():
		t.Fatalf("expected no event for record missing seq, got %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestHandleRecordMissingSymbolSkipped(t *testing.T) {
	h := NewHub()
	c := newTestConsumer(h)
	s := h.Subscribe("AAPL")

	c.handleRecord([]byte(`{"seq":1,"type":"delta"}`))

	select {
	case ev := <-s.Events():
		t.Fatalf("expected no event for record missing symbol, got %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}
