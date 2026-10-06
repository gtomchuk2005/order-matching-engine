package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/gtomchuk2005/order-matching-engine/gateway/internal/stream"
)

// fakeBroker wraps a real Hub: the Hub itself has no Kafka dependency, so tests
// exercise the real fanout/subscribe/unsubscribe logic without a broker.
type fakeBroker struct {
	hub *stream.Hub
}

func (b *fakeBroker) Subscribe(symbol string) *stream.Subscriber {
	return b.hub.Subscribe(symbol)
}

func (b *fakeBroker) Unsubscribe(symbol string, s *stream.Subscriber) {
	b.hub.Unsubscribe(symbol, s)
}

func newWSTestHandler() (*Handler, *fakeStore, *stream.Hub) {
	s := newFakeStore()
	p := &fakeProducer{}
	hub := stream.NewHub()
	h := New(p, s, &fakeBroker{hub: hub}, 24*time.Hour)
	return h, s, hub
}

func dialStream(t *testing.T, server *httptest.Server, symbol string) (*websocket.Conn, *http.Response) {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/stream?symbol=" + symbol
	conn, resp, err := websocket.Dial(context.Background(), wsURL, nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	return conn, resp
}

func readFrame(t *testing.T, conn *websocket.Conn) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal frame: %v, data=%s", err, data)
	}
	return m
}

func TestWSSnapshotFirstFrame(t *testing.T) {
	h, s, _ := newWSTestHandler()
	_ = s.Set(context.Background(), "book:AAPL", `{"symbol":"AAPL","seq":5,"bids":[[100,1]],"asks":[]}`, 0)

	server := httptest.NewServer(h.Routes())
	defer server.Close()

	conn, _ := dialStream(t, server, "AAPL")
	defer conn.CloseNow()

	frame := readFrame(t, conn)
	if frame["type"] != "snapshot" {
		t.Fatalf("type = %v, want snapshot", frame["type"])
	}
	if frame["symbol"] != "AAPL" {
		t.Fatalf("symbol = %v, want AAPL", frame["symbol"])
	}
	seq, _ := frame["seq"].(float64)
	if seq != 5 {
		t.Fatalf("seq = %v, want 5", frame["seq"])
	}
}

func TestWSDropsEventsAtOrBelowSnapshotSeq(t *testing.T) {
	h, s, hub := newWSTestHandler()
	_ = s.Set(context.Background(), "book:AAPL", `{"symbol":"AAPL","seq":5,"bids":[],"asks":[]}`, 0)

	server := httptest.NewServer(h.Routes())
	defer server.Close()

	conn, _ := dialStream(t, server, "AAPL")
	defer conn.CloseNow()

	_ = readFrame(t, conn) // snapshot

	// Give the handler time to subscribe and read the snapshot before publishing.
	time.Sleep(100 * time.Millisecond)

	stale := []byte(`{"seq":5,"symbol":"AAPL","type":"delta"}`)
	fresh := []byte(`{"seq":6,"symbol":"AAPL","type":"delta"}`)
	hub.Publish("AAPL", 5, stale)
	hub.Publish("AAPL", 6, fresh)

	frame := readFrame(t, conn)
	seq, _ := frame["seq"].(float64)
	if seq != 6 {
		t.Fatalf("seq = %v, want 6 (stale event should have been dropped)", frame["seq"])
	}
}

func TestWSEventsArriveInOrderByteIdentical(t *testing.T) {
	h, s, hub := newWSTestHandler()
	_ = s.Set(context.Background(), "book:AAPL", `{"symbol":"AAPL","seq":1,"bids":[],"asks":[]}`, 0)

	server := httptest.NewServer(h.Routes())
	defer server.Close()

	conn, _ := dialStream(t, server, "AAPL")
	defer conn.CloseNow()

	_ = readFrame(t, conn) // snapshot
	time.Sleep(100 * time.Millisecond)

	payloads := [][]byte{
		[]byte(`{"seq":2,"symbol":"AAPL","type":"delta","x":1}`),
		[]byte(`{"seq":3,"symbol":"AAPL","type":"delta","x":2}`),
		[]byte(`{"seq":4,"symbol":"AAPL","type":"delta","x":3}`),
	}
	for i, p := range payloads {
		hub.Publish("AAPL", uint64(i+2), p)
	}

	for _, want := range payloads {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, data, err := conn.Read(ctx)
		cancel()
		if err != nil {
			t.Fatalf("read failed: %v", err)
		}
		if string(data) != string(want) {
			t.Fatalf("frame = %s, want byte-identical %s", data, want)
		}
	}
}

func TestWSNoSnapshotStillStreams(t *testing.T) {
	h, _, hub := newWSTestHandler()

	server := httptest.NewServer(h.Routes())
	defer server.Close()

	conn, _ := dialStream(t, server, "AAPL")
	defer conn.CloseNow()

	time.Sleep(100 * time.Millisecond)
	payload := []byte(`{"seq":1,"symbol":"AAPL","type":"delta"}`)
	hub.Publish("AAPL", 1, payload)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(data) != string(payload) {
		t.Fatalf("frame = %s, want %s (no snapshot frame expected first)", data, payload)
	}
}

func TestWSRedisErrorClosesWithoutStreaming(t *testing.T) {
	h, s, hub := newWSTestHandler()
	s.failErr = errors.New("redis down")

	server := httptest.NewServer(h.Routes())
	defer server.Close()

	conn, _ := dialStream(t, server, "AAPL")
	defer conn.CloseNow()

	time.Sleep(100 * time.Millisecond)
	hub.Publish("AAPL", 1, []byte(`{"seq":1,"symbol":"AAPL","type":"delta"}`))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _, err := conn.Read(ctx)
	if err == nil {
		t.Fatal("expected connection to be closed on redis error, got a frame instead")
	}
}

func TestWSMalformedSnapshotClosesWithoutStreaming(t *testing.T) {
	h, s, hub := newWSTestHandler()
	_ = s.Set(context.Background(), "book:AAPL", `not json`, 0)

	server := httptest.NewServer(h.Routes())
	defer server.Close()

	conn, _ := dialStream(t, server, "AAPL")
	defer conn.CloseNow()

	time.Sleep(100 * time.Millisecond)
	hub.Publish("AAPL", 1, []byte(`{"seq":1,"symbol":"AAPL","type":"delta"}`))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _, err := conn.Read(ctx)
	if err == nil {
		t.Fatal("expected connection to be closed on malformed snapshot, got a frame instead")
	}
}

func TestWSInvalidSymbolRejected(t *testing.T) {
	h, _, _ := newWSTestHandler()
	server := httptest.NewServer(h.Routes())
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/stream?symbol=aapl"
	_, resp, err := websocket.Dial(context.Background(), wsURL, nil)
	if err == nil {
		t.Fatal("expected dial to fail for invalid symbol")
	}
	if resp == nil || resp.StatusCode != http.StatusUnprocessableEntity {
		code := 0
		if resp != nil {
			code = resp.StatusCode
		}
		t.Fatalf("status = %d, want 422", code)
	}
}
