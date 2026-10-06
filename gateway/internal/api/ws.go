package api

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/gtomchuk2005/order-matching-engine/gateway/internal/stream"
)

const wsWriteTimeout = 10 * time.Second

// Broker is the narrow view of the hub the WS handler needs, so tests can fake it
// without a real Kafka consumer.
type Broker interface {
	Subscribe(symbol string) *stream.Subscriber
	Unsubscribe(symbol string, s *stream.Subscriber)
}

type bookSeq struct {
	Seq  uint64          `json:"seq"`
	Bids json.RawMessage `json:"bids"`
	Asks json.RawMessage `json:"asks"`
}

type snapshotFrame struct {
	Type   string          `json:"type"`
	Symbol string          `json:"symbol"`
	Seq    uint64          `json:"seq"`
	Bids   json.RawMessage `json:"bids"`
	Asks   json.RawMessage `json:"asks"`
}

func (h *Handler) handleStream(w http.ResponseWriter, r *http.Request) {
	symbol := r.URL.Query().Get("symbol")
	if verr := validateSymbol(symbol); verr != nil {
		writeValidationError(w, verr)
		return
	}

	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()

	// Subscribe before touching Redis: buffered events accumulate from this
	// instant, so nothing published between here and the snapshot read is lost.
	sub := h.Broker.Subscribe(symbol)
	defer h.Broker.Unsubscribe(symbol, sub)

	ctx := r.Context()
	// Detect client disconnect/close frames; this connection only ever writes.
	ctx = conn.CloseRead(ctx)

	var lastSeq uint64
	body, found, err := h.Store.Get(ctx, "book:"+symbol)
	if err != nil {
		log.Printf("gateway: redis Get failed: %v", err)
		conn.Close(websocket.StatusInternalError, "snapshot unavailable")
		return
	}
	if found {
		var snap bookSeq
		if err := json.Unmarshal([]byte(body), &snap); err != nil {
			// A Redis miss is a valid "symbol not touched yet" state, but a failure to
			// parse a found snapshot must not be treated the same way: the client
			// would stream deltas against a book it never received.
			log.Printf("gateway: malformed book snapshot for %s: %v", symbol, err)
			conn.Close(websocket.StatusInternalError, "snapshot unavailable")
			return
		}
		lastSeq = snap.Seq
		frame := snapshotFrame{
			Type:   "snapshot",
			Symbol: symbol,
			Seq:    snap.Seq,
			Bids:   snap.Bids,
			Asks:   snap.Asks,
		}
		value, err := json.Marshal(frame)
		if err != nil {
			log.Printf("gateway: marshal snapshot failed: %v", err)
			conn.Close(websocket.StatusInternalError, "snapshot unavailable")
			return
		}
		if !writeFrame(ctx, conn, value) {
			return
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-sub.Closed():
			return
		case ev, ok := <-sub.Events():
			if !ok {
				return
			}
			if ev.Seq <= lastSeq {
				continue
			}
			if !writeFrame(ctx, conn, ev.Payload) {
				return
			}
		}
	}
}

func writeFrame(ctx context.Context, conn *websocket.Conn, payload []byte) bool {
	writeCtx, cancel := context.WithTimeout(ctx, wsWriteTimeout)
	defer cancel()
	if err := conn.Write(writeCtx, websocket.MessageText, payload); err != nil {
		return false
	}
	return true
}
