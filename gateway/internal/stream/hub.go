// Package stream fans out engine delta/trade events from Kafka to WebSocket subscribers.
package stream

import "sync"

// Hub routes published events to the subscribers registered for each symbol.
type Hub struct {
	mu   sync.RWMutex
	subs map[string]map[*Subscriber]struct{}
}

func NewHub() *Hub {
	return &Hub{subs: make(map[string]map[*Subscriber]struct{})}
}

func (h *Hub) Subscribe(symbol string) *Subscriber {
	s := newSubscriber()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subs[symbol] == nil {
		h.subs[symbol] = make(map[*Subscriber]struct{})
	}
	h.subs[symbol][s] = struct{}{}
	return s
}

func (h *Hub) Unsubscribe(symbol string, s *Subscriber) {
	s.close()
	h.mu.Lock()
	defer h.mu.Unlock()
	if set, ok := h.subs[symbol]; ok {
		delete(set, s)
		if len(set) == 0 {
			delete(h.subs, symbol)
		}
	}
}

// Publish never blocks: it only takes RLock, so a dead subscriber (full buffer) is
// collected here and removed in a separate Lock pass rather than upgraded in place.
func (h *Hub) Publish(symbol string, seq uint64, payload []byte) {
	ev := Event{Seq: seq, Payload: payload}

	h.mu.RLock()
	set := h.subs[symbol]
	var dead []*Subscriber
	for s := range set {
		if !s.send(ev) {
			dead = append(dead, s)
		}
	}
	h.mu.RUnlock()

	if len(dead) == 0 {
		return
	}
	h.mu.Lock()
	if set, ok := h.subs[symbol]; ok {
		for _, s := range dead {
			delete(set, s)
		}
		if len(set) == 0 {
			delete(h.subs, symbol)
		}
	}
	h.mu.Unlock()
	for _, s := range dead {
		s.close()
	}
}

// Close shuts down every subscriber across every symbol so writer goroutines exit.
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for symbol, set := range h.subs {
		for s := range set {
			s.close()
		}
		delete(h.subs, symbol)
	}
}
