package stream

import "sync"

// subBufferSize bounds how far a slow client can lag before being dropped.
const subBufferSize = 256

// Event carries a record's seq alongside its original bytes so handlers can filter by seq.
type Event struct {
	Seq     uint64
	Payload []byte
}

// Subscriber is one client's channel of events for a symbol.
type Subscriber struct {
	ch        chan Event
	closeOnce sync.Once
	closed    chan struct{}
}

func newSubscriber() *Subscriber {
	return &Subscriber{
		ch:     make(chan Event, subBufferSize),
		closed: make(chan struct{}),
	}
}

// Events returns the receive-only channel of published events.
func (s *Subscriber) Events() <-chan Event {
	return s.ch
}

// Closed signals when the subscriber has been closed, for select loops to exit on.
func (s *Subscriber) Closed() <-chan struct{} {
	return s.closed
}

// close is idempotent: a send on an already-closed channel would panic.
func (s *Subscriber) close() {
	s.closeOnce.Do(func() {
		close(s.closed)
	})
}

// send is non-blocking: a full buffer means this subscriber is dropped, never that Publish blocks.
func (s *Subscriber) send(ev Event) bool {
	select {
	case <-s.closed:
		return false
	default:
	}
	select {
	case s.ch <- ev:
		return true
	default:
		return false
	}
}
