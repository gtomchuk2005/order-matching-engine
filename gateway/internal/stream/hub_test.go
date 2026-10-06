package stream

import (
	"sync"
	"testing"
	"time"
)

func TestHubFanoutMultipleSubscribers(t *testing.T) {
	h := NewHub()
	s1 := h.Subscribe("AAPL")
	s2 := h.Subscribe("AAPL")

	h.Publish("AAPL", 1, []byte(`{"seq":1}`))

	for _, s := range []*Subscriber{s1, s2} {
		select {
		case ev := <-s.Events():
			if ev.Seq != 1 {
				t.Fatalf("seq = %d, want 1", ev.Seq)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for event")
		}
	}
}

func TestHubSymbolIsolation(t *testing.T) {
	h := NewHub()
	aapl := h.Subscribe("AAPL")
	bbb := h.Subscribe("BBB")

	h.Publish("AAPL", 1, []byte(`{"seq":1}`))

	select {
	case <-aapl.Events():
	case <-time.After(time.Second):
		t.Fatal("expected AAPL subscriber to receive event")
	}

	select {
	case ev := <-bbb.Events():
		t.Fatalf("BBB subscriber should not receive AAPL events, got %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestHubUnsubscribeRemoves(t *testing.T) {
	h := NewHub()
	s := h.Subscribe("AAPL")
	h.Unsubscribe("AAPL", s)

	h.mu.RLock()
	set := h.subs["AAPL"]
	h.mu.RUnlock()
	if len(set) != 0 {
		t.Fatalf("expected symbol removed/empty, got %d subscribers", len(set))
	}

	select {
	case <-s.Closed():
	default:
		t.Fatal("expected subscriber to be closed after unsubscribe")
	}
}

func TestHubPublishDoesNotBlockOnFullBuffer(t *testing.T) {
	h := NewHub()
	s := h.Subscribe("AAPL")

	// Fill the buffer without draining it.
	for i := 0; i < subBufferSize; i++ {
		h.Publish("AAPL", uint64(i), []byte("x"))
	}

	done := make(chan struct{})
	go func() {
		h.Publish("AAPL", 999, []byte("overflow"))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Publish blocked on a full subscriber buffer")
	}

	select {
	case <-s.Closed():
	case <-time.After(time.Second):
		t.Fatal("expected overflowed subscriber to be marked closed/dropped")
	}
}

func TestSubscriberDoubleCloseDoesNotPanic(t *testing.T) {
	s := newSubscriber()
	s.close()
	s.close()
}

func TestHubConcurrentSubscribePublishUnsubscribe(t *testing.T) {
	h := NewHub()
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sym := "AAPL"
			if i%2 == 0 {
				sym = "BBB"
			}
			s := h.Subscribe(sym)
			for j := 0; j < 50; j++ {
				h.Publish(sym, uint64(j), []byte("x"))
			}
			h.Unsubscribe(sym, s)
		}(i)
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			h.Publish("AAPL", uint64(i), []byte("y"))
			h.Publish("BBB", uint64(i), []byte("y"))
		}(i)
	}

	wg.Wait()
}
