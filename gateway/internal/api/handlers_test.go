package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeStore struct {
	mu       sync.Mutex
	data     map[string]string
	failErr  error
	failNext bool
}

func newFakeStore() *fakeStore {
	return &fakeStore{data: make(map[string]string)}
}

func (s *fakeStore) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failErr != nil {
		return false, s.failErr
	}
	if _, exists := s.data[key]; exists {
		return false, nil
	}
	s.data[key] = value
	return true, nil
}

func (s *fakeStore) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failErr != nil {
		return s.failErr
	}
	s.data[key] = value
	return nil
}

func (s *fakeStore) Get(ctx context.Context, key string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failErr != nil {
		return "", false, s.failErr
	}
	v, ok := s.data[key]
	return v, ok, nil
}

type producedRecord struct {
	key   string
	value []byte
}

type fakeProducer struct {
	mu        sync.Mutex
	records   []producedRecord
	failErr   error
	callCount int
}

func (p *fakeProducer) Produce(ctx context.Context, key string, value []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.callCount++
	if p.failErr != nil {
		return p.failErr
	}
	p.records = append(p.records, producedRecord{key: key, value: value})
	return nil
}

func newTestHandler() (*Handler, *fakeStore, *fakeProducer) {
	s := newFakeStore()
	p := &fakeProducer{}
	h := New(p, s, 24*time.Hour)
	return h, s, p
}

func postOrder(t *testing.T, h *Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)
	return rec
}

func TestPostOrderHappyPath(t *testing.T) {
	h, _, p := newTestHandler()
	body := `{"symbol":"AAPL","side":"buy","price":10050,"qty":10,"client_order_id":"c1"}`
	rec := postOrder(t, h, body)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201, body=%s", rec.Code, rec.Body.String())
	}

	var resp map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp["symbol"] != "AAPL" || resp["status"] != "accepted" || resp["client_order_id"] != "c1" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if resp["order_id"] == "" {
		t.Fatal("expected order_id to be set")
	}

	if len(p.records) != 1 {
		t.Fatalf("expected 1 produced record, got %d", len(p.records))
	}
	if p.records[0].key != "AAPL" {
		t.Fatalf("produced key = %q, want AAPL", p.records[0].key)
	}

	var msg struct {
		Type        string          `json:"type"`
		Symbol      string          `json:"symbol"`
		OrderID     string          `json:"order_id"`
		Side        string          `json:"side"`
		Price       json.Number     `json:"price"`
		Qty         json.Number     `json:"qty"`
		IngressTsNs json.Number     `json:"ingress_ts_ns"`
		Extra       json.RawMessage `json:"extra,omitempty"`
	}
	dec := json.NewDecoder(bytes.NewReader(p.records[0].value))
	dec.UseNumber()
	if err := dec.Decode(&msg); err != nil {
		t.Fatalf("unmarshal produced value: %v", err)
	}
	if msg.Type != "new" || msg.Symbol != "AAPL" || msg.Side != "buy" {
		t.Fatalf("unexpected message: %+v", msg)
	}
	if msg.OrderID != resp["order_id"] {
		t.Fatalf("order_id mismatch: %s vs %s", msg.OrderID, resp["order_id"])
	}
	if price, err := msg.Price.Int64(); err != nil || price != 10050 {
		t.Fatalf("price = %v, err %v", msg.Price, err)
	}
	if qty, err := msg.Qty.Int64(); err != nil || qty != 10 {
		t.Fatalf("qty = %v, err %v", msg.Qty, err)
	}
	if _, err := msg.IngressTsNs.Int64(); err != nil {
		t.Fatalf("ingress_ts_ns not an integer: %v", err)
	}
}

func TestPostOrderDuplicateClientOrderID(t *testing.T) {
	h, _, p := newTestHandler()
	body := `{"symbol":"AAPL","side":"buy","price":10050,"qty":10,"client_order_id":"dup1"}`

	rec1 := postOrder(t, h, body)
	if rec1.Code != http.StatusCreated {
		t.Fatalf("first request status = %d, want 201", rec1.Code)
	}
	var resp1 map[string]string
	_ = json.Unmarshal(rec1.Body.Bytes(), &resp1)
	firstOrderID := resp1["order_id"]

	rec2 := postOrder(t, h, body)
	if rec2.Code != http.StatusConflict {
		t.Fatalf("second request status = %d, want 409, body=%s", rec2.Code, rec2.Body.String())
	}
	var resp2 map[string]string
	_ = json.Unmarshal(rec2.Body.Bytes(), &resp2)
	if resp2["order_id"] != firstOrderID {
		t.Fatalf("order_id = %q, want %q", resp2["order_id"], firstOrderID)
	}

	if p.callCount != 1 {
		t.Fatalf("producer called %d times, want 1", p.callCount)
	}
}

func TestPostOrderValidationFailures(t *testing.T) {
	cases := []struct {
		name  string
		body  string
		field string
	}{
		{"bad symbol", `{"symbol":"aapl","side":"buy","price":1,"qty":1,"client_order_id":"c"}`, "symbol"},
		{"bad side", `{"symbol":"AAPL","side":"hold","price":1,"qty":1,"client_order_id":"c"}`, "side"},
		{"zero price", `{"symbol":"AAPL","side":"buy","price":0,"qty":1,"client_order_id":"c"}`, "price"},
		{"zero qty", `{"symbol":"AAPL","side":"buy","price":1,"qty":0,"client_order_id":"c"}`, "qty"},
		{"empty client_order_id", `{"symbol":"AAPL","side":"buy","price":1,"qty":1,"client_order_id":""}`, "client_order_id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, _, _ := newTestHandler()
			rec := postOrder(t, h, tc.body)
			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want 422, body=%s", rec.Code, rec.Body.String())
			}
			var resp map[string]string
			_ = json.Unmarshal(rec.Body.Bytes(), &resp)
			if resp["field"] != tc.field {
				t.Fatalf("field = %q, want %q", resp["field"], tc.field)
			}
		})
	}
}

func TestPostOrderFloatPriceRejected(t *testing.T) {
	h, _, p := newTestHandler()
	body := `{"symbol":"AAPL","side":"buy","price":100.7,"qty":1,"client_order_id":"c"}`
	rec := postOrder(t, h, body)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422, body=%s", rec.Code, rec.Body.String())
	}
	if p.callCount != 0 {
		t.Fatalf("producer should not have been called")
	}
}

func TestPostOrderUnknownField(t *testing.T) {
	h, _, _ := newTestHandler()
	body := `{"symbol":"AAPL","side":"buy","price":1,"qty":1,"client_order_id":"c","bogus":true}`
	rec := postOrder(t, h, body)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422, body=%s", rec.Code, rec.Body.String())
	}
}

func TestPostOrderOversizedBody(t *testing.T) {
	h, _, _ := newTestHandler()
	huge := strings.Repeat("a", 9*1024)
	body := `{"symbol":"AAPL","side":"buy","price":1,"qty":1,"client_order_id":"` + huge + `"}`
	rec := postOrder(t, h, body)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422, body=%s", rec.Code, rec.Body.String())
	}
}

func TestPostOrderTrailingData(t *testing.T) {
	h, _, _ := newTestHandler()
	body := `{"symbol":"AAPL","side":"buy","price":1,"qty":1,"client_order_id":"c"}{"extra":1}`
	rec := postOrder(t, h, body)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422, body=%s", rec.Code, rec.Body.String())
	}
}

func TestDeleteOrderUnknown(t *testing.T) {
	h, _, p := newTestHandler()
	req := httptest.NewRequest(http.MethodDelete, "/orders/unknown-id", nil)
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404, body=%s", rec.Code, rec.Body.String())
	}
	if p.callCount != 0 {
		t.Fatal("producer should not have been called")
	}
}

func TestDeleteOrderKnown(t *testing.T) {
	h, s, p := newTestHandler()
	_ = s.Set(context.Background(), "order:abc123", "AAPL", 0)

	req := httptest.NewRequest(http.MethodDelete, "/orders/abc123", nil)
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202, body=%s", rec.Code, rec.Body.String())
	}
	var resp map[string]string
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp["status"] != "cancel_accepted" || resp["symbol"] != "AAPL" {
		t.Fatalf("unexpected response: %+v", resp)
	}

	if len(p.records) != 1 {
		t.Fatalf("expected 1 produced record, got %d", len(p.records))
	}
	if p.records[0].key != "AAPL" {
		t.Fatalf("produced key = %q, want AAPL", p.records[0].key)
	}
	var msg map[string]any
	_ = json.Unmarshal(p.records[0].value, &msg)
	if msg["type"] != "cancel" {
		t.Fatalf("unexpected cancel message: %+v", msg)
	}
}

func TestGetBookHit(t *testing.T) {
	h, s, _ := newTestHandler()
	snapshot := `{"symbol":"AAPL","seq":3,"bids":[[10000,6]],"asks":[]}`
	_ = s.Set(context.Background(), "book:AAPL", snapshot, 0)

	req := httptest.NewRequest(http.MethodGet, "/book/AAPL", nil)
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != snapshot {
		t.Fatalf("body = %q, want exact byte match %q", rec.Body.String(), snapshot)
	}
}

func TestGetBookMiss(t *testing.T) {
	h, _, _ := newTestHandler()
	req := httptest.NewRequest(http.MethodGet, "/book/AAPL", nil)
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestPostOrderRedisError(t *testing.T) {
	h, s, _ := newTestHandler()
	s.failErr = errors.New("boom")
	rec := postOrder(t, h, `{"symbol":"AAPL","side":"buy","price":1,"qty":1,"client_order_id":"c"}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503, body=%s", rec.Code, rec.Body.String())
	}
}

func TestPostOrderProduceError(t *testing.T) {
	h, _, p := newTestHandler()
	p.failErr = errors.New("broker unreachable")
	rec := postOrder(t, h, `{"symbol":"AAPL","side":"buy","price":1,"qty":1,"client_order_id":"c"}`)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502, body=%s", rec.Code, rec.Body.String())
	}
}

func TestPostOrderDifferentClientOrderIDsGetDifferentOrderIDs(t *testing.T) {
	h, _, _ := newTestHandler()
	rec1 := postOrder(t, h, `{"symbol":"AAPL","side":"buy","price":1,"qty":1,"client_order_id":"c1"}`)
	rec2 := postOrder(t, h, `{"symbol":"AAPL","side":"buy","price":1,"qty":1,"client_order_id":"c2"}`)

	var resp1, resp2 map[string]string
	_ = json.Unmarshal(rec1.Body.Bytes(), &resp1)
	_ = json.Unmarshal(rec2.Body.Bytes(), &resp2)

	if resp1["order_id"] == "" || resp2["order_id"] == "" {
		t.Fatal("expected both order_ids to be set")
	}
	if resp1["order_id"] == resp2["order_id"] {
		t.Fatal("expected different order_ids for different client_order_ids")
	}
}
