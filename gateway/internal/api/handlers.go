// Package api implements the HTTP order-intake gateway: validation,
// idempotency, and translation into the engine's Kafka message schema.
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"
)

const maxBodyBytes = 8 * 1024

type Producer interface {
	Produce(ctx context.Context, key string, value []byte) error
}

// A key miss is (_, false, nil), never a leaked redis.Nil.
type Store interface {
	SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	Set(ctx context.Context, key, value string, ttl time.Duration) error
	Get(ctx context.Context, key string) (string, bool, error)
}

type Handler struct {
	Producer Producer
	Store    Store
	IdemTTL  time.Duration
}

func New(producer Producer, store Store, idemTTL time.Duration) *Handler {
	return &Handler{Producer: producer, Store: store, IdemTTL: idemTTL}
}

func (h *Handler) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", h.handlePostOrder)
	mux.HandleFunc("DELETE /orders/{id}", h.handleDeleteOrder)
	mux.HandleFunc("GET /book/{symbol}", h.handleGetBook)
	return mux
}

type orderRequest struct {
	Symbol        string      `json:"symbol"`
	Side          string      `json:"side"`
	Price         json.Number `json:"price"`
	Qty           json.Number `json:"qty"`
	ClientOrderID string      `json:"client_order_id"`
}

type newOrderMessage struct {
	Type        string `json:"type"`
	Symbol      string `json:"symbol"`
	OrderID     string `json:"order_id"`
	Side        string `json:"side"`
	Price       int64  `json:"price"`
	Qty         int64  `json:"qty"`
	IngressTsNs int64  `json:"ingress_ts_ns"`
}

type cancelOrderMessage struct {
	Type        string `json:"type"`
	Symbol      string `json:"symbol"`
	OrderID     string `json:"order_id"`
	IngressTsNs int64  `json:"ingress_ts_ns"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeValidationError(w http.ResponseWriter, verr *ValidationError) {
	writeJSON(w, http.StatusUnprocessableEntity, map[string]string{
		"error": verr.Message,
		"field": verr.Field,
	})
}

func newOrderID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func (h *Handler) handlePostOrder(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	dec.UseNumber()

	var req orderRequest
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "invalid request body", "field": ""})
		return
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "trailing data after JSON body", "field": ""})
		return
	}

	if verr := validateSymbol(req.Symbol); verr != nil {
		writeValidationError(w, verr)
		return
	}
	if verr := validateSide(req.Side); verr != nil {
		writeValidationError(w, verr)
		return
	}
	price, verr := validatePrice(req.Price)
	if verr != nil {
		writeValidationError(w, verr)
		return
	}
	qty, verr := validateQty(req.Qty)
	if verr != nil {
		writeValidationError(w, verr)
		return
	}
	if verr := validateClientOrderID(req.ClientOrderID); verr != nil {
		writeValidationError(w, verr)
		return
	}

	ctx := r.Context()

	orderID, err := newOrderID()
	if err != nil {
		log.Printf("gateway: order id generation failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	idemKey := "idem:" + req.ClientOrderID
	created, err := h.Store.SetNX(ctx, idemKey, orderID, h.IdemTTL)
	if err != nil {
		log.Printf("gateway: redis SetNX failed: %v", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "storage unavailable"})
		return
	}
	if !created {
		existing, found, err := h.Store.Get(ctx, idemKey)
		if err != nil {
			log.Printf("gateway: redis Get failed: %v", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "storage unavailable"})
			return
		}
		resp := map[string]string{"error": "duplicate client_order_id", "client_order_id": req.ClientOrderID}
		if found {
			resp["order_id"] = existing
		}
		writeJSON(w, http.StatusConflict, resp)
		return
	}

	if err := h.Store.Set(ctx, "order:"+orderID, req.Symbol, h.IdemTTL); err != nil {
		log.Printf("gateway: redis Set failed: %v", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "storage unavailable"})
		return
	}

	msg := newOrderMessage{
		Type:        "new",
		Symbol:      req.Symbol,
		OrderID:     orderID,
		Side:        req.Side,
		Price:       price,
		Qty:         qty,
		IngressTsNs: time.Now().UnixNano(),
	}
	value, err := json.Marshal(msg)
	if err != nil {
		log.Printf("gateway: marshal new order message failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	if err := h.Producer.Produce(ctx, req.Symbol, value); err != nil {
		log.Printf("gateway: produce failed: %v", err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "failed to publish order"})
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"order_id":        orderID,
		"client_order_id": req.ClientOrderID,
		"symbol":          req.Symbol,
		"status":          "accepted",
	})
}

func (h *Handler) handleDeleteOrder(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ctx := r.Context()

	symbol, found, err := h.Store.Get(ctx, "order:"+id)
	if err != nil {
		log.Printf("gateway: redis Get failed: %v", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "storage unavailable"})
		return
	}
	if !found {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown order"})
		return
	}

	msg := cancelOrderMessage{
		Type:        "cancel",
		Symbol:      symbol,
		OrderID:     id,
		IngressTsNs: time.Now().UnixNano(),
	}
	value, err := json.Marshal(msg)
	if err != nil {
		log.Printf("gateway: marshal cancel message failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	if err := h.Producer.Produce(ctx, symbol, value); err != nil {
		log.Printf("gateway: produce failed: %v", err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "failed to publish cancel"})
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{
		"order_id": id,
		"symbol":   symbol,
		"status":   "cancel_accepted",
	})
}

func (h *Handler) handleGetBook(w http.ResponseWriter, r *http.Request) {
	symbol := r.PathValue("symbol")
	if verr := validateSymbol(symbol); verr != nil {
		writeValidationError(w, verr)
		return
	}

	body, found, err := h.Store.Get(r.Context(), "book:"+symbol)
	if err != nil {
		log.Printf("gateway: redis Get failed: %v", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "storage unavailable"})
		return
	}
	if !found {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown symbol"})
		return
	}

	// Write the stored bytes as-is: the engine owns this shape, re-marshaling would reorder keys.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(body))
}
