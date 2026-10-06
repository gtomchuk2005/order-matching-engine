package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gtomchuk2005/order-matching-engine/gateway/internal/api"
	gwkafka "github.com/gtomchuk2005/order-matching-engine/gateway/internal/kafka"
	"github.com/gtomchuk2005/order-matching-engine/gateway/internal/store"
	"github.com/gtomchuk2005/order-matching-engine/gateway/internal/stream"
)

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// mustEnv has no fallback: a silently empty topic would let the gateway run against the wrong topic.
func mustEnv(name string) string {
	v := os.Getenv(name)
	if v == "" {
		log.Fatalf("gateway: required environment variable %s is not set", name)
	}
	return v
}

const restRequestTimeout = 10 * time.Second

// withRESTTimeouts applies the old ReadTimeout/WriteTimeout behavior per-request to
// everything except /stream, which must stay open for the life of the WebSocket.
func withRESTTimeouts(next http.Handler) http.Handler {
	timed := http.TimeoutHandler(next, restRequestTimeout, "request timed out")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/stream" {
			next.ServeHTTP(w, r)
			return
		}
		timed.ServeHTTP(w, r)
	})
}

func main() {
	httpAddr := envOr("HTTP_ADDR", ":8080")
	brokers := strings.Split(envOr("KAFKA_BROKERS", "kafka:9092"), ",")
	ordersTopic := mustEnv("ORDERS_TOPIC")
	deltasTopic := mustEnv("DELTAS_TOPIC")
	redisAddr := envOr("REDIS_ADDR", "redis:6379")

	idemTTL, err := time.ParseDuration(envOr("IDEM_TTL", "24h"))
	if err != nil {
		log.Fatalf("gateway: invalid IDEM_TTL: %v", err)
	}

	producer, err := gwkafka.NewProducer(brokers, ordersTopic)
	if err != nil {
		log.Fatalf("gateway: failed to create kafka producer: %v", err)
	}
	defer producer.Close()

	redisStore := store.New(redisAddr)
	defer redisStore.Close()

	hub := stream.NewHub()
	consumer, err := stream.NewConsumer(brokers, deltasTopic, hub)
	if err != nil {
		log.Fatalf("gateway: failed to create kafka consumer: %v", err)
	}
	defer consumer.Close()

	handler := api.New(producer, redisStore, hub, idemTTL)

	// WriteTimeout/ReadTimeout are unset here because a WebSocket stream is long-lived;
	// REST handlers instead get per-request deadlines applied explicitly below.
	server := &http.Server{
		Addr:              httpAddr,
		Handler:           withRESTTimeouts(handler.Routes()),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go consumer.Run(ctx)

	go func() {
		log.Printf("gateway: listening on %s", httpAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("gateway: server error: %v", err)
		}
	}()

	<-ctx.Done()
	hub.Close()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("gateway: shutdown error: %v", err)
	}
}
