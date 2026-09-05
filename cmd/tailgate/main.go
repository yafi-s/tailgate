package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/yafi-s/tailgate/hedge"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:8080", "gateway address")
	admin := flag.String("admin", "127.0.0.1:9090", "separate health/metrics address")
	backends := flag.String("backends", "http://127.0.0.1:8081,http://127.0.0.1:8082", "comma-separated equivalent replicas")
	delay := flag.Duration("hedge-delay", 25*time.Millisecond, "delay before a duplicate read")
	fraction := flag.Float64("hedge-fraction", .1, "maximum earned extra attempts per admission")
	timeout := flag.Duration("timeout", time.Second, "total upstream deadline")
	requests := flag.Int("max-requests", 128, "admitted request limit")
	attempts := flag.Int("max-attempts", 160, "active upstream attempt limit")
	flag.Parse()
	c := hedge.DefaultConfig(strings.Split(*backends, ",")...)
	c.HedgeDelay = *delay
	c.HedgeFraction = *fraction
	c.Timeout = *timeout
	c.MaxRequests = *requests
	c.MaxAttempts = *attempts
	g, err := hedge.New(c)
	if err != nil {
		log.Fatal(err)
	}
	defer g.CloseIdleConnections()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(g.Metrics())
	})
	front := &http.Server{Addr: *listen, Handler: g, ReadHeaderTimeout: 5 * time.Second, WriteTimeout: c.Timeout + 5*time.Second, IdleTimeout: time.Minute, MaxHeaderBytes: 64 << 10}
	control := &http.Server{Addr: *admin, Handler: mux, ReadHeaderTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second, IdleTimeout: 30 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errors := make(chan error, 2)
	go func() { log.Printf("gateway listening on %s", *listen); errors <- front.ListenAndServe() }()
	go func() { errors <- control.ListenAndServe() }()
	select {
	case <-ctx.Done():
	case err := <-errors:
		if err != http.ErrServerClosed {
			log.Printf("server: %v", err)
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = front.Shutdown(shutdown)
	_ = control.Shutdown(shutdown)
}
