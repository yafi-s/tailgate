// A controlled, reproducible straggler experiment using real loopback HTTP.
// This is a closed-loop client: it does not estimate latency under overload.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/yafi-s/tailgate/hedge"
)

func backend(index int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, _ := strconv.Atoi(r.URL.Query().Get("id"))
		delay := 2 * time.Millisecond
		if id%10 == index {
			delay = 40 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
			return
		case <-timer.C:
		}
		_, _ = fmt.Fprint(w, "replicated-value")
	}))
}
func run(urls []string, n int, fraction float64) error {
	c := hedge.DefaultConfig(urls...)
	c.HedgeDelay = 5 * time.Millisecond
	c.HedgeFraction = fraction
	c.HedgeBurst = 4
	g, err := hedge.New(c)
	if err != nil {
		return err
	}
	defer g.CloseIdleConnections()
	front := httptest.NewServer(g)
	defer front.Close()
	client := &http.Client{Timeout: 2 * time.Second}
	defer client.CloseIdleConnections()
	samples := make([]float64, 0, n)
	for i := 0; i < n; i++ {
		start := time.Now()
		r, err := client.Get(fmt.Sprintf("%s/read?id=%d", front.URL, i))
		if err != nil {
			return err
		}
		body, err := io.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			return err
		}
		if r.StatusCode != 200 || string(body) != "replicated-value" {
			return fmt.Errorf("invalid response: %d", r.StatusCode)
		}
		samples = append(samples, float64(time.Since(start).Microseconds())/1000)
	}
	sort.Float64s(samples)
	p := func(q float64) float64 { return samples[int(q*float64(len(samples)-1))] }
	mode := "baseline"
	if fraction > 0 {
		mode = "hedged"
	}
	m := g.Metrics()
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"mode": mode, "requests": n, "p50_ms": p(.5), "p95_ms": p(.95), "p99_ms": p(.99), "attempts_per_request": float64(m.Attempts) / float64(n), "metrics": m})
}
func main() {
	n := flag.Int("requests", 500, "requests per policy, minimum 100")
	flag.Parse()
	if *n < 100 {
		fmt.Fprintln(os.Stderr, "minimum 100 requests")
		os.Exit(2)
	}
	a, b := backend(0), backend(1)
	defer a.Close()
	defer b.Close()
	for _, fraction := range []float64{0, .25} {
		if err := run([]string{a.URL, b.URL}, *n, fraction); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}
