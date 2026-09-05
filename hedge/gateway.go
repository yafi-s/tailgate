// Package hedge provides a bounded, buffered read gateway for equivalent replicas.
package hedge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

type Config struct {
	Backends         []string
	HedgeDelay       time.Duration
	Timeout          time.Duration
	MaxRequests      int
	MaxAttempts      int
	MaxResponseBytes int64
	HedgeFraction    float64
	HedgeBurst       int
	FailureThreshold int
	Cooldown         time.Duration
	Transport        http.RoundTripper
}

func DefaultConfig(backends ...string) Config {
	return Config{Backends: backends, HedgeDelay: 25 * time.Millisecond, Timeout: time.Second,
		MaxRequests: 128, MaxAttempts: 160, MaxResponseBytes: 4 << 20,
		HedgeFraction: .1, HedgeBurst: 2, FailureThreshold: 5, Cooldown: time.Second}
}

type Metrics struct {
	Requests        uint64 `json:"requests"`
	Rejected        uint64 `json:"rejected"`
	Attempts        uint64 `json:"attempts"`
	Hedges          uint64 `json:"hedges"`
	HedgeWins       uint64 `json:"hedge_wins"`
	Timeouts        uint64 `json:"timeouts"`
	BackendFailures uint64 `json:"backend_failures"`
	InFlight        int64  `json:"in_flight"`
}
type counters struct {
	requests, rejected, attempts, hedges, wins, timeouts, failures atomic.Uint64
	inFlight                                                       atomic.Int64
}
type endpoint struct {
	url     *url.URL
	breaker breaker
}
type Gateway struct {
	config             Config
	endpoints          []*endpoint
	transport          http.RoundTripper
	requests, attempts chan struct{}
	budget             budget
	next               atomic.Uint64
	metrics            counters
}

func New(c Config) (*Gateway, error) {
	if len(c.Backends) == 0 || c.Timeout <= 0 || c.HedgeDelay < 0 || c.MaxRequests <= 0 || c.MaxAttempts <= 0 ||
		c.MaxResponseBytes <= 0 || c.MaxResponseBytes > 64<<20 || c.HedgeFraction < 0 || c.HedgeFraction > 1 ||
		math.IsNaN(c.HedgeFraction) || c.HedgeBurst < 1 || c.FailureThreshold < 1 || c.Cooldown <= 0 {
		return nil, errors.New("invalid gateway limits")
	}
	g := &Gateway{config: c, requests: make(chan struct{}, c.MaxRequests), attempts: make(chan struct{}, c.MaxAttempts),
		budget: budget{credits: float64(c.HedgeBurst), capacity: float64(c.HedgeBurst), fraction: c.HedgeFraction}}
	seen := map[string]bool{}
	for _, raw := range c.Backends {
		u, err := url.Parse(raw)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return nil, fmt.Errorf("invalid backend URL: %q", raw)
		}
		if seen[u.String()] {
			return nil, errors.New("duplicate replica URL")
		}
		seen[u.String()] = true
		g.endpoints = append(g.endpoints, &endpoint{url: u})
	}
	g.transport = c.Transport
	if g.transport == nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.Proxy = nil // explicit destinations; do not inherit a process-wide proxy
		t.MaxIdleConns = c.MaxAttempts
		t.MaxIdleConnsPerHost = c.MaxAttempts
		t.MaxConnsPerHost = c.MaxAttempts
		g.transport = t
	}
	return g, nil
}
func (g *Gateway) Metrics() Metrics {
	m := &g.metrics
	return Metrics{m.requests.Load(), m.rejected.Load(), m.attempts.Load(), m.hedges.Load(), m.wins.Load(), m.timeouts.Load(), m.failures.Load(), m.inFlight.Load()}
}
func (g *Gateway) CloseIdleConnections() {
	if closer, ok := g.transport.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

type response struct {
	status int
	header http.Header
	body   []byte
	err    error
	hedged bool
}

func stripHopHeaders(h http.Header) {
	for _, line := range h.Values("Connection") {
		for _, name := range strings.Split(line, ",") {
			h.Del(strings.TrimSpace(name))
		}
	}
	for _, name := range []string{"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade"} {
		h.Del(name)
	}
}

func (g *Gateway) attempt(ctx context.Context, incoming *http.Request, ep *endpoint, ticket ticket, extra bool, ch chan<- response) {
	defer func() { <-g.attempts; g.metrics.inFlight.Add(-1) }()
	u := *ep.url
	u.Path = strings.TrimRight(u.Path, "/") + incoming.URL.Path
	u.RawPath = strings.TrimRight(ep.url.EscapedPath(), "/") + incoming.URL.EscapedPath()
	u.RawQuery = incoming.URL.RawQuery
	req := incoming.Clone(ctx)
	req.URL = &u
	req.RequestURI = ""
	req.Host = u.Host
	req.Body = nil
	req.GetBody = nil
	req.ContentLength = 0
	stripHopHeaders(req.Header)
	// Avoid transparently decompressing an unbounded response before our limit.
	if req.Header.Get("Accept-Encoding") == "" {
		req.Header.Set("Accept-Encoding", "identity")
	}
	result := response{hedged: extra}
	r, err := g.transport.RoundTrip(req)
	if err != nil && r != nil && r.Body != nil {
		r.Body.Close()
	}
	if err == nil {
		result.status = r.StatusCode
		result.header = r.Header.Clone()
		result.body, err = io.ReadAll(io.LimitReader(r.Body, g.config.MaxResponseBytes+1))
		r.Body.Close()
		if int64(len(result.body)) > g.config.MaxResponseBytes {
			err = errors.New("response exceeds buffering limit")
			result.body = nil
		}
	}
	result.err = err
	state := success
	if ctx.Err() != nil {
		state = neutral
	} else if err != nil || result.status >= 500 {
		state = failure
		g.metrics.failures.Add(1)
	}
	ep.breaker.finish(ticket, state, g.config.FailureThreshold, g.config.Cooldown, time.Now())
	// Capacity two means a cancelled losing attempt can always send without a
	// reader, then release its permit. No drain goroutine can be stranded.
	ch <- result
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "read methods only", http.StatusMethodNotAllowed)
		return
	}
	if r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
		http.Error(w, "request bodies are unsupported", http.StatusBadRequest)
		return
	}
	select {
	case g.requests <- struct{}{}:
		defer func() { <-g.requests }()
	default:
		g.metrics.rejected.Add(1)
		http.Error(w, "request capacity exhausted", http.StatusServiceUnavailable)
		return
	}
	g.metrics.requests.Add(1)
	g.budget.earn()
	ctx, cancel := context.WithTimeout(r.Context(), g.config.Timeout)
	defer cancel()
	ch := make(chan response, 2)
	used := make(map[int]bool, 2)
	start := int((g.next.Add(1) - 1) % uint64(len(g.endpoints)))
	launch := func(extra bool) bool {
		if ctx.Err() != nil {
			return false
		}
		select {
		case g.attempts <- struct{}{}:
		default:
			return false
		}
		for offset := 0; offset < len(g.endpoints); offset++ {
			i := (start + offset) % len(g.endpoints)
			if used[i] {
				continue
			}
			ep := g.endpoints[i]
			ticket, ok := ep.breaker.acquire(time.Now())
			if !ok {
				continue
			}
			if extra && !g.budget.take() {
				ep.breaker.finish(ticket, neutral, g.config.FailureThreshold, g.config.Cooldown, time.Now())
				<-g.attempts
				return false
			}
			used[i] = true
			g.metrics.attempts.Add(1)
			g.metrics.inFlight.Add(1)
			if extra {
				g.metrics.hedges.Add(1)
			}
			go g.attempt(ctx, r, ep, ticket, extra, ch)
			return true
		}
		<-g.attempts
		return false
	}
	if !launch(false) {
		g.metrics.rejected.Add(1)
		http.Error(w, "no available replica capacity", http.StatusServiceUnavailable)
		return
	}
	pending := 1
	timer := time.NewTimer(g.config.HedgeDelay)
	defer timer.Stop()
	timerC := timer.C
	triedExtra := false
	var last response
	for pending > 0 {
		select {
		case <-ctx.Done():
			g.metrics.timeouts.Add(1)
			http.Error(w, "upstream deadline exceeded", http.StatusGatewayTimeout)
			return
		case <-timerC:
			timerC = nil
			triedExtra = true
			if launch(true) {
				pending++
			}
		case got := <-ch:
			pending--
			last = got
			if got.err == nil && got.status < 500 {
				if got.hedged {
					g.metrics.wins.Add(1)
				}
				cancel()
				g.respond(w, got)
				return
			}
			if !triedExtra {
				triedExtra = true
				timerC = nil
				timer.Stop()
				if launch(true) {
					pending++
				}
			}
		}
	}
	if ctx.Err() != nil {
		g.metrics.timeouts.Add(1)
		http.Error(w, "upstream deadline exceeded", http.StatusGatewayTimeout)
		return
	}
	if last.err != nil {
		http.Error(w, "upstream transport or response failure", http.StatusBadGateway)
		return
	}
	g.respond(w, last)
}
func (g *Gateway) respond(w http.ResponseWriter, r response) {
	stripHopHeaders(r.header)
	for name, values := range r.header {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}
	w.WriteHeader(r.status)
	_, _ = w.Write(r.body)
}
