package hedge

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func reply(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}
func configured(t *testing.T, modify func(*Config)) *Gateway {
	t.Helper()
	c := DefaultConfig("http://primary.test", "http://secondary.test")
	c.HedgeDelay = time.Millisecond
	c.Timeout = time.Second
	c.HedgeFraction = 1
	modify(&c)
	g, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(g.CloseIdleConnections)
	return g
}
func wait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}
func TestHedgeWinsAndCancelsLosingRequest(t *testing.T) {
	cancelled := make(chan struct{})
	g := configured(t, func(c *Config) {
		c.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
			if r.URL.Host == "primary.test" {
				<-r.Context().Done()
				close(cancelled)
				return nil, r.Context().Err()
			}
			return reply(200, "winner"), nil
		})
	})
	w := httptest.NewRecorder()
	g.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	if w.Code != 200 || w.Body.String() != "winner" {
		t.Fatalf("unexpected result: %v", w)
	}
	wait(t, cancelled)
	if m := g.Metrics(); m.Hedges != 1 || m.HedgeWins != 1 || m.BackendFailures != 0 {
		t.Fatalf("metrics: %+v", m)
	}
}
func TestFastPrimaryDoesNotLaunchHedge(t *testing.T) {
	g := configured(t, func(c *Config) {
		c.HedgeDelay = time.Hour
		c.Transport = transportFunc(func(*http.Request) (*http.Response, error) { return reply(200, "fast"), nil })
	})
	g.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
	if m := g.Metrics(); m.Attempts != 1 || m.Hedges != 0 {
		t.Fatalf("metrics: %+v", m)
	}
}
func TestEarlyFailureFallsBackBeforeHedgeTimer(t *testing.T) {
	g := configured(t, func(c *Config) {
		c.HedgeDelay = time.Hour
		c.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
			if r.URL.Host == "primary.test" {
				return reply(503, "unavailable"), nil
			}
			return reply(200, "recovered"), nil
		})
	})
	w := httptest.NewRecorder()
	g.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	if w.Code != 200 || g.Metrics().Attempts != 2 {
		t.Fatalf("fallback failed: %v", w)
	}
}
func TestNoHedgingUnsafeMethodsOrBodies(t *testing.T) {
	var attempts atomic.Int64
	g := configured(t, func(c *Config) {
		c.Transport = transportFunc(func(*http.Request) (*http.Response, error) { attempts.Add(1); return reply(200, ""), nil })
	})
	for _, method := range []string{"POST", "PUT", "DELETE", "PATCH"} {
		w := httptest.NewRecorder()
		g.ServeHTTP(w, httptest.NewRequest(method, "/", nil))
		if w.Code != 405 {
			t.Fatal(w.Code)
		}
	}
	w := httptest.NewRecorder()
	g.ServeHTTP(w, httptest.NewRequest("GET", "/", strings.NewReader("body")))
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
	if attempts.Load() != 0 {
		t.Fatal("unexpected attempt")
	}
}
func TestRequestOverloadIsRejectedWithoutQueueing(t *testing.T) {
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	g := configured(t, func(c *Config) {
		c.MaxRequests = 1
		c.HedgeFraction = 0
		c.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
			close(started)
			select {
			case <-release:
				return reply(200, "ok"), nil
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		})
	})
	go func() { g.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil)); close(done) }()
	wait(t, started)
	w := httptest.NewRecorder()
	g.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	close(release)
	wait(t, done)
	if w.Code != 503 || g.Metrics().Rejected != 1 {
		t.Fatalf("overload: %v", w)
	}
}
func TestDeadlineReleasesAttempts(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(2)
	g := configured(t, func(c *Config) {
		c.Timeout = 30 * time.Millisecond
		c.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
			defer wg.Done()
			<-r.Context().Done()
			return nil, r.Context().Err()
		})
	})
	w := httptest.NewRecorder()
	g.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	wait(t, done)
	if w.Code != 504 || g.Metrics().Timeouts != 1 {
		t.Fatalf("deadline: %v", w)
	}
}
func TestOversizedResponseRejected(t *testing.T) {
	g := configured(t, func(c *Config) {
		c.MaxResponseBytes = 4
		c.HedgeFraction = 0
		c.Transport = transportFunc(func(*http.Request) (*http.Response, error) { return reply(200, "12345"), nil })
	})
	w := httptest.NewRecorder()
	g.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	if w.Code != 502 {
		t.Fatal(w.Code)
	}
}
func TestBackendErrorDoesNotLeakDetails(t *testing.T) {
	g := configured(t, func(c *Config) {
		c.HedgeFraction = 0
		c.Transport = transportFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("secret endpoint detail") })
	})
	w := httptest.NewRecorder()
	g.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	if w.Code != 502 || strings.Contains(w.Body.String(), "secret") {
		t.Fatal(w.Body.String())
	}
}
func TestHopHeadersAndEscapedPath(t *testing.T) {
	g := configured(t, func(c *Config) {
		c.HedgeFraction = 0
		c.Backends = []string{"http://primary.test/base"}
		c.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
			if r.Header.Get("X-Remove") != "" || r.Header.Get("Connection") != "" {
				t.Error("hop headers forwarded")
			}
			if r.URL.EscapedPath() != "/base/a%2Fb" || r.URL.RawQuery != "q=1" {
				t.Errorf("URL: %v", r.URL)
			}
			out := reply(200, "ok")
			out.Header.Set("Connection", "X-Internal")
			out.Header.Set("X-Internal", "hidden")
			return out, nil
		})
	})
	r := httptest.NewRequest("GET", "/a%2Fb?q=1", nil)
	r.Header.Set("Connection", "X-Remove")
	r.Header.Set("X-Remove", "hidden")
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Header().Get("X-Internal") != "" || w.Header().Get("Connection") != "" {
		t.Fatal(w.Header())
	}
}
func TestAggregateAmplificationBoundUnderConcurrency(t *testing.T) {
	g := configured(t, func(c *Config) {
		c.MaxRequests = 256
		c.MaxAttempts = 512
		c.HedgeFraction = .1
		c.HedgeBurst = 2
		c.Transport = transportFunc(func(*http.Request) (*http.Response, error) { return reply(503, "fail"), nil })
	})
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); g.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil)) }()
	}
	wg.Wait()
	m := g.Metrics()
	if m.Hedges > 2+m.Requests/10 || m.Attempts > m.Requests+m.Hedges {
		t.Fatalf("amplification unbounded: %+v", m)
	}
}
func TestAttemptLimitUnderConcurrentLoad(t *testing.T) {
	var active, peak atomic.Int64
	g := configured(t, func(c *Config) {
		c.MaxRequests = 100
		c.MaxAttempts = 3
		c.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
			n := active.Add(1)
			defer active.Add(-1)
			for old := peak.Load(); n > old; old = peak.Load() {
				if peak.CompareAndSwap(old, n) {
					break
				}
			}
			select {
			case <-time.After(10 * time.Millisecond):
				return reply(200, "ok"), nil
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		})
	})
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); g.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil)) }()
	}
	wg.Wait()
	if peak.Load() > 3 {
		t.Fatal(peak.Load())
	}
}
func TestCallerCancellation(t *testing.T) {
	started := make(chan struct{})
	cancelled := make(chan struct{})
	g := configured(t, func(c *Config) {
		c.HedgeFraction = 0
		c.Transport = transportFunc(func(r *http.Request) (*http.Response, error) {
			close(started)
			<-r.Context().Done()
			close(cancelled)
			return nil, r.Context().Err()
		})
	})
	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest("GET", "/", nil).WithContext(ctx)
	done := make(chan struct{})
	go func() { g.ServeHTTP(httptest.NewRecorder(), r); close(done) }()
	wait(t, started)
	cancel()
	wait(t, done)
	wait(t, cancelled)
}
func TestRealHTTPTransport(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Backend", "yes")
		_, _ = w.Write([]byte("network"))
	}))
	defer backend.Close()
	g := configured(t, func(c *Config) { c.Backends = []string{backend.URL} })
	front := httptest.NewServer(g)
	defer front.Close()
	r, err := http.Get(front.URL + "/read")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	body, _ := io.ReadAll(r.Body)
	if string(body) != "network" || r.Header.Get("X-Backend") != "yes" {
		t.Fatal(string(body))
	}
}
func TestInvalidConfig(t *testing.T) {
	for _, raw := range []string{"file:///tmp/test", "http://user:pass@host", "http://host?q=1", "/relative"} {
		c := DefaultConfig(raw)
		if _, err := New(c); err == nil {
			t.Fatal(raw)
		}
	}
}
