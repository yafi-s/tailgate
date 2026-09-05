package hedge

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func BenchmarkGatewayFastPath(b *testing.B) {
	c := DefaultConfig("http://replica.test")
	c.HedgeDelay = time.Hour
	c.HedgeFraction = 0
	c.Transport = transportFunc(func(*http.Request) (*http.Response, error) { return reply(200, "small-response"), nil })
	g, err := New(c)
	if err != nil {
		b.Fatal(err)
	}
	defer g.CloseIdleConnections()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/read", nil))
	}
}
