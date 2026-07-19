package httpfault_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Tharun-bot/faultline/core"
	"github.com/Tharun-bot/faultline/interceptors/httpfault"
)

// staticSource is a trivial RuleSource for these tests, same pattern
// as grpcfault.StaticRuleSource in Phase 3.
type staticSource struct {
	matcher *core.Matcher
}

func newStaticSource(rules []core.Rule) *staticSource {
	return &staticSource{matcher: core.NewMatcher(rules)}
}

func (s *staticSource) Find(cc core.CallContext) (core.Rule, bool) {
	return s.matcher.Find(cc)
}

// realHandler is our trivial "business logic" — mirrors the
// toyservice OrderService.Create handler but as plain HTTP.
func realHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"order_id":"order-real","status":"created"}`))
	})
}

func TestMiddleware_LatencyInjection_ActuallyDelays(t *testing.T) {
	rules := []core.Rule{
		{
			ID:          "lat",
			Target:      core.Target{Service: "OrderService", Method: "/orders/create", Client: "*"},
			FaultType:   core.FaultLatency,
			Params:      core.Params{LatencyMS: 150},
			Probability: 1.0,
			Active:      true,
		},
	}
	mw := httpfault.Middleware("OrderService", newStaticSource(rules))
	server := httptest.NewServer(mw(realHandler()))
	defer server.Close()

	start := time.Now()
	resp, err := http.Get(server.URL + "/orders/create")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if elapsed < 130*time.Millisecond {
		t.Fatalf("expected ~150ms delay from injected latency, took %v", elapsed)
	}
}

func TestMiddleware_ErrorInjection_ShortCircuits(t *testing.T) {
	rules := []core.Rule{
		{
			ID:          "err",
			Target:      core.Target{Service: "OrderService", Method: "/orders/create", Client: "*"},
			FaultType:   core.FaultError,
			Params:      core.Params{ErrorCode: "UNAVAILABLE"},
			Probability: 1.0,
			Active:      true,
		},
	}
	mw := httpfault.Middleware("OrderService", newStaticSource(rules))
	server := httptest.NewServer(mw(realHandler()))
	defer server.Close()

	resp, err := http.Get(server.URL + "/orders/create")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}
}

func TestMiddleware_CorruptPayload_MutatesBody(t *testing.T) {
	rules := []core.Rule{
		{
			ID:          "corrupt",
			Target:      core.Target{Service: "OrderService", Method: "/orders/create", Client: "*"},
			FaultType:   core.FaultCorruptPayload,
			Params:      core.Params{CorruptPct: 50},
			Probability: 1.0,
			Active:      true,
		},
	}
	mw := httpfault.Middleware("OrderService", newStaticSource(rules))
	server := httptest.NewServer(mw(realHandler()))
	defer server.Close()

	resp, err := http.Get(server.URL + "/orders/create")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	original := `{"order_id":"order-real","status":"created"}`

	if len(body) != len(original) {
		t.Fatalf("expected same length, got %d vs %d", len(body), len(original))
	}
	if string(body) == original {
		t.Fatal("expected body to be corrupted, but it matches the original exactly")
	}
}

func TestMiddleware_NoMatchingRule_PassesThroughFast(t *testing.T) {
	rules := []core.Rule{
		{
			ID:          "irrelevant",
			Target:      core.Target{Service: "SomeOtherService", Method: "/whatever", Client: "*"},
			FaultType:   core.FaultLatency,
			Params:      core.Params{LatencyMS: 5000},
			Probability: 1.0,
			Active:      true,
		},
	}
	mw := httpfault.Middleware("OrderService", newStaticSource(rules))
	server := httptest.NewServer(mw(realHandler()))
	defer server.Close()

	start := time.Now()
	resp, err := http.Get(server.URL + "/orders/create")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if elapsed > 50*time.Millisecond {
		t.Fatalf("expected fast passthrough, took %v", elapsed)
	}
}
