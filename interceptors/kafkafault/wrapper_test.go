package kafkafault_test

import (
	"context"
	"testing"
	"time"

	"github.com/Tharun-bot/faultline/core"
	"github.com/Tharun-bot/faultline/interceptors/kafkafault"
)

// staticSource is a trivial RuleSource for these tests, same pattern
// used across grpcfault/httpfault tests.
type staticSource struct {
	matcher *core.Matcher
}

func newStaticSource(rules []core.Rule) *staticSource {
	return &staticSource{matcher: core.NewMatcher(rules)}
}

func (s *staticSource) Find(cc core.CallContext) (core.Rule, bool) {
	return s.matcher.Find(cc)
}

func TestWrapMessageHandler_LatencyInjection_ActuallyDelays(t *testing.T) {
	rules := []core.Rule{
		{
			ID:          "lat",
			Target:      core.Target{Service: "OrderConsumer", Method: "orders.created", Client: "*"},
			FaultType:   core.FaultLatency,
			Params:      core.Params{LatencyMS: 100},
			Probability: 1.0,
			Active:      true,
		},
	}
	w := kafkafault.NewWrapper("OrderConsumer", "order-group", newStaticSource(rules), nil)

	called := false
	handler := w.WrapMessageHandler(func(ctx context.Context, msg kafkafault.Message) error {
		called = true
		return nil
	})

	start := time.Now()
	err := handler(context.Background(), kafkafault.Message{Topic: "orders.created", Value: []byte("hi")})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !called {
		t.Fatal("expected real handler to still be called after latency")
	}
	if elapsed < 80*time.Millisecond {
		t.Fatalf("expected ~100ms delay, took %v", elapsed)
	}
}

func TestWrapMessageHandler_ErrorInjection_SkipsRealHandler(t *testing.T) {
	rules := []core.Rule{
		{
			ID:          "err",
			Target:      core.Target{Service: "OrderConsumer", Method: "orders.created", Client: "*"},
			FaultType:   core.FaultError,
			Params:      core.Params{ErrorCode: "INTERNAL"},
			Probability: 1.0,
			Active:      true,
		},
	}
	w := kafkafault.NewWrapper("OrderConsumer", "order-group", newStaticSource(rules), nil)

	called := false
	handler := w.WrapMessageHandler(func(ctx context.Context, msg kafkafault.Message) error {
		called = true
		return nil
	})

	err := handler(context.Background(), kafkafault.Message{Topic: "orders.created"})
	if err == nil {
		t.Fatal("expected injected error")
	}
	if called {
		t.Fatal("expected real handler NOT to be called on error injection")
	}
}

func TestWrapMessageHandler_CorruptPayload_MutatesValueBeforeHandler(t *testing.T) {
	rules := []core.Rule{
		{
			ID:          "corrupt",
			Target:      core.Target{Service: "OrderConsumer", Method: "orders.created", Client: "*"},
			FaultType:   core.FaultCorruptPayload,
			Params:      core.Params{CorruptPct: 100},
			Probability: 1.0,
			Active:      true,
		},
	}
	w := kafkafault.NewWrapper("OrderConsumer", "order-group", newStaticSource(rules), nil)

	original := []byte("perfectly valid payload")
	var received []byte
	handler := w.WrapMessageHandler(func(ctx context.Context, msg kafkafault.Message) error {
		received = msg.Value
		return nil
	})

	err := handler(context.Background(), kafkafault.Message{Topic: "orders.created", Value: original})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(received) == string(original) {
		t.Fatal("expected handler to receive corrupted value, got original")
	}
	if len(received) != len(original) {
		t.Fatalf("expected same length, got %d vs %d", len(received), len(original))
	}
}

func TestWrapMessageHandler_NoMatchingRule_PassesThroughUnchanged(t *testing.T) {
	rules := []core.Rule{
		{
			ID:          "irrelevant",
			Target:      core.Target{Service: "OtherConsumer", Method: "other.topic", Client: "*"},
			FaultType:   core.FaultLatency,
			Params:      core.Params{LatencyMS: 5000},
			Probability: 1.0,
			Active:      true,
		},
	}
	w := kafkafault.NewWrapper("OrderConsumer", "order-group", newStaticSource(rules), nil)

	handler := w.WrapMessageHandler(func(ctx context.Context, msg kafkafault.Message) error {
		return nil
	})

	start := time.Now()
	err := handler(context.Background(), kafkafault.Message{Topic: "orders.created"})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed > 50*time.Millisecond {
		t.Fatalf("expected fast passthrough, took %v", elapsed)
	}
}

func TestWrapBatchHandler_PartialFailure_OnlyFirstKSucceed(t *testing.T) {
	rules := []core.Rule{
		{
			ID:          "partial",
			Target:      core.Target{Service: "OrderConsumer", Method: "orders.created", Client: "*"},
			FaultType:   core.FaultPartialFailure,
			Params:      core.Params{PartialOKCount: 3},
			Probability: 1.0,
			Active:      true,
		},
	}
	w := kafkafault.NewWrapper("OrderConsumer", "order-group", newStaticSource(rules), nil)

	var handledCount int
	batchHandler := w.WrapBatchHandler(func(ctx context.Context, msgs []kafkafault.Message) error {
		handledCount++
		return nil
	})

	msgs := make([]kafkafault.Message, 5)
	for i := range msgs {
		msgs[i] = kafkafault.Message{Topic: "orders.created", Offset: int64(i)}
	}

	result := batchHandler(context.Background(), msgs)

	for i := 0; i < 3; i++ {
		if !result.Succeeded[i] {
			t.Fatalf("expected message %d to succeed", i)
		}
	}
	for i := 3; i < 5; i++ {
		if result.Succeeded[i] {
			t.Fatalf("expected message %d to fail", i)
		}
	}
	if handledCount != 3 {
		t.Fatalf("expected real handler called exactly 3 times, got %d", handledCount)
	}
}

func TestWrapBatchHandler_NonPartialFault_AppliesUniformly(t *testing.T) {
	rules := []core.Rule{
		{
			ID:          "batch-err",
			Target:      core.Target{Service: "OrderConsumer", Method: "orders.created", Client: "*"},
			FaultType:   core.FaultError,
			Params:      core.Params{ErrorCode: "INTERNAL"},
			Probability: 1.0,
			Active:      true,
		},
	}
	w := kafkafault.NewWrapper("OrderConsumer", "order-group", newStaticSource(rules), nil)

	batchHandler := w.WrapBatchHandler(func(ctx context.Context, msgs []kafkafault.Message) error {
		return nil
	})

	msgs := []kafkafault.Message{
		{Topic: "orders.created", Offset: 0},
		{Topic: "orders.created", Offset: 1},
	}
	result := batchHandler(context.Background(), msgs)

	for i, ok := range result.Succeeded {
		if ok {
			t.Fatalf("expected message %d to fail uniformly under error injection, but it succeeded", i)
		}
	}
}
