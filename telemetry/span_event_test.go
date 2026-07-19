package telemetry_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/Tharun-bot/faultline/telemetry"
)

// newTestTracerProvider builds a TracerProvider backed by an in-memory
// span recorder, so tests can assert on exactly what got recorded
// without needing stdout parsing or a real OTLP collector running.
func newTestTracerProvider(t *testing.T) (*sdktrace.TracerProvider, *tracetest.SpanRecorder) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return tp, sr
}

func TestRecordFaultEvent_AddsEventToActiveSpan(t *testing.T) {
	_, sr := newTestTracerProvider(t)

	ctx, span := telemetry.Tracer().Start(context.Background(), "test-span")
	telemetry.RecordFaultEvent(ctx, "latency", "rule-1",
		attribute.Float64("faultline.latency_seconds", 0.2))
	span.End()

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 recorded span, got %d", len(spans))
	}

	events := spans[0].Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 span event, got %d", len(events))
	}
	if events[0].Name != "faultline.injected" {
		t.Fatalf("expected event name faultline.injected, got %s", events[0].Name)
	}

	var foundFaultType, foundRuleID bool
	for _, attr := range events[0].Attributes {
		if attr.Key == "faultline.fault_type" && attr.Value.AsString() == "latency" {
			foundFaultType = true
		}
		if attr.Key == "faultline.rule_id" && attr.Value.AsString() == "rule-1" {
			foundRuleID = true
		}
	}
	if !foundFaultType || !foundRuleID {
		t.Fatalf("expected fault_type and rule_id attributes present, got %+v", events[0].Attributes)
	}
}

func TestRecordFaultEvent_NoActiveSpan_IsNoOp(t *testing.T) {
	// Deliberately no tracer provider set up here beyond the default —
	// this proves RecordFaultEvent doesn't panic when called against a
	// context with no real span (e.g. from a unit test that isn't
	// exercising tracing at all).
	ctx := context.Background()
	telemetry.RecordFaultEvent(ctx, "latency", "rule-1") // should not panic
}
