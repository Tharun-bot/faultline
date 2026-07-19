package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// RecordFaultEvent adds a "faultline.injected" span event to whatever
// span is active in ctx, with structured attributes describing what
// was injected. This is the ONE function all three interceptors
// (grpcfault, httpfault, kafkafault) call — keeping the event name and
// attribute schema in exactly one place means a trace viewer shows a
// consistent, greppable event across every protocol Faultline touches.
//
// If ctx has no active span (e.g. the caller didn't set up tracing, or
// this is a unit test with no tracer configured), trace.SpanFromContext
// returns a no-op span and this call is silently a no-op too — callers
// never need to check "is tracing enabled" themselves.
func RecordFaultEvent(ctx context.Context, faultType, ruleID string, extra ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)

	attrs := append([]attribute.KeyValue{
		attribute.String("faultline.fault_type", faultType),
		attribute.String("faultline.rule_id", ruleID),
	}, extra...)

	span.AddEvent("faultline.injected", trace.WithAttributes(attrs...))
}
