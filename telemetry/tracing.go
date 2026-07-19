package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

// TracerName is the instrumentation library name Faultline registers
// its tracer under. OTel conventions expect a stable, reverse-DNS-ish
// name here so traces from Faultline are identifiable in a viewer
// alongside spans from other instrumented libraries in the same
// service.
const TracerName = "github.com/Tharun-bot/faultline"

// InitTracing builds and registers a global TracerProvider. exporterKind
// selects "stdout" (prints spans to the console — zero infra, good for
// local dev and the very first "does this even work" check) or "otlp"
// (sends to a real collector at otlpEndpoint, e.g. "localhost:4317",
// for the Phase 12 Jaeger/Grafana demo). serviceName is attached as a
// resource attribute so a collector receiving spans from multiple
// services (toyservice, controlplaned) can tell them apart.
//
// Returns a shutdown function the caller MUST call (typically via
// defer) to flush any buffered spans before the process exits —
// without this, the last batch of spans can be silently lost.
func InitTracing(ctx context.Context, serviceName, exporterKind, otlpEndpoint string) (shutdown func(context.Context) error, err error) {
	var exporter sdktrace.SpanExporter

	switch exporterKind {
	case "stdout":
		exporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
	case "otlp":
		exporter, err = otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(otlpEndpoint),
			otlptracegrpc.WithInsecure(), // local demo only — real deployments would use TLS
		)
	default:
		return nil, fmt.Errorf("telemetry: unknown exporter kind %q (want \"stdout\" or \"otlp\")", exporterKind)
	}
	if err != nil {
		return nil, fmt.Errorf("telemetry: failed to create %s exporter: %w", exporterKind, err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("telemetry: failed to build resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	return tp.Shutdown, nil
}

// Tracer returns the global tracer Faultline uses to record span
// events. Kept as a small function (rather than a package-level var)
// so it always reflects whatever TracerProvider InitTracing most
// recently registered — important for tests, which may set up their
// own provider independently of a real running service.
func Tracer() trace.Tracer {
	return otel.Tracer(TracerName)
}
