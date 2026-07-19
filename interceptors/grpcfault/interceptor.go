package grpcfault

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Tharun-bot/faultline/core"
	"github.com/Tharun-bot/faultline/executors"
	"github.com/Tharun-bot/faultline/telemetry"
)

// clientMetadataKey is the gRPC metadata header we expect callers to
// set so we know WHO is calling, for rules that target a specific
// client (e.g. "only inject latency for checkout-service").
const clientMetadataKey = "x-faultline-client"

// RuleSource is the interface the interceptor depends on to find
// active rules. Defined here (not imported from ruleengine directly)
// so this package can be tested with a trivial in-memory fake, and
// Phase 5's real Redis-backed Cache can be swapped in without this
// file changing.
type RuleSource interface {
	Find(cc core.CallContext) (core.Rule, bool)
}

// UnaryServerInterceptor builds a grpc.UnaryServerInterceptor backed
// by the given RuleSource. metrics may be nil (e.g. in tests that
// don't care about metrics) — a nil metrics is treated as a no-op.
func UnaryServerInterceptor(rules RuleSource, metrics *telemetry.Metrics) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		ctx, span := telemetry.Tracer().Start(ctx, info.FullMethod)
		defer span.End()

		service, method := parseFullMethod(info.FullMethod)
		client := clientFromMetadata(ctx)

		cc := core.CallContext{Service: service, Method: method, Client: client}

		rule, matched := rules.Find(cc)
		if !matched || !core.ShouldFire(rule) {
			return handler(ctx, req)
		}

		return applyFault(ctx, req, handler, rule, metrics)
	}
}

// applyFault dispatches to the correct executor based on rule.FaultType.
// This switch is the one place in the whole codebase that translates a
// generic fault decision into gRPC-specific behavior (status codes,
// short-circuiting handler(), etc) — everything above and below it is
// protocol-agnostic.
func applyFault(
	ctx context.Context,
	req interface{},
	handler grpc.UnaryHandler,
	rule core.Rule,
	metrics *telemetry.Metrics,
) (interface{}, error) {
	switch rule.FaultType {

	case core.FaultLatency:
		d := msToDuration(rule.Params.LatencyMS)
		if err := executors.InjectLatency(ctx, d); err != nil {
			return nil, err
		}
		if metrics != nil {
			metrics.RecordLatencyInjection(rule.ID, d.Seconds())
		}
		telemetry.RecordFaultEvent(ctx, string(rule.FaultType), rule.ID,
			attribute.Float64("faultline.latency_seconds", d.Seconds()))
		return handler(ctx, req)

	case core.FaultError:
		err := executors.InjectError(rule.Params.ErrorCode)
		ie, _ := executors.AsInjectedError(err)
		telemetry.RecordFaultEvent(ctx, string(rule.FaultType), rule.ID,
			attribute.String("faultline.error_code", ie.Code))
		return nil, status.Error(grpcCodeFromString(ie.Code), err.Error())

	case core.FaultDropConnection:
		telemetry.RecordFaultEvent(ctx, string(rule.FaultType), rule.ID)
		return nil, status.Error(codes.Unavailable, executors.ErrConnectionDropped.Error())

	case core.FaultCorruptPayload:
		resp, err := handler(ctx, req)
		if err != nil {
			return resp, err
		}
		telemetry.RecordFaultEvent(ctx, string(rule.FaultType), rule.ID,
			attribute.Int("faultline.corrupt_pct", rule.Params.CorruptPct))
		return corruptResponse(resp, rule.Params.CorruptPct), nil

	case core.FaultPartialFailure:
		return handler(ctx, req)

	default:
		return handler(ctx, req)
	}
}

// parseFullMethod splits gRPC's "/package.Service/Method" format into
// our Service/Method fields.
func parseFullMethod(fullMethod string) (service, method string) {
	trimmed := strings.TrimPrefix(fullMethod, "/")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) != 2 {
		return "", ""
	}
	servicePart := parts[0]
	method = parts[1]

	if idx := strings.LastIndex(servicePart, "."); idx != -1 {
		service = servicePart[idx+1:]
	} else {
		service = servicePart
	}
	return service, method
}

// clientFromMetadata reads the caller identity header. Returns ""
// if absent, which simply means no rule with a specific (non-wildcard)
// Client target will match this call — wildcard rules still apply.
func clientFromMetadata(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vals := md.Get(clientMetadataKey)
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}
