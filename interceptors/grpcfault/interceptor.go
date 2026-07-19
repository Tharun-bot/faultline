package grpcfault

import (
	"context"
	"strings"

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
		service, method := parseFullMethod(info.FullMethod)
		client := clientFromMetadata(ctx)

		cc := core.CallContext{Service: service, Method: method, Client: client}

		rule, matched := rules.Find(cc)
		if !matched || !core.ShouldFire(rule) {
			// No applicable rule, or the probability roll said "not this
			// time" — pass straight through to the real handler untouched.
			return handler(ctx, req)
		}

		if metrics != nil {
			metrics.RecordInjection(string(rule.FaultType), rule.ID)
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
			// The context was cancelled during our injected sleep — the
			// caller gave up, so there's nothing useful to return.
			return nil, err
		}
		if metrics != nil {
			metrics.RecordLatencyInjection(rule.ID, d.Seconds())
		}
		// After sleeping, proceed with the REAL call — latency injection
		// is additive, not a replacement for the real logic.
		return handler(ctx, req)

	case core.FaultError:
		err := executors.InjectError(rule.Params.ErrorCode)
		ie, _ := executors.AsInjectedError(err)
		// Translate our generic Code string into an actual gRPC status.
		// The real handler is never called — error injection is a
		// short-circuit, not an addition.
		return nil, status.Error(grpcCodeFromString(ie.Code), err.Error())

	case core.FaultDropConnection:
		// gRPC has no clean way to "just close the socket" from inside a
		// unary handler without terminating the whole connection — so we
		// approximate a dropped connection with Unavailable, which is
		// what a real client sees when a connection genuinely drops
		// mid-call.
		return nil, status.Error(codes.Unavailable, executors.ErrConnectionDropped.Error())

	case core.FaultCorruptPayload:
		// Corruption needs the REAL response first, then mutates it —
		// so unlike error injection, we DO call the real handler.
		resp, err := handler(ctx, req)
		if err != nil {
			// Don't corrupt an already-failed response — that would
			// conflate two different failure modes in one experiment.
			return resp, err
		}
		return corruptResponse(resp, rule.Params.CorruptPct), nil

	case core.FaultPartialFailure:
		// Partial failure is inherently about batches/streams, which a
		// single unary RPC doesn't have — for the unary interceptor we
		// treat it as a no-op and just pass through. This fault type is
		// really meant for Phase 8's Kafka consumer wrapper.
		return handler(ctx, req)

	default:
		// Unknown fault type slipped through despite Rule.Validate() —
		// fail safe by passing through untouched rather than breaking
		// the call in an undefined way.
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
