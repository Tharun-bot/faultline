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
// active rules.
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
			return handler(ctx, req)
		}

		if metrics != nil {
			metrics.RecordInjection(string(rule.FaultType), rule.ID)
		}

		return applyFault(ctx, req, handler, rule, metrics)
	}
}

// applyFault dispatches to the correct executor based on rule.FaultType.
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
		return handler(ctx, req)

	case core.FaultError:
		err := executors.InjectError(rule.Params.ErrorCode)
		ie, _ := executors.AsInjectedError(err)
		return nil, status.Error(grpcCodeFromString(ie.Code), err.Error())

	case core.FaultDropConnection:
		return nil, status.Error(codes.Unavailable, executors.ErrConnectionDropped.Error())

	case core.FaultCorruptPayload:
		resp, err := handler(ctx, req)
		if err != nil {
			return resp, err
		}
		return corruptResponse(resp, rule.Params.CorruptPct), nil

	case core.FaultPartialFailure:
		return handler(ctx, req)

	default:
		return handler(ctx, req)
	}
}

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
