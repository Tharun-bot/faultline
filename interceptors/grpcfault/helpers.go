package grpcfault

import (
	"time"

	"github.com/Tharun-bot/faultline/executors"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// durationMS is a tiny named type so durationFromMS's return value is
// self-documenting at the call site rather than a bare int.
type durationMS int

// Duration converts to a real time.Duration for use with executors.InjectLatency.
func (d durationMS) Duration() time.Duration {
	return time.Duration(d) * time.Millisecond
}

func corruptBytes(data []byte, pct int) []byte {
	return executors.CorruptPayload(data, pct)
}

// grpcCodeFromString maps our generic, transport-agnostic error code
// strings (as stored in Rule.Params.ErrorCode) onto real grpc/codes
// values. We keep this mapping small and explicit rather than trying
// to auto-derive it, since typos in rule config ("UNAVAILBLE") should
// fail safe to Unknown rather than panic.
func grpcCodeFromString(code string) codes.Code {
	switch code {
	case "UNAVAILABLE":
		return codes.Unavailable
	case "DEADLINE_EXCEEDED":
		return codes.DeadlineExceeded
	case "INTERNAL":
		return codes.Internal
	case "RESOURCE_EXHAUSTED":
		return codes.ResourceExhausted
	case "UNAUTHENTICATED":
		return codes.Unauthenticated
	default:
		return codes.Unknown
	}
}

// corruptResponse type-asserts the handler's response to a proto.Message,
// marshals it to bytes, corrupts those bytes, and unmarshals back into
// a NEW message of the same concrete type. If any step fails (e.g. the
// corrupted bytes no longer parse as valid protobuf, which is likely
// at higher corruption percentages), we fail safe and return the
// ORIGINAL uncorrupted response rather than crashing the RPC — a
// corruption experiment should degrade the payload, not take down the
// whole call with a marshal panic.
func corruptResponse(resp interface{}, pct int) interface{} {
	msg, ok := resp.(proto.Message)
	if !ok {
		return resp
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return resp
	}

	corrupted := corruptBytes(data, pct)

	// Build a fresh instance of the same concrete message type to
	// unmarshal into — we must not mutate the original `msg` in place
	// in case corruption fails partway and we need to fall back to it.
	newMsg := msg.ProtoReflect().New().Interface()
	if err := proto.Unmarshal(corrupted, newMsg); err != nil {
		return resp
	}

	return newMsg
}
