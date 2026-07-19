package grpcfault

import (
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/Tharun-bot/faultline/executors"
)

// msToDuration converts a plain int milliseconds value (as stored in
// Rule.Params.LatencyMS) directly into a time.Duration. We deliberately
// return time.Duration itself rather than wrapping it in a named type
// (an earlier version of this file introduced a `durationMS` type with
// its own .Duration() method) — that extra indirection added no real
// value and was the source of a type-mismatch bug. A plain conversion
// function is simpler and exactly as clear at the call site.
func msToDuration(ms int) time.Duration {
	return time.Duration(ms) * time.Millisecond
}

// grpcCodeFromString maps our generic, transport-agnostic error code
// strings (as stored in Rule.Params.ErrorCode) onto real grpc/codes
// values. Kept small and explicit rather than auto-derived, since
// typos in rule config ("UNAVAILBLE") should fail safe to Unknown
// rather than panic.
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
// corrupted bytes no longer parse as valid protobuf, which is likely at
// higher corruption percentages), we fail safe and return the ORIGINAL
// uncorrupted response rather than crashing the RPC.
func corruptResponse(resp interface{}, pct int) interface{} {
	msg, ok := resp.(proto.Message)
	if !ok {
		return resp
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return resp
	}

	corrupted := executors.CorruptPayload(data, pct)

	// Build a fresh instance of the same concrete message type to
	// unmarshal into — we must not mutate the original `msg` in place
	// in case corruption fails partway and we need to fall back to it.
	newMsg := msg.ProtoReflect().New().Interface()
	if err := proto.Unmarshal(corrupted, newMsg); err != nil {
		return resp
	}

	return newMsg
}
