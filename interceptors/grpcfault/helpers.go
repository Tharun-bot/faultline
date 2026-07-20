package grpcfault

import (
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/Tharun-bot/faultline/executors"
)

// msToDuration converts a plain int milliseconds value (as stored in
// Rule.Params.LatencyMS) into a time.Duration.
func msToDuration(ms int) time.Duration {
	return time.Duration(ms) * time.Millisecond
}

// grpcCodeFromString maps our generic, transport-agnostic error code
// strings onto real grpc/codes values. Kept small and explicit so a
// typo in rule config fails safe to Unknown rather than panicking.
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

// corruptResponse type-asserts the handler's response to a
// proto.Message, marshals it, corrupts the bytes, and unmarshals back
// into a NEW message of the same concrete type. Fails safe (returns
// the original response) if any step errors — a corruption experiment
// should degrade the payload, not crash the RPC.
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

	newMsg := msg.ProtoReflect().New().Interface()
	if err := proto.Unmarshal(corrupted, newMsg); err != nil {
		return resp
	}

	return newMsg
}
