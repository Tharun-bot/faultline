package kafkafault

import (
	"context"
	"time"

	"github.com/Tharun-bot/faultline/core"
	"github.com/Tharun-bot/faultline/executors"
)

// consumerGroupHeader-equivalent: unlike gRPC metadata or HTTP headers,
// Kafka messages don't have a clean built-in notion of "who is
// consuming this" the way a caller identity works for RPC — the
// consumer group is a property of the READER, not the message. So we
// pass the consumer group in explicitly at Wrapper construction time,
// same rationale as httpfault's explicit serviceName.
type RuleSource interface {
	Find(cc core.CallContext) (core.Rule, bool)
}

// Wrapper holds the shared config needed to fault-inject Kafka
// consumption: which logical "service" this consumer represents (for
// rule targeting) and which RuleSource to consult. The topic is
// treated as the "method" — mirroring httpfault's URL-path-as-method
// choice, since Kafka's most natural per-call identity is the topic
// a message arrived on.
type Wrapper struct {
	serviceName   string
	consumerGroup string
	rules         RuleSource
}

func NewWrapper(serviceName, consumerGroup string, rules RuleSource) *Wrapper {
	return &Wrapper{serviceName: serviceName, consumerGroup: consumerGroup, rules: rules}
}

func (w *Wrapper) callContext(topic string) core.CallContext {
	return core.CallContext{
		Service: w.serviceName,
		Method:  topic,
		Client:  w.consumerGroup,
	}
}

// WrapMessageHandler wraps a per-message handler with fault injection
// for latency/error/corrupt/drop. FaultPartialFailure is intentionally
// NOT handled here — see WrapBatchHandler below, since partial failure
// requires a batch to have any meaning.
func (w *Wrapper) WrapMessageHandler(handler MessageHandler) MessageHandler {
	return func(ctx context.Context, msg Message) error {
		cc := w.callContext(msg.Topic)

		rule, matched := w.rules.Find(cc)
		if !matched || !core.ShouldFire(rule) {
			return handler(ctx, msg)
		}

		return w.applyMessageFault(ctx, msg, handler, rule)
	}
}

func (w *Wrapper) applyMessageFault(
	ctx context.Context,
	msg Message,
	handler MessageHandler,
	rule core.Rule,
) error {
	switch rule.FaultType {

	case core.FaultLatency:
		if err := executors.InjectLatency(ctx, time.Duration(rule.Params.LatencyMS)*time.Millisecond); err != nil {
			// Context cancelled during our injected sleep — the
			// consumer is shutting down or the poll loop's own timeout
			// fired. Propagate as-is so the caller's normal
			// cancellation handling takes over.
			return err
		}
		return handler(ctx, msg)

	case core.FaultError:
		// Error injection short-circuits: the real handler never runs,
		// and we return a synthetic error. What the CALLER does with
		// this error (retry the message, send to a dead-letter topic,
		// skip and commit anyway) is entirely up to their own consumer
		// loop's error-handling policy — Faultline only injects the
		// failure, it doesn't prescribe retry semantics.
		return executors.InjectError(rule.Params.ErrorCode)

	case core.FaultDropConnection:
		// Unlike gRPC/HTTP, there's no clean single-message analog to
		// "the connection dropped" in a consumer loop — a real
		// connection drop affects the WHOLE reader, not one message.
		// We approximate it the same way grpcfault does: return the
		// generic dropped-connection error and let the caller's retry/
		// reconnect logic (which any production Kafka consumer should
		// already have) handle it as if the broker connection blipped.
		return executors.ErrConnectionDropped

	case core.FaultCorruptPayload:
		// Corruption mutates the message bytes BEFORE the real handler
		// ever sees them — unlike gRPC/HTTP where we corrupt the
		// RESPONSE. This is deliberate: Kafka consumers don't produce a
		// response to corrupt, they consume and act on input. Corrupting
		// the incoming payload is what actually tests a consumer's
		// deserialization/validation robustness, which is the realistic
		// production failure mode this fault type exists to catch
		// (e.g. a truncated or garbled message from a misbehaving
		// producer or a serialization bug upstream).
		corrupted := msg
		corrupted.Value = executors.CorruptPayload(msg.Value, rule.Params.CorruptPct)
		return handler(ctx, corrupted)

	case core.FaultPartialFailure:
		// No-op here, by design — see WrapBatchHandler. A single
		// message has no "partial" outcome to speak of.
		return handler(ctx, msg)

	default:
		return handler(ctx, msg)
	}
}

// WrapBatchHandler wraps a batch handler with fault injection,
// specifically enabling FaultPartialFailure: when it matches and
// fires, the first Params.PartialOKCount messages in the batch are
// passed to the real handler individually and their success reported,
// while the remaining messages are marked failed WITHOUT ever being
// handed to the real handler at all — simulating a consumer that
// chokes partway through a batch, exactly the kind of bug a
// batch-processing consumer needs to be tested against (e.g. does it
// correctly commit only the succeeded offsets, or does a partial
// failure incorrectly commit the whole batch?).
//
// For all OTHER fault types, we apply the fault uniformly to the whole
// batch by delegating to WrapMessageHandler per message — those types
// don't have a meaningful "partial" concept, so consistent
// whole-batch behavior is the more honest simulation.
func (w *Wrapper) WrapBatchHandler(handler BatchHandler) func(ctx context.Context, msgs []Message) BatchResult {
	perMessage := w.WrapMessageHandler(func(ctx context.Context, msg Message) error {
		return handler(ctx, []Message{msg})
	})

	return func(ctx context.Context, msgs []Message) BatchResult {
		if len(msgs) == 0 {
			return BatchResult{}
		}

		// Use the FIRST message's topic to decide which rule applies —
		// a batch is assumed to come from one topic/partition
		// assignment at a time, which matches how kafka-go's reader
		// actually delivers messages in practice.
		cc := w.callContext(msgs[0].Topic)
		rule, matched := w.rules.Find(cc)

		if matched && rule.FaultType == core.FaultPartialFailure && core.ShouldFire(rule) {
			return w.applyPartialFailure(ctx, msgs, handler, rule)
		}

		// Not a partial-failure rule (or didn't fire) — apply whatever
		// fault DID match uniformly across the whole batch via the
		// per-message path, then report the outcome as all-succeeded
		// or all-failed together (since latency/error/corrupt/drop
		// don't distinguish individual messages within a batch).
		result := BatchResult{Succeeded: make([]bool, len(msgs))}
		for i, msg := range msgs {
			err := perMessage(ctx, msg)
			result.Succeeded[i] = err == nil
		}
		return result
	}
}

func (w *Wrapper) applyPartialFailure(
	ctx context.Context,
	msgs []Message,
	handler BatchHandler,
	rule core.Rule,
) BatchResult {
	pr := executors.InjectPartialFailure(len(msgs), rule.Params.PartialOKCount)

	result := BatchResult{Succeeded: make([]bool, len(msgs))}
	for i, msg := range msgs {
		if !pr.Succeeded(i) {
			// This message is beyond the "OK count" — deliberately
			// never handed to the real handler at all, simulating a
			// consumer that stops partway through a batch rather than
			// one that processes everything but reports failure after
			// the fact. This distinction matters for testing: a
			// consumer might behave differently if it never even SAW
			// the later messages versus seeing them and rejecting them.
			result.Succeeded[i] = false
			continue
		}
		err := handler(ctx, []Message{msg})
		result.Succeeded[i] = err == nil
	}
	return result
}
