package kafkafault

import (
	"context"
	"time"

	"github.com/Tharun-bot/faultline/core"
	"github.com/Tharun-bot/faultline/executors"
	"github.com/Tharun-bot/faultline/telemetry"
)

// RuleSource is the interface the wrapper depends on to find active
// rules. Defined here (not imported from ruleengine directly) so this
// package can be tested with a trivial in-memory fake, and Phase 5's
// real Redis-backed Cache can be swapped in without this file changing.
type RuleSource interface {
	Find(cc core.CallContext) (core.Rule, bool)
}

// Wrapper holds the shared config needed to fault-inject Kafka
// consumption: which logical "service" this consumer represents (for
// rule targeting) and which RuleSource to consult. The topic is
// treated as the "method" — mirroring httpfault's URL-path-as-method
// choice, since Kafka's most natural per-call identity is the topic a
// message arrived on. Unlike gRPC/HTTP, Kafka has no per-call caller
// identity to read off the wire, so consumerGroup is passed in
// explicitly at construction time and used as the "client" for rule
// targeting.
type Wrapper struct {
	serviceName   string
	consumerGroup string
	rules         RuleSource
	metrics       *telemetry.Metrics // may be nil, treated as a no-op
}

func NewWrapper(serviceName, consumerGroup string, rules RuleSource, metrics *telemetry.Metrics) *Wrapper {
	return &Wrapper{
		serviceName:   serviceName,
		consumerGroup: consumerGroup,
		rules:         rules,
		metrics:       metrics,
	}
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

		if w.metrics != nil {
			w.metrics.RecordInjection(string(rule.FaultType), rule.ID)
		}

		return w.applyMessageFault(ctx, msg, handler, rule)
	}
}

// applyMessageFault is the one place that translates a generic fault
// decision into Kafka-specific behavior — everything above it is
// protocol-agnostic (core.Matcher/ShouldFire already decided WHAT to
// do; this decides HOW that looks for a Kafka consumer).
func (w *Wrapper) applyMessageFault(
	ctx context.Context,
	msg Message,
	handler MessageHandler,
	rule core.Rule,
) error {
	switch rule.FaultType {

	case core.FaultLatency:
		d := time.Duration(rule.Params.LatencyMS) * time.Millisecond
		if err := executors.InjectLatency(ctx, d); err != nil {
			// Context cancelled during our injected sleep — the
			// consumer is shutting down or its own poll loop timed out.
			// Propagate as-is so normal cancellation handling upstream
			// takes over.
			return err
		}
		if w.metrics != nil {
			w.metrics.RecordLatencyInjection(rule.ID, d.Seconds())
		}
		return handler(ctx, msg)

	case core.FaultError:
		// Error injection short-circuits: the real handler never runs.
		// What the CALLER does with this error (retry, dead-letter,
		// skip-and-commit) is entirely up to their own consumer loop's
		// error-handling policy — Faultline only injects the failure.
		return executors.InjectError(rule.Params.ErrorCode)

	case core.FaultDropConnection:
		// No clean single-message analog to "the connection dropped"
		// in a consumer loop — a real drop affects the WHOLE reader,
		// not one message. We approximate with the generic dropped-
		// connection error and let the caller's existing reconnect
		// logic handle it, same rationale as grpcfault's approximation.
		return executors.ErrConnectionDropped

	case core.FaultCorruptPayload:
		// Corruption mutates the INCOMING message before the real
		// handler ever sees it — the opposite direction from gRPC/HTTP,
		// which corrupt the outgoing RESPONSE. Kafka consumers don't
		// produce a response to corrupt; corrupting the input is what
		// actually tests a consumer's deserialization/validation
		// robustness, the realistic production failure mode this
		// exists to catch.
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
// handed to the real handler — simulating a consumer that chokes
// partway through a batch, testing whether it correctly commits only
// the succeeded offsets.
//
// For all OTHER fault types, the fault is applied uniformly to the
// whole batch by delegating to WrapMessageHandler per message — those
// types don't have a meaningful "partial" concept.
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
		// assignment at a time, matching how kafka-go actually
		// delivers messages in practice.
		cc := w.callContext(msgs[0].Topic)
		rule, matched := w.rules.Find(cc)

		if matched && rule.FaultType == core.FaultPartialFailure && core.ShouldFire(rule) {
			if w.metrics != nil {
				w.metrics.RecordInjection(string(rule.FaultType), rule.ID)
			}
			return w.applyPartialFailure(ctx, msgs, handler, rule)
		}

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
			// Beyond the "OK count" — deliberately never handed to the
			// real handler, simulating a consumer that stops partway
			// through rather than one that sees everything but rejects
			// some of it after the fact.
			result.Succeeded[i] = false
			continue
		}
		err := handler(ctx, []Message{msg})
		result.Succeeded[i] = err == nil
	}
	return result
}
