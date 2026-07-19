package kafkafault

import (
	"context"
	"time"

	"github.com/Tharun-bot/faultline/core"
	"github.com/Tharun-bot/faultline/executors"
	"github.com/Tharun-bot/faultline/telemetry"
)

// RuleSource is the interface the wrapper depends on to find active rules.
type RuleSource interface {
	Find(cc core.CallContext) (core.Rule, bool)
}

// Wrapper holds the shared config needed to fault-inject Kafka
// consumption. metrics may be nil (treated as a no-op) for tests.
type Wrapper struct {
	serviceName   string
	consumerGroup string
	rules         RuleSource
	metrics       *telemetry.Metrics
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
// NOT handled here — see WrapBatchHandler.
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
			return err
		}
		if w.metrics != nil {
			w.metrics.RecordLatencyInjection(rule.ID, d.Seconds())
		}
		return handler(ctx, msg)

	case core.FaultError:
		return executors.InjectError(rule.Params.ErrorCode)

	case core.FaultDropConnection:
		return executors.ErrConnectionDropped

	case core.FaultCorruptPayload:
		corrupted := msg
		corrupted.Value = executors.CorruptPayload(msg.Value, rule.Params.CorruptPct)
		return handler(ctx, corrupted)

	case core.FaultPartialFailure:
		// No-op here, by design — see WrapBatchHandler.
		return handler(ctx, msg)

	default:
		return handler(ctx, msg)
	}
}

// WrapBatchHandler wraps a batch handler with fault injection,
// specifically enabling FaultPartialFailure.
func (w *Wrapper) WrapBatchHandler(handler BatchHandler) func(ctx context.Context, msgs []Message) BatchResult {
	perMessage := w.WrapMessageHandler(func(ctx context.Context, msg Message) error {
		return handler(ctx, []Message{msg})
	})

	return func(ctx context.Context, msgs []Message) BatchResult {
		if len(msgs) == 0 {
			return BatchResult{}
		}

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
			result.Succeeded[i] = false
			continue
		}
		err := handler(ctx, []Message{msg})
		result.Succeeded[i] = err == nil
	}
	return result
}
