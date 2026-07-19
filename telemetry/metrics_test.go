package telemetry_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/Tharun-bot/faultline/telemetry"
)

// counterValue digs the current value out of a CounterVec for a
// specific label combination — Prometheus client library doesn't
// expose a simple "get current value" method directly, so we go
// through the metric's Write() method into the protobuf Metric type.
func counterValue(t *testing.T, c *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()
	m := &dto.Metric{}
	if err := c.WithLabelValues(labels...).Write(m); err != nil {
		t.Fatalf("failed to read counter value: %v", err)
	}
	return m.GetCounter().GetValue()
}

func TestMetrics_RecordInjection_IncrementsCounter(t *testing.T) {
	reg := prometheus.NewRegistry() // isolated registry, avoids clashing with other tests
	m := telemetry.NewMetrics(reg)

	m.RecordInjection("latency", "rule-1")
	m.RecordInjection("latency", "rule-1")
	m.RecordInjection("error", "rule-2")

	if got := counterValue(t, m.InjectionsTotal, "latency", "rule-1"); got != 2 {
		t.Fatalf("expected 2 injections for latency/rule-1, got %v", got)
	}
	if got := counterValue(t, m.InjectionsTotal, "error", "rule-2"); got != 1 {
		t.Fatalf("expected 1 injection for error/rule-2, got %v", got)
	}
}

func TestMetrics_RecordLatencyInjection_ObservesHistogram(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := telemetry.NewMetrics(reg)

	m.RecordLatencyInjection("rule-1", 0.2)
	m.RecordLatencyInjection("rule-1", 0.5)

	metricCh := make(chan prometheus.Metric, 10)
	m.InjectionLatencySeconds.Collect(metricCh)
	close(metricCh)

	var found bool
	for metric := range metricCh {
		dtoMetric := &dto.Metric{}
		if err := metric.Write(dtoMetric); err != nil {
			t.Fatalf("failed to write metric: %v", err)
		}
		if dtoMetric.GetHistogram().GetSampleCount() == 2 {
			found = true
		}
	}
	if !found {
		t.Fatal("expected histogram to have recorded 2 samples")
	}
}
