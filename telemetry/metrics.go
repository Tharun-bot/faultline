package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds every Faultline-owned Prometheus collector. Bundled
// into one struct (rather than package-level global vars) so tests
// can construct an isolated Metrics against their own
// prometheus.NewRegistry(), instead of fighting over the global
// default registry — the global registry approach panics on
// "duplicate metrics collector registration" if more than one test in
// the same process tries to register the same metric name.
type Metrics struct {
	InjectionsTotal         *prometheus.CounterVec
	InjectionLatencySeconds *prometheus.HistogramVec
}

// NewMetrics builds and registers all Faultline collectors against
// the given registry. Pass prometheus.NewRegistry() for isolated
// tests, or prometheus.DefaultRegisterer for a real running service.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		InjectionsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "faultline_injections_total",
				Help: "Total number of times Faultline actually injected a fault (rule matched AND probability roll fired).",
			},
			[]string{"fault_type", "rule_id"},
		),
		InjectionLatencySeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "faultline_injection_latency_seconds",
				Help:    "Distribution of injected latency durations, for FaultLatency rules only.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"rule_id"},
		),
	}

	reg.MustRegister(m.InjectionsTotal, m.InjectionLatencySeconds)
	return m
}

// RecordInjection increments the injections counter for a given fault
// type and rule. Called from every interceptor at the exact point a
// fault actually fires — NOT when a rule merely matches but the
// probability roll said no.
func (m *Metrics) RecordInjection(faultType, ruleID string) {
	m.InjectionsTotal.WithLabelValues(faultType, ruleID).Inc()
}

// RecordLatencyInjection additionally records the actual injected
// duration in the histogram — called only from the FaultLatency
// branch of each interceptor, in addition to (not instead of)
// RecordInjection.
func (m *Metrics) RecordLatencyInjection(ruleID string, seconds float64) {
	m.InjectionLatencySeconds.WithLabelValues(ruleID).Observe(seconds)
}
