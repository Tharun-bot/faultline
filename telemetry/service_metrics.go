package telemetry

import "github.com/prometheus/client_golang/prometheus"

// ServiceMetrics represents metrics a REAL wrapped service would
// normally already expose on its own — request counts split by
// success/failure. The rollback watcher reads THIS, not
// faultline_injections_total, because what matters for deciding
// "should we roll back" is the actual observed failure rate of the
// service under test, not how many times Faultline itself pulled the
// trigger. In a real deployment this metric would already exist (e.g.
// from the service's own middleware) — we add a minimal version here
// only because our toy service doesn't have one yet.
type ServiceMetrics struct {
	RequestsTotal *prometheus.CounterVec
}

func NewServiceMetrics(reg prometheus.Registerer) *ServiceMetrics {
	m := &ServiceMetrics{
		RequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "toyservice_requests_total",
				Help: "Total requests handled by the toy service, labeled by outcome.",
			},
			[]string{"outcome"}, // "success" or "error"
		),
	}
	reg.MustRegister(m.RequestsTotal)
	return m
}

func (m *ServiceMetrics) RecordSuccess() { m.RequestsTotal.WithLabelValues("success").Inc() }
func (m *ServiceMetrics) RecordError()   { m.RequestsTotal.WithLabelValues("error").Inc() }
