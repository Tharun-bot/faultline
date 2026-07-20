package controlplane

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"

	"github.com/Tharun-bot/faultline/ruleengine"
)

// ExperimentStatus mirrors the state machine: an experiment starts
// Watching, and ends either RolledBack (auto-detected error spike) or
// Stopped (deactivated normally, no spike detected).
type ExperimentStatus string

const (
	StatusWatching   ExperimentStatus = "watching"
	StatusRolledBack ExperimentStatus = "rolled_back"
	StatusStopped    ExperimentStatus = "stopped"
)

// Experiment records everything about one activation of a rule that
// the rollback watcher is tracking — the "before/after metrics" the
// design calls for.
type Experiment struct {
	RuleID          string
	Status          ExperimentStatus
	BaselineErrRate float64
	PeakErrRate     float64
	StartedAt       time.Time
	EndedAt         time.Time
}

// RollbackWatcher polls Prometheus for the wrapped service's real
// error rate after a rule is activated, and automatically deactivates
// the rule (via Store) if the error rate spikes beyond baseline by
// more than errorRateThreshold. It has NO knowledge of Faultline's own
// injection counters — see ServiceMetrics's doc comment for why that
// separation matters.
type RollbackWatcher struct {
	store              *ruleengine.Store
	promClient         promv1.API
	pollInterval       time.Duration
	errorRateThreshold float64

	mu          sync.Mutex
	experiments map[string]*Experiment

	logger *slog.Logger
}

func NewRollbackWatcher(store *ruleengine.Store, prometheusURL string, pollInterval time.Duration, errorRateThreshold float64) (*RollbackWatcher, error) {
	client, err := promapi.NewClient(promapi.Config{Address: prometheusURL})
	if err != nil {
		return nil, fmt.Errorf("controlplane: failed to create prometheus client: %w", err)
	}

	return &RollbackWatcher{
		store:              store,
		promClient:         promv1.NewAPI(client),
		pollInterval:       pollInterval,
		errorRateThreshold: errorRateThreshold,
		experiments:        make(map[string]*Experiment),
		logger:             slog.Default(),
	}, nil
}

// StartExperiment begins watching a newly-activated rule. Captures a
// baseline error rate FIRST (from just before/at activation), so we
// have something honest to compare against.
func (rw *RollbackWatcher) StartExperiment(ctx context.Context, ruleID string) error {
	baseline, err := rw.queryErrorRate(ctx)
	if err != nil {
		return fmt.Errorf("controlplane: failed to capture baseline: %w", err)
	}

	rw.mu.Lock()
	rw.experiments[ruleID] = &Experiment{
		RuleID:          ruleID,
		Status:          StatusWatching,
		BaselineErrRate: baseline,
		PeakErrRate:     baseline,
		StartedAt:       time.Now(),
	}
	rw.mu.Unlock()

	rw.logger.Info("experiment started", "rule_id", ruleID, "baseline_error_rate", baseline)
	return nil
}

// Run is the background polling loop — call via `go watcher.Run(ctx)`
// at control plane startup.
func (rw *RollbackWatcher) Run(ctx context.Context) {
	ticker := time.NewTicker(rw.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rw.checkAll(ctx)
		}
	}
}

func (rw *RollbackWatcher) checkAll(ctx context.Context) {
	rw.mu.Lock()
	watching := make([]string, 0, len(rw.experiments))
	for id, exp := range rw.experiments {
		if exp.Status == StatusWatching {
			watching = append(watching, id)
		}
	}
	rw.mu.Unlock()

	if len(watching) == 0 {
		return
	}

	current, err := rw.queryErrorRate(ctx)
	if err != nil {
		rw.logger.Error("failed to query error rate during rollback check", "err", err)
		return
	}

	for _, ruleID := range watching {
		rw.evaluateExperiment(ctx, ruleID, current)
	}
}

func (rw *RollbackWatcher) evaluateExperiment(ctx context.Context, ruleID string, currentErrRate float64) {
	rw.mu.Lock()
	exp, exists := rw.experiments[ruleID]
	if !exists || exp.Status != StatusWatching {
		rw.mu.Unlock()
		return
	}
	if currentErrRate > exp.PeakErrRate {
		exp.PeakErrRate = currentErrRate
	}
	shouldRollback := currentErrRate-exp.BaselineErrRate > rw.errorRateThreshold
	rw.mu.Unlock()

	if !shouldRollback {
		return
	}

	rw.logger.Warn("error rate spike detected, rolling back rule",
		"rule_id", ruleID, "baseline", exp.BaselineErrRate, "current", currentErrRate)

	rule, err := rw.store.GetRule(ctx, ruleID)
	if err != nil {
		rw.logger.Error("rollback: failed to fetch rule", "rule_id", ruleID, "err", err)
		return
	}
	rule.Active = false
	if _, err := rw.store.SaveRule(ctx, rule); err != nil {
		rw.logger.Error("rollback: failed to deactivate rule", "rule_id", ruleID, "err", err)
	}

	rw.mu.Lock()
	exp.Status = StatusRolledBack
	exp.EndedAt = time.Now()
	rw.mu.Unlock()
}

// StopExperiment marks a manually-deactivated experiment as Stopped
// (as opposed to auto-rolled-back).
func (rw *RollbackWatcher) StopExperiment(ruleID string) {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	if exp, ok := rw.experiments[ruleID]; ok && exp.Status == StatusWatching {
		exp.Status = StatusStopped
		exp.EndedAt = time.Now()
	}
}

// GetExperiment returns the current record for a rule, if being tracked.
func (rw *RollbackWatcher) GetExperiment(ruleID string) (Experiment, bool) {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	exp, ok := rw.experiments[ruleID]
	if !ok {
		return Experiment{}, false
	}
	return *exp, true
}

// queryErrorRate runs an instant PromQL query computing the error
// rate over the last minute: errors / total requests.
func (rw *RollbackWatcher) queryErrorRate(ctx context.Context) (float64, error) {
	query := `sum(rate(toyservice_requests_total{outcome="error"}[1m])) / sum(rate(toyservice_requests_total[1m]))`

	result, _, err := rw.promClient.Query(ctx, query, time.Now())
	if err != nil {
		return 0, fmt.Errorf("prometheus query failed: %w", err)
	}

	vector, ok := result.(model.Vector)
	if !ok || len(vector) == 0 {
		// No data yet — treat as 0 error rate rather than erroring.
		return 0, nil
	}
	return float64(vector[0].Value), nil
}
