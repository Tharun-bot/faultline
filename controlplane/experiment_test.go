//go:build integration

package controlplane

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/Tharun-bot/faultline/core"
	"github.com/Tharun-bot/faultline/ruleengine"
)

// newTestStore connects to a local Redis (expected via docker compose)
// and flushes the DB before each test so tests don't interfere with
// each other's state. Mirrors ruleengine's own test helper since this
// package needs the same setup for evaluateExperiment's Store calls.
func newTestStore(t *testing.T) (*ruleengine.Store, context.Context) {
	t.Helper()
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("failed to connect/flush test redis (is docker compose up?): %v", err)
	}
	t.Cleanup(func() { rdb.Close() })

	return ruleengine.NewStore(rdb), ctx
}

// TestRollbackWatcher_ThresholdMath isolates just the "should we roll
// back" comparison against a real Store (since evaluateExperiment
// calls GetRule/SaveRule internally), independent of a real running
// Prometheus — full end-to-end rollback behavior including the actual
// Prometheus query is verified manually via the docker-compose demo
// script, since it depends on real infrastructure and wall-clock
// timing that don't suit an automated unit test.
func TestRollbackWatcher_ThresholdMath(t *testing.T) {
	store, ctx := newTestStore(t)

	rule := core.Rule{
		ID:          "r1",
		Target:      core.Target{Service: "OrderService", Method: "Create", Client: "*"},
		FaultType:   core.FaultLatency,
		Params:      core.Params{LatencyMS: 100},
		Probability: 1.0,
		Active:      true,
	}
	if _, err := store.SaveRule(ctx, rule); err != nil {
		t.Fatalf("failed to seed rule: %v", err)
	}

	rw := &RollbackWatcher{
		store:              store,
		errorRateThreshold: 0.20,
		experiments:        make(map[string]*Experiment),
	}

	tests := []struct {
		name           string
		currentErrRate float64
		wantRollback   bool
	}{
		{"well within threshold", 0.10, false},
		{"right at threshold boundary", 0.25, false}, // 0.25 - 0.05 = 0.20, NOT > threshold
		{"just over threshold", 0.26, true},          // 0.26 - 0.05 = 0.21 > 0.20
		{"dramatic spike", 1.0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset a fresh Watching experiment before each subtest so
			// earlier subtests' rollback doesn't leak into later ones.
			rw.experiments["r1"] = &Experiment{
				RuleID:          "r1",
				Status:          StatusWatching,
				BaselineErrRate: 0.05,
				PeakErrRate:     0.05,
			}
			// Re-activate the rule in the store too, since a prior
			// subtest's rollback deactivated it.
			r, err := store.GetRule(ctx, "r1")
			if err != nil {
				t.Fatalf("failed to fetch seeded rule: %v", err)
			}
			r.Active = true
			if _, err := store.SaveRule(ctx, r); err != nil {
				t.Fatalf("failed to reactivate rule: %v", err)
			}

			rw.evaluateExperiment(ctx, "r1", tt.currentErrRate)

			exp := rw.experiments["r1"]
			gotRollback := exp.Status == StatusRolledBack
			if gotRollback != tt.wantRollback {
				t.Fatalf("currentErrRate=%.2f: got rollback=%v, want %v", tt.currentErrRate, gotRollback, tt.wantRollback)
			}
		})
	}
}
