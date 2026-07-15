package core

import "testing"

func TestMatcher_Find(t *testing.T) {
	rules := []Rule{
		{
			ID:     "r1",
			Target: Target{Service: "OrderService", Method: "Create", Client: "checkout-service"},
			Active: true,
		},
		{
			ID:     "r2",
			Target: Target{Service: "OrderService", Method: "Create", Client: "*"},
			Active: true,
		},
		{
			ID:     "r3-inactive",
			Target: Target{Service: "OrderService", Method: "Create", Client: "checkout-service"},
			Active: false, // should never be returned
		},
	}

	m := NewMatcher(rules)

	tests := []struct {
		name   string
		cc     CallContext
		wantID string
		wantOK bool
	}{
		{
			name:   "exact client match wins first",
			cc:     CallContext{Service: "OrderService", Method: "Create", Client: "checkout-service"},
			wantID: "r1", // r1 is listed before r2, and both match — Find returns first match
			wantOK: true,
		},
		{
			name:   "wildcard matches unknown client",
			cc:     CallContext{Service: "OrderService", Method: "Create", Client: "some-other-service"},
			wantID: "r2",
			wantOK: true,
		},
		{
			name:   "no match on different method",
			cc:     CallContext{Service: "OrderService", Method: "Cancel", Client: "checkout-service"},
			wantOK: false,
		},
		{
			name:   "no match on different service",
			cc:     CallContext{Service: "InventoryService", Method: "Create", Client: "checkout-service"},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := m.Find(tt.cc)
			if ok != tt.wantOK {
				t.Fatalf("Find() ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && got.ID != tt.wantID {
				t.Fatalf("Find() matched rule = %s, want %s", got.ID, tt.wantID)
			}
		})
	}
}

// TestMatcher_InactiveRulesFiltered specifically checks that an
// inactive rule is never returned, even when nothing else matches
// instead. This protects the important invariant that NewMatcher
// filters at construction time, not at lookup time.
func TestMatcher_InactiveRulesFiltered(t *testing.T) {
	rules := []Rule{
		{
			ID:     "inactive-only",
			Target: Target{Service: "X", Method: "Y", Client: "*"},
			Active: false,
		},
	}
	m := NewMatcher(rules)
	_, ok := m.Find(CallContext{Service: "X", Method: "Y", Client: "anyone"})
	if ok {
		t.Fatal("expected no match, inactive rule should have been filtered out")
	}
}

// TestShouldFire_Probability runs many trials and checks the observed
// fire rate is statistically close to the configured probability. This
// is a probabilistic test, so we allow a tolerance band rather than an
// exact match — flakiness here should be rare but not impossible;
// if this test ever flakes, widen the tolerance slightly before
// suspecting a logic bug.
func TestShouldFire_Probability(t *testing.T) {
	r := Rule{Probability: 0.3}
	const trials = 100_000
	fired := 0
	for i := 0; i < trials; i++ {
		if ShouldFire(r) {
			fired++
		}
	}
	observed := float64(fired) / float64(trials)
	const tolerance = 0.01
	if observed < r.Probability-tolerance || observed > r.Probability+tolerance {
		t.Fatalf("observed fire rate %.4f, want ~%.2f (+/- %.2f)", observed, r.Probability, tolerance)
	}
}

// TestRule_Validate checks the validation choke point catches each
// class of bad input.
func TestRule_Validate(t *testing.T) {
	base := Rule{
		ID:          "ok",
		Target:      Target{Service: "S", Method: "M", Client: "*"},
		FaultType:   FaultLatency,
		Probability: 0.5,
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("expected valid rule to pass, got %v", err)
	}

	missingID := base
	missingID.ID = ""
	if err := missingID.Validate(); err == nil {
		t.Fatal("expected error for missing ID")
	}

	badProb := base
	badProb.Probability = 1.5
	if err := badProb.Validate(); err == nil {
		t.Fatal("expected error for out-of-range probability")
	}

	badFault := base
	badFault.FaultType = "not_a_real_type"
	if err := badFault.Validate(); err == nil {
		t.Fatal("expected error for unknown fault type")
	}
}
