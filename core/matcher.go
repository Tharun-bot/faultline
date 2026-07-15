package core

import "math/rand"

// CallContext describes one incoming call at the moment we need to
// decide whether to inject a fault. It's deliberately a separate type
// from Target — Target is "what a rule applies to" (can have wildcards),
// CallContext is "what actually happened" (always concrete values).
type CallContext struct {
	Service string
	Method  string
	Client  string // extracted from request metadata/headers upstream
}

// matches reports whether this rule's target applies to the given call.
// Service and Method must match exactly. Client matches if the rule's
// Client is "*" (wildcard) or equals the call's client exactly.
func (r Rule) matches(cc CallContext) bool {
	if r.Target.Service != cc.Service {
		return false
	}
	if r.Target.Method != cc.Method {
		return false
	}
	if r.Target.Client != "*" && r.Target.Client != cc.Client {
		return false
	}
	return true
}

// Matcher holds the current set of known rules and decides, for a given
// call, which single rule (if any) should fire. It does NOT talk to
// Redis — it just operates on whatever rules were handed to it. Phase 5's
// local cache is what's responsible for keeping this set up to date.
type Matcher struct {
	rules []Rule
}

// NewMatcher builds a Matcher from a slice of rules. We only keep
// Active rules — inactive ones are filtered out immediately so the
// hot path (Find) never has to check r.Active per call.
func NewMatcher(rules []Rule) *Matcher {
	active := make([]Rule, 0, len(rules))
	for _, r := range rules {
		if r.Active {
			active = append(active, r)
		}
	}
	return &Matcher{rules: active}
}

// Find returns the first rule whose Target matches the given call.
// Design choice: we return at most ONE matching rule, not all matches.
// In production fault injection you almost never want to stack two
// faults on the same call (e.g. latency AND error) because it makes
// the experiment's cause-and-effect ambiguous. If two rules could match
// the same call, that's a configuration mistake we accept silently for
// now and revisit if it becomes a real need.
func (m *Matcher) Find(cc CallContext) (Rule, bool) {
	for _, r := range m.rules {
		if r.matches(cc) {
			return r, true
		}
	}
	return Rule{}, false
}

// ShouldFire rolls the dice for a matched rule's Probability and
// reports whether the fault should actually be injected THIS time.
// Separated from Find() deliberately: Find is deterministic and easy
// to unit test ("does matching logic work"), ShouldFire is random and
// tested statistically over many trials ("does probability hold up").
// Keeping them as two functions means a test can call Find alone with
// no randomness involved.
func ShouldFire(r Rule) bool {
	return rand.Float64() < r.Probability
}
