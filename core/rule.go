package core

import "time"

// FaultType enumerates the kinds of chaos Faultline can inject.
// We use a string-based type (not raw string) so the compiler catches
// typos like FaultType("laytency") if you ever construct one directly —
// though in practice rules will usually come from Redis/JSON as strings,
// so we still validate at the boundary (see Validate() below).
type FaultType string

const (
	FaultLatency        FaultType = "latency"
	FaultError          FaultType = "error"
	FaultCorruptPayload FaultType = "corrupt_payload"
	FaultDropConnection FaultType = "drop_connection"
	FaultPartialFailure FaultType = "partial_failure"
)

// Target describes WHICH calls a rule applies to.
// A rule matches a call if service+method match exactly, and if
// Client is either "*" (wildcard, matches any caller) or matches the
// caller's identity exactly.
type Target struct {
	Service string `json:"service"` // e.g. "OrderService"
	Method  string `json:"method"`  // e.g. "Create"
	Client  string `json:"client"`  // e.g. "checkout-service", or "*" for any
}

// Params holds fault-type-specific configuration. We keep this as a
// flat struct with all possible fields rather than separate structs
// per fault type, because it needs to serialize cleanly to/from Redis
// as JSON, and a single flat shape is much simpler to marshal/unmarshal
// than a tagged union (Go has no native sum types). Unused fields for
// a given fault type are just left at zero value.
type Params struct {
	LatencyMS      int    `json:"latency_ms,omitempty"`       // used by FaultLatency
	ErrorCode      string `json:"error_code,omitempty"`       // used by FaultError, e.g. "UNAVAILABLE"
	CorruptPct     int    `json:"corrupt_pct,omitempty"`      // used by FaultCorruptPayload: % of bytes to flip
	PartialOKCount int    `json:"partial_ok_count,omitempty"` // used by FaultPartialFailure
}

// Rule is the single unit of chaos configuration. This is the exact
// struct that gets serialized to JSON and stored in Redis (Phase 4),
// and it's what the Matcher operates on in memory.
type Rule struct {
	ID          string    `json:"id"`
	Target      Target    `json:"target"`
	FaultType   FaultType `json:"fault_type"`
	Params      Params    `json:"params"`
	Probability float64   `json:"probability"` // 0.0 to 1.0
	Active      bool      `json:"active"`
	Version     int       `json:"version"`    // bumped on every write, used for cache invalidation
	ExpiresAt   time.Time `json:"expires_at"` // safety auto-off; zero value = never expires
}

// Validate checks that a Rule is well-formed before it's ever allowed
// to be saved or matched against. This is our single choke point for
// bad data — better to reject here than to silently no-op or panic
// deep inside the matcher during a live request.
func (r Rule) Validate() error {
	if r.ID == "" {
		return errRuleMissingID
	}
	if r.Target.Service == "" || r.Target.Method == "" {
		return errRuleMissingTarget
	}
	if r.Probability < 0 || r.Probability > 1 {
		return errRuleBadProbability
	}
	switch r.FaultType {
	case FaultLatency, FaultError, FaultCorruptPayload, FaultDropConnection, FaultPartialFailure:
		// ok
	default:
		return errRuleBadFaultType
	}
	return nil
}
