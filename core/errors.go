package core

import "errors"

// Sentinel errors for Rule validation. Using errors.New + package-level
// vars (rather than fmt.Errorf inline) means callers can compare with
// errors.Is(err, core.ErrRuleMissingID) instead of string-matching.
var (
	errRuleMissingID      = errors.New("rule: id is required")
	errRuleMissingTarget  = errors.New("rule: target.service and target.method are required")
	errRuleBadProbability = errors.New("rule: probability must be between 0 and 1")
	errRuleBadFaultType   = errors.New("rule: unknown fault_type")
)
