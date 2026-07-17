package grpcfault

import "github.com/Tharun-bot/faultline/core"

// StaticRuleSource is a trivial RuleSource backed by an in-memory
// Matcher, used for Phase 3 testing before Redis-backed rules exist
// in Phase 4/5. It satisfies the RuleSource interface the interceptor
// depends on.
type StaticRuleSource struct {
	matcher *core.Matcher
}

func NewStaticRuleSource(rules []core.Rule) *StaticRuleSource {
	return &StaticRuleSource{matcher: core.NewMatcher(rules)}
}

func (s *StaticRuleSource) Find(cc core.CallContext) (core.Rule, bool) {
	return s.matcher.Find(cc)
}
