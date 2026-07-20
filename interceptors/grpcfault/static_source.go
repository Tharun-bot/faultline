package grpcfault

import "github.com/Tharun-bot/faultline/core"

// StaticRuleSource is a trivial RuleSource backed by an in-memory
// Matcher, used for tests and simple demos before Redis-backed rules
// exist. It satisfies the RuleSource interface the interceptor
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
