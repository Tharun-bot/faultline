package executors

// PartialResult describes the outcome of a partial-failure injection
// over a batch of N items: the first OKCount succeed, the rest fail.
// This type exists so callers (interceptors handling batch/streaming
// RPCs) get a clear, structured answer instead of having to re-derive
// "which indices succeeded" themselves.
type PartialResult struct {
	OKCount    int
	TotalCount int
}

// Succeeded reports whether the item at the given zero-based index
// should be treated as successful.
func (p PartialResult) Succeeded(index int) bool {
	return index < p.OKCount
}

// InjectPartialFailure builds a PartialResult for a batch of size total,
// where the first okCount items succeed. It clamps okCount into
// [0, total] so a misconfigured rule (e.g. okCount > total) can't
// produce a nonsensical result.
func InjectPartialFailure(total, okCount int) PartialResult {
	if okCount < 0 {
		okCount = 0
	}
	if okCount > total {
		okCount = total
	}
	return PartialResult{OKCount: okCount, TotalCount: total}
}
