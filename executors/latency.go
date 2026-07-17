package executors

import (
	"context"
	"time"
)

// InjectLatency blocks for the given duration, then returns nil — UNLESS
// the context is cancelled first (e.g. the caller's own deadline expires
// during our injected sleep), in which case it returns ctx.Err() early.
//
// This context-awareness matters: if we used a plain time.Sleep(d), an
// injected 5-second latency fault would keep holding the goroutine even
// after the real caller gave up and disconnected — wasting resources and
// producing misleading traces. Respecting ctx.Done() makes the fault
// behave like a real slow dependency would.
func InjectLatency(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
