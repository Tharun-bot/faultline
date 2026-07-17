package executors

import (
	"context"
	"testing"
	"time"
)

func TestInjectLatency_SleepsApproximately(t *testing.T) {
	start := time.Now()
	err := InjectLatency(context.Background(), 50*time.Millisecond)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	// Allow generous slack for scheduler jitter in CI environments —
	// we only care that it's in the right ballpark, not exact to the ms.
	if elapsed < 40*time.Millisecond || elapsed > 200*time.Millisecond {
		t.Fatalf("expected ~50ms sleep, took %v", elapsed)
	}
}

func TestInjectLatency_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := InjectLatency(ctx, 5*time.Second) // much longer than the ctx timeout
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected context deadline error, got nil")
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("expected early return near 10ms due to ctx cancellation, took %v", elapsed)
	}
}

func TestInjectLatency_ZeroDurationNoOp(t *testing.T) {
	start := time.Now()
	err := InjectLatency(context.Background(), 0)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if elapsed > 5*time.Millisecond {
		t.Fatalf("expected near-instant return for zero duration, took %v", elapsed)
	}
}
