package executors

import "testing"

func TestInjectPartialFailure_Basic(t *testing.T) {
	pr := InjectPartialFailure(10, 6)
	for i := 0; i < 6; i++ {
		if !pr.Succeeded(i) {
			t.Fatalf("expected index %d to succeed", i)
		}
	}
	for i := 6; i < 10; i++ {
		if pr.Succeeded(i) {
			t.Fatalf("expected index %d to fail", i)
		}
	}
}

func TestInjectPartialFailure_ClampsOKCount(t *testing.T) {
	pr := InjectPartialFailure(5, 999) // okCount way over total
	if pr.OKCount != 5 {
		t.Fatalf("expected OKCount clamped to 5, got %d", pr.OKCount)
	}

	pr2 := InjectPartialFailure(5, -3) // negative okCount
	if pr2.OKCount != 0 {
		t.Fatalf("expected OKCount clamped to 0, got %d", pr2.OKCount)
	}
}
