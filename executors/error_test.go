package executors

import (
	"errors"
	"testing"
)

func TestInjectError_ProducesIdentifiableError(t *testing.T) {
	err := InjectError("UNAVAILABLE")
	if err == nil {
		t.Fatal("expected non-nil error")
	}

	ie, ok := AsInjectedError(err)
	if !ok {
		t.Fatal("expected AsInjectedError to succeed")
	}
	if ie.Code != "UNAVAILABLE" {
		t.Fatalf("expected code UNAVAILABLE, got %s", ie.Code)
	}
}

func TestAsInjectedError_RejectsRealErrors(t *testing.T) {
	realErr := errors.New("some genuine application error")
	_, ok := AsInjectedError(realErr)
	if ok {
		t.Fatal("expected AsInjectedError to reject a non-injected error")
	}
}
