package executors

import "errors"

// InjectedError wraps a synthetic error code so callers up the stack
// (interceptors) can distinguish "this failure was injected by
// Faultline" from a real application error, e.g. for metrics/logging.
// We don't want a real bug in OrderService to get miscounted as a
// successful fault injection, or vice versa.
type InjectedError struct {
	Code string // e.g. "UNAVAILABLE", "DEADLINE_EXCEEDED" — transport-agnostic string
}

func (e *InjectedError) Error() string {
	return "faultline: injected error " + e.Code
}

// InjectError constructs the synthetic error for a given error code.
// It's deliberately trivial right now — the real translation from this
// generic Code string into an actual grpc.Status or http.StatusCode
// happens in the interceptor layer (Phase 3/7), because only the
// interceptor knows which protocol it's speaking. This function's job
// is just to produce a well-formed, identifiable error value.
func InjectError(code string) error {
	return &InjectedError{Code: code}
}

// AsInjectedError is a small helper so interceptors can check "was this
// error one we injected" using errors.As, rather than every interceptor
// re-implementing a type assertion.
func AsInjectedError(err error) (*InjectedError, bool) {
	var ie *InjectedError
	ok := errors.As(err, &ie)
	return ie, ok
}
