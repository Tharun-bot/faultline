package executors

import "math/rand"

// CorruptPayload mutates a copy of the given bytes, flipping roughly
// pct percent of them to random values. It returns a NEW slice rather
// than mutating in place — callers (interceptors) usually hold the
// original response and shouldn't have it silently changed out from
// under them via aliasing; returning a copy makes the data flow explicit
// at the call site ("here is the corrupted version, you choose to use it").
func CorruptPayload(data []byte, pct int) []byte {
	if pct <= 0 || len(data) == 0 {
		return data
	}
	if pct > 100 {
		pct = 100
	}

	out := make([]byte, len(data))
	copy(out, data)

	// Number of bytes to corrupt, at least 1 if pct > 0 and data is non-empty.
	n := (len(out) * pct) / 100
	if n == 0 {
		n = 1
	}

	for i := 0; i < n; i++ {
		idx := rand.Intn(len(out))
		out[idx] = byte(rand.Intn(256))
	}

	return out
}
