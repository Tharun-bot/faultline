package httpfault

import (
	"bytes"
	"net/http"
)

// responseRecorder implements http.ResponseWriter but captures
// everything written into an in-memory buffer instead of sending it
// immediately. This lets us apply corruption to the FULL response body
// before it ever reaches the real client — exactly mirroring what
// grpcfault's corruptResponse does with a typed proto message, just
// operating on raw bytes instead since HTTP has no schema.
type responseRecorder struct {
	http.ResponseWriter
	statusCode int
	body       bytes.Buffer
}

func newResponseRecorder(w http.ResponseWriter) *responseRecorder {
	return &responseRecorder{ResponseWriter: w, statusCode: http.StatusOK}
}

// WriteHeader captures the status code instead of immediately sending
// it, since we might still need to short-circuit or otherwise haven't
// decided the final response yet.
func (rr *responseRecorder) WriteHeader(code int) {
	rr.statusCode = code
}

// Write captures bytes into our buffer instead of the real
// ResponseWriter. We deliberately do NOT call rr.ResponseWriter.Write
// here — nothing reaches the real client until flush() is called
// explicitly, which is what lets applyFault corrupt the buffered
// bytes first.
func (rr *responseRecorder) Write(b []byte) (int, error) {
	return rr.body.Write(b)
}

// flush sends the (possibly corrupted) buffered response to the real
// underlying ResponseWriter. Must be called exactly once, after any
// desired mutation of rr.body has happened.
func (rr *responseRecorder) flush() {
	rr.ResponseWriter.WriteHeader(rr.statusCode)
	rr.ResponseWriter.Write(rr.body.Bytes())
}
