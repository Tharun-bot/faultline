package httpfault

import (
	"bytes"
	"net/http"
)

// responseRecorder implements http.ResponseWriter but captures
// everything written into an in-memory buffer instead of sending it
// immediately, so applyFault can corrupt the full response body
// before it ever reaches the real client.
type responseRecorder struct {
	http.ResponseWriter
	statusCode int
	body       bytes.Buffer
}

func newResponseRecorder(w http.ResponseWriter) *responseRecorder {
	return &responseRecorder{ResponseWriter: w, statusCode: http.StatusOK}
}

func (rr *responseRecorder) WriteHeader(code int) {
	rr.statusCode = code
}

func (rr *responseRecorder) Write(b []byte) (int, error) {
	return rr.body.Write(b)
}

// flush sends the (possibly corrupted) buffered response to the real
// underlying ResponseWriter. Must be called exactly once.
func (rr *responseRecorder) flush() {
	rr.ResponseWriter.WriteHeader(rr.statusCode)
	rr.ResponseWriter.Write(rr.body.Bytes())
}
