package httpfault

import (
	"net/http"
	"time"

	"github.com/Tharun-bot/faultline/core"
	"github.com/Tharun-bot/faultline/executors"
	"github.com/Tharun-bot/faultline/telemetry"
)

// applyFault mirrors grpcfault.applyFault's switch, but every branch
// is expressed in terms of http.ResponseWriter/http.Request instead of
// typed proto messages.
func applyFault(w http.ResponseWriter, r *http.Request, next http.Handler, rule core.Rule, metrics *telemetry.Metrics) {
	switch rule.FaultType {

	case core.FaultLatency:
		d := time.Duration(rule.Params.LatencyMS) * time.Millisecond
		if err := executors.InjectLatency(r.Context(), d); err != nil {
			// Client's own context was cancelled during our injected
			// sleep — nothing useful to write back.
			return
		}
		if metrics != nil {
			metrics.RecordLatencyInjection(rule.ID, d.Seconds())
		}
		next.ServeHTTP(w, r)

	case core.FaultError:
		err := executors.InjectError(rule.Params.ErrorCode)
		ie, _ := executors.AsInjectedError(err)
		http.Error(w, err.Error(), httpStatusFromString(ie.Code))

	case core.FaultDropConnection:
		// HTTP can do a real drop, unlike gRPC's approximation —
		// hijacking the underlying TCP connection and closing it
		// without writing any response simulates "the server vanished
		// mid-request." Falls back to a 503 if the transport doesn't
		// support hijacking (e.g. HTTP/2).
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				conn.Close()
				return
			}
		}
		http.Error(w, "connection dropped", http.StatusServiceUnavailable)

	case core.FaultCorruptPayload:
		// Corruption needs the REAL response body first, so we run the
		// real handler against our recording writer, then mutate the
		// captured bytes before flushing to the real client.
		rec := newResponseRecorder(w)
		next.ServeHTTP(rec, r)
		corrupted := executors.CorruptPayload(rec.body.Bytes(), rule.Params.CorruptPct)
		rec.body.Reset()
		rec.body.Write(corrupted)
		rec.flush()

	case core.FaultPartialFailure:
		// Batch/stream concept that doesn't map onto a single HTTP
		// request/response — no-op here.
		next.ServeHTTP(w, r)

	default:
		next.ServeHTTP(w, r)
	}
}

// httpStatusFromString mirrors grpcfault's grpcCodeFromString mapping,
// but into HTTP status codes. Kept separate since the two protocols'
// error vocabularies don't line up one-to-one.
func httpStatusFromString(code string) int {
	switch code {
	case "UNAVAILABLE":
		return http.StatusServiceUnavailable
	case "DEADLINE_EXCEEDED":
		return http.StatusGatewayTimeout
	case "INTERNAL":
		return http.StatusInternalServerError
	case "RESOURCE_EXHAUSTED":
		return http.StatusTooManyRequests
	case "UNAUTHENTICATED":
		return http.StatusUnauthorized
	default:
		return http.StatusInternalServerError
	}
}
