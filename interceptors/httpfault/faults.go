package httpfault

import (
	"net/http"
	"time"

	"github.com/Tharun-bot/faultline/core"
	"github.com/Tharun-bot/faultline/executors"
)

// applyFault mirrors grpcfault.applyFault's switch, but every branch
// is expressed in terms of http.ResponseWriter/http.Request instead of
// typed proto messages — this is the only place HTTP-specific
// mechanics live; the DECISION of which fault type to apply already
// happened identically to the gRPC path, in core.Matcher/ShouldFire.
func applyFault(w http.ResponseWriter, r *http.Request, next http.Handler, rule core.Rule) {
	switch rule.FaultType {

	case core.FaultLatency:
		if err := executors.InjectLatency(r.Context(), time.Duration(rule.Params.LatencyMS)*time.Millisecond); err != nil {
			// Client's own context was cancelled during our injected
			// sleep (e.g. they set a client-side timeout) — nothing
			// useful to write back, the connection is likely already
			// gone from the client's perspective.
			return
		}
		// Latency is additive, not a replacement — proceed to the real
		// handler after sleeping, same as the gRPC latency branch.
		next.ServeHTTP(w, r)

	case core.FaultError:
		err := executors.InjectError(rule.Params.ErrorCode)
		ie, _ := executors.AsInjectedError(err)
		// Error injection is a short-circuit — the real handler never
		// runs. We translate our generic Code string into an HTTP
		// status the same way grpcfault translates it into a
		// grpc/codes.Code.
		http.Error(w, err.Error(), httpStatusFromString(ie.Code))

	case core.FaultDropConnection:
		// HTTP actually CAN do a real drop, unlike gRPC's approximation
		// — hijacking the underlying TCP connection and closing it
		// without writing any response at all is a faithful simulation
		// of "the server vanished mid-request." We attempt a hijack and
		// fall back to a 503 if the underlying transport doesn't
		// support it (e.g. HTTP/2 connections generally don't support
		// hijacking).
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
		// Same rationale as grpcfault: partial failure is a batch/stream
		// concept that doesn't map cleanly onto a single HTTP
		// request/response. No-op here; this fault type is really for
		// Phase 8's Kafka consumer.
		next.ServeHTTP(w, r)

	default:
		next.ServeHTTP(w, r)
	}
}

// httpStatusFromString mirrors grpcfault's grpcCodeFromString mapping,
// but into HTTP status codes instead of grpc/codes.Code. Kept
// deliberately separate (not shared) since the two protocols' error
// vocabularies don't line up one-to-one (e.g. gRPC's
// DEADLINE_EXCEEDED and HTTP's 504 Gateway Timeout are close but not
// semantically identical) — an explicit per-protocol mapping is more
// honest than pretending there's a universal translation.
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
