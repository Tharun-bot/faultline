package httpfault

import (
	"net/http"
	"time"

	"github.com/Tharun-bot/faultline/core"
	"github.com/Tharun-bot/faultline/executors"
	"github.com/Tharun-bot/faultline/telemetry"
)

func applyFault(w http.ResponseWriter, r *http.Request, next http.Handler, rule core.Rule, metrics *telemetry.Metrics) {
	switch rule.FaultType {

	case core.FaultLatency:
		d := time.Duration(rule.Params.LatencyMS) * time.Millisecond
		if err := executors.InjectLatency(r.Context(), d); err != nil {
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
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				conn.Close()
				return
			}
		}
		http.Error(w, "connection dropped", http.StatusServiceUnavailable)

	case core.FaultCorruptPayload:
		rec := newResponseRecorder(w)
		next.ServeHTTP(rec, r)
		corrupted := executors.CorruptPayload(rec.body.Bytes(), rule.Params.CorruptPct)
		rec.body.Reset()
		rec.body.Write(corrupted)
		rec.flush()

	case core.FaultPartialFailure:
		next.ServeHTTP(w, r)

	default:
		next.ServeHTTP(w, r)
	}
}

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
