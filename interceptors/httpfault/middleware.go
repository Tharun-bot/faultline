package httpfault

import (
	"net/http"

	"github.com/Tharun-bot/faultline/core"
	"github.com/Tharun-bot/faultline/telemetry"
)

// clientHeader is the HTTP header we expect callers to set identifying
// themselves, mirroring grpcfault's clientMetadataKey.
const clientHeader = "X-Faultline-Client"

// RuleSource is the interface the middleware depends on to find
// active rules.
type RuleSource interface {
	Find(cc core.CallContext) (core.Rule, bool)
}

// Middleware builds HTTP middleware for a given logical service name,
// backed by the given RuleSource. metrics may be nil (treated as a
// no-op) for tests that don't care about metrics.
func Middleware(serviceName string, rules RuleSource, metrics *telemetry.Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cc := core.CallContext{
				Service: serviceName,
				Method:  r.URL.Path,
				Client:  r.Header.Get(clientHeader),
			}

			rule, matched := rules.Find(cc)
			if !matched || !core.ShouldFire(rule) {
				next.ServeHTTP(w, r)
				return
			}

			if metrics != nil {
				metrics.RecordInjection(string(rule.FaultType), rule.ID)
			}

			applyFault(w, r, next, rule, metrics)
		})
	}
}
