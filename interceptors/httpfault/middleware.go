package httpfault

import (
	"net/http"

	"github.com/Tharun-bot/faultline/core"
)

// clientHeader is the HTTP header we expect callers to set identifying
// themselves, mirroring grpcfault's clientMetadataKey. Kept as its own
// constant (not shared with grpcfault) since HTTP headers and gRPC
// metadata keys have different naming conventions/casing rules, and
// tying them together would create a coupling between two packages
// that should otherwise be fully independent.
const clientHeader = "X-Faultline-Client"

// serviceName is passed in explicitly at middleware construction time,
// NOT derived from the request the way gRPC derives Service from
// info.FullMethod. Plain HTTP has no equivalent built-in concept of
// "service name" — a URL path doesn't reliably map to one the way a
// gRPC method's fully-qualified name does. So the middleware is told
// which logical service it's protecting, and Method is derived from
// the request path instead.
type RuleSource interface {
	Find(cc core.CallContext) (core.Rule, bool)
}

// Middleware builds HTTP middleware for a given logical service name,
// backed by the given RuleSource. serviceName should match whatever
// Target.Service value your rules use (e.g. "OrderService") — same
// RuleSource/Cache from Phase 5 can back both the gRPC and HTTP
// interceptors simultaneously if a service exposes both protocols.
func Middleware(serviceName string, rules RuleSource) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cc := core.CallContext{
				Service: serviceName,
				Method:  r.URL.Path, // e.g. "/orders/create" — the closest HTTP equivalent to a gRPC method name
				Client:  r.Header.Get(clientHeader),
			}

			rule, matched := rules.Find(cc)
			if !matched || !core.ShouldFire(rule) {
				next.ServeHTTP(w, r)
				return
			}

			applyFault(w, r, next, rule)
		})
	}
}
