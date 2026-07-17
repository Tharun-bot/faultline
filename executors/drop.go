package executors

import "errors"

// ErrConnectionDropped is a sentinel returned by InjectDropConnection.
// Interceptors translate this into whatever "connection died" looks
// like for their transport — e.g. closing the underlying net.Conn for
// a raw TCP case, or returning codes.Unavailable for gRPC (since gRPC
// clients can't easily distinguish "server closed the socket" from
// "server returned Unavailable" in a portable way across languages).
var ErrConnectionDropped = errors.New("faultline: connection dropped")

// InjectDropConnection is a marker function — right now it just returns
// the sentinel error. It exists as its own function (rather than
// inlining ErrConnectionDropped at call sites) so that if drop-connection
// semantics grow more complex later (e.g. actually closing a raw socket
// for the HTTP case), there's one place to change.
func InjectDropConnection() error {
	return ErrConnectionDropped
}
