package retailnetwork

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Errors the live gateway returns that a caller can test for.
var (
	// ErrCircuitOpen: the breaker guarding the Vendor API refused the call
	// without trying it (ADR 0004 §2, ADR 0026 Decision 8).
	ErrCircuitOpen = errors.New("retail-network: circuit breaker open, call not attempted")

	// ErrUnknownRef: no purchase order is recorded under the NetworkRef, so no
	// answer can be built for it.
	ErrUnknownRef = errors.New("retail-network: no purchase order is recorded for this ref")

	// ErrNoLabel: a shipment cannot be confirmed before its labels exist, the
	// same rule the network enforces (confirm-without-label).
	ErrNoLabel = errors.New("retail-network: no label is recorded for this order; request the label before confirming shipment")

	// ErrBadAcknowledgement: the answer handed to SubmitAcknowledgement cannot be
	// put on the wire (an accept with a reason, a reject without one, or a
	// reason that is internal to the fleet).
	ErrBadAcknowledgement = errors.New("retail-network: this acknowledgement cannot be sent")

	// ErrBadPackageCount: a label was requested for fewer than one package.
	ErrBadPackageCount = errors.New("retail-network: an order has at least one package")

	// ErrWriteRefused: the network answered a write with a definitive refusal
	// (for example ack-window-closed). The ledger row is FAILURE and the write
	// is never sent again; the wrapped *APIError carries the problem.
	ErrWriteRefused = errors.New("retail-network: the network refused this write")
)

// APIError is the network answering with a problem (RFC 9457) that is not worth
// retrying: any 4xx other than 429, and a 5xx that outlived the retries.
type APIError struct {
	Op     string // the operation, for logs
	Status int
	Type   string // the problem slug, for example "ack-window-closed"
	Title  string
	Detail string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("retail-network: %s: %d %s: %s", e.Op, e.Status, e.Type, e.Detail)
}

// Problem returns the slug of err when it is an *APIError, or "".
func Problem(err error) string {
	var api *APIError
	if errors.As(err, &api) {
		return api.Type
	}
	return ""
}

// RateLimitedError: the network said 429 and either the wait it asked for is
// longer than this call may wait, or the attempts ran out.
type RateLimitedError struct {
	Op         string
	RetryAfter time.Duration
}

func (e *RateLimitedError) Error() string {
	return fmt.Sprintf("retail-network: %s: rate limited, retry after %s", e.Op, e.RetryAfter)
}

// serverError is a 5xx: the dependency failed, so the breaker counts it and the
// call is retried.
type serverError struct {
	status int
}

func (e *serverError) Error() string { return fmt.Sprintf("retail-network answered %d", e.status) }

// retryAfter parses a Retry-After header: whole seconds, or an HTTP date.
func retryAfter(h http.Header, now time.Time) (time.Duration, bool) {
	v := strings.TrimSpace(h.Get("Retry-After"))
	if v == "" {
		return 0, false
	}
	var secs int
	if _, err := fmt.Sscanf(v, "%d", &secs); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second, true
	}
	if t, err := http.ParseTime(v); err == nil {
		// A date in the past is "retry now", not a negative wait.
		return max(t.Sub(now), 0), true
	}
	return 0, false
}
