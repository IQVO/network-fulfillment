package ordermanagement

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/replayclass"
)

// The anti-corruption layer's reading of order-management's refusals (ADR 0037).
//
// order-management declares a replay class for every problem type it answers on an
// idempotent route (its ADR 0054). A RETRYABLE refusal is RELEASED: nothing was committed,
// the Idempotency-Key is free, and the same request under the SAME key executes again and
// may pass. A FINAL or OUTCOME refusal is stored and replayed for 24 h. The class is the
// callee's knowledge and lives here, in one visible table.
//
// A slug that is not in the table is TERMINAL: an unknown refusal must never loop, and a
// 4xx without a problem document has no slug at all. refusal_classes_test.go holds the
// table equal to a copy of the callee's class table and checks that every problem type
// the pinned OpenAPI names on the two keyed routes is classified.
//
// The table is read for the refusals of the two keyed routes this client calls,
// POST /orders (RaiseHeldOrder, key netful-raise-<networkRef>) and
// POST /orders/{id}/releases (ReleaseHeldOrder, key netful-release-<localOrderId>), and
// only for 400, 404, 409 and 422. DELETE /orders/{id} is not keyed: its answers keep the
// classification of ADR 0024 section 4. A 5xx and a 429 were never verdicts.
var heldOrderRefusals = replayclass.NewTable(
	// RETRYABLE at order-management: nothing committed, the answer depends on state that
	// changes. The first two are what POST /orders answers from an event-fed catalogue;
	// concurrent-modification and order-not-held are what the release can answer.
	[]string{
		"unknown-process-path",              // the process-path catalogue is fed by events
		"line-ineligible-for-resolved-path", // product classification and catalogue are fed by events
		"concurrent-modification",           // the write lost an optimistic-concurrency race
		"order-not-held",
		// RETRYABLE at the callee, not answered by these two calls today (or a 5xx, which
		// is never read as a refusal): classified as the callee classifies them.
		"no-backordered-lines", "no-allocated-lines", "promise-date-not-set", "order-already-released",
		"order-line-already-allocated", "order-line-not-allocated", "order-line-not-backordered",
		"order-line-not-found", "order-line-not-pending",
		"insufficient-stock", "downstream-not-configured", "downstream-unavailable",
		"internal-error", "report-store-error",
	},
	// FINAL at order-management (it has no OUTCOME): stored and replayed, a retry only repeats it.
	[]string{
		"malformed-request-body", "order-without-lines", "empty-sku", "empty-order-id", "empty-path-id",
		"invalid-fulfillment-site-id", "non-positive-quantity", "held-order-must-be-ship-complete",
		"order-not-found", "invalid-query-parameter", "invalid-report-query",
		// The key problems: raiseIdempotencyKey and releaseIdempotencyKey are never empty and
		// print only characters of the network ref and the order id.
		"idempotency-key-required", "idempotency-key-invalid", "idempotency-key-reused",
	},
)

// RefusalError is order-management refusing a keyed call (400, 404, 409 or 422) with the
// class of its problem type. Released means it releases the Idempotency-Key for that
// problem type: the same request under the same key may pass later. It wraps the plain
// status error, so a caller that only knows "non-2xx" reads it as before.
type RefusalError struct {
	Status   int
	Problem  string // the slug of the problem document; empty when there is none
	Detail   string
	Released bool
	cause    error
}

func (e *RefusalError) Error() string {
	switch {
	case e.Problem == "":
		return fmt.Sprintf("%s: no problem document, terminal", e.cause)
	case e.Released:
		return fmt.Sprintf("%s %s: released, a retry under the same key may pass", e.cause, e.Problem)
	}
	return fmt.Sprintf("%s %s: final, a retry under the same key is answered the same", e.cause, e.Problem)
}

func (e *RefusalError) Unwrap() error { return e.cause }

// asRefusal turns a status error of a KEYED call into a RefusalError by the table; any
// other error, and any other status, is returned unchanged.
func asRefusal(err error) error {
	var status *statusError
	if !errors.As(err, &status) {
		return err
	}
	switch status.code {
	case http.StatusBadRequest, http.StatusNotFound, http.StatusConflict, http.StatusUnprocessableEntity:
		slug, detail := replayclass.ProblemOf(status.body)
		return &RefusalError{Status: status.code, Problem: slug, Detail: detail, Released: heldOrderRefusals.Retryable(slug), cause: err}
	}
	return err
}
