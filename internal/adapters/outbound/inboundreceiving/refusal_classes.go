package inboundreceiving

import "github.com/claudioed/network-fulfillment/internal/adapters/outbound/replayclass"

// The anti-corruption layer's reading of inbound-receiving's refusals (ADR 0037).
//
// inbound-receiving declares a replay class for every problem type it answers on an
// idempotent route (its ADR 0027). A RETRYABLE refusal is RELEASED: nothing was
// committed, the Idempotency-Key is free, and the same request under the SAME key
// executes again and may pass. A FINAL or OUTCOME refusal is stored and replayed for
// 24 h, so asking again only repeats it. The class is the callee's knowledge and lives
// here, in one visible table; the use case above only sees AsnRefusedError.Retryable.
//
// A slug that is not in the table is TERMINAL: an unknown refusal must never loop, and
// a 4xx without a problem document has no slug at all. refusal_spec_test.go holds the
// table against the byte-pinned copy of the callee's OpenAPI (contracts/pinned.json).
//
// The table is read for the refusals of POST /asns only, and only for 400, 404, 409
// and 422; a 401, 403, 429 or 5xx was never a verdict and stays a plain error.
var asnRefusals = replayclass.NewTable(
	// RETRYABLE at inbound-receiving (ADR 0027): nothing committed, the answer depends on
	// state that changes. What POST /asns answers today is the first group; the others
	// belong to other routes and are classified as the callee classifies them.
	[]string{
		"unknown-sku",             // the product projection has not caught up with product-master
		"duplicate-sscc",          // the SSCC sits on another ASN; cancelling that one frees it
		"concurrent-modification", // a write lost a race on a version-guarded save
		"idempotency-key-reused",  // answered before the store, never stored (a key row that expires frees it)
		"unknown-asn",
		"unknown-appointment",
		"asn-not-on-appointment",
		"line-not-on-asn",
		"asn-not-found",
		"appointment-not-found",
		"receiving-policy-not-found",
		"carton-not-found",
		"asn-in-progress",
		"asn-not-receivable",
		"receipt-already-open",
		"tolerance-override-required",
		"tolerance-blocked",
		"sscc-prefix-not-configured",
		"sscc-serial-exhausted",
		"counted-contents-not-mixed",
		"counted-sku-unknown",
		"scan-kind-mismatch",
		"unsupported-pack-level",
		"unknown-gtin",
		"version-mismatch",
	},
	[]string{
		// RETRYABLE at the callee, TERMINAL here: a mixed slug (invalid JSON, or an
		// unparsable If-Match) whose every cause on this route is a function of our own
		// request, the same bytes on every attempt. See the audit's releasedButTerminalHere.
		"malformed-request",
		// FINAL at the callee: stored and replayed.
		"asn-terminal", "appointment-cancelled", "receipt-closed", "receipt-not-found",
		"homogeneous-carton-mixed", "carton-contents-mismatch", "unsupported-gtin",
		"duplicate-sku", "duplicate-asn-number", "asn-requires-lines", "appointment-requires-asns",
		"invalid-sscc", "invalid-gtin", "invalid-carton", "invalid-carton-kind", "invalid-condition",
		"invalid-owner-ref", "invalid-supplier-ref", "invalid-asn-number", "invalid-sku", "invalid-line-no",
		"invalid-quantity", "invalid-scan", "invalid-scan-kind", "invalid-counted-contents", "invalid-cases",
		"invalid-override-reason", "invalid-tolerance-band", "invalid-carrier", "invalid-window",
		"invalid-reason", "invalid-appointment-id", "invalid-receipt-id", "invalid-door-code",
		"invalid-limit", "invalid-cursor", "invalid-query", "unknown-query-parameter",
		// The key problems: this client sends a valid key (nf-asn-<number>, 1 to 128 printable characters).
		"idempotency-key-required", "idempotency-key-invalid",
		// Named by the OpenAPI text, absent from the callee's class table and from this
		// route: terminal, as every unknown slug.
		"unknown-dock-door",
	},
)
