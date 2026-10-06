package http

import (
	"errors"
	"net/http"

	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// statusFor maps a typed domain/application error to an HTTP status code.
func statusFor(err error) int {
	switch {
	case errors.Is(err, usecases.ErrOrderNotFound):
		return http.StatusNotFound

	// The order exists but is in the wrong lifecycle state for this
	// request: confirming shipment before the acknowledgement settled.
	// A conflict with current resource state, which the caller can
	// resolve, not a server bug.
	case errors.Is(err, networkorder.ErrConfirmBeforeAcknowledge):
		return http.StatusConflict

	// A product the Anti-Corruption Layer cannot translate is a business
	// fact about OUR catalogue, not a malformed request: the network
	// asked for something we do not stock. 422 rather than 400 because
	// the request was perfectly well formed.
	case errors.Is(err, shared.ErrUnknownProduct):
		return http.StatusUnprocessableEntity

	case errors.Is(err, shared.ErrEmptyNetworkRef),
		errors.Is(err, shared.ErrEmptyNetworkLineRef),
		errors.Is(err, shared.ErrEmptyNetworkProductId),
		errors.Is(err, shared.ErrNonPositiveQuantity),
		errors.Is(err, shared.ErrNoLines):
		return http.StatusUnprocessableEntity

	default:
		return http.StatusInternalServerError
	}
}

// problemBaseURI is the namespace for this service's RFC 7807 "type"
// URIs. It does not need to resolve to a real page — it is an
// identifier, unique per distinct error category in this service.
const problemBaseURI = "https://errors.network-fulfillment.warehouse-systems.dev/"

// problemInfo is the fixed, category-level (type, title) pair for an RFC
// 7807 problem response. slug becomes the last path segment of "type";
// title is a fixed human string for the category (the dynamic detail
// comes from err.Error() at write time, not from this table).
type problemInfo struct {
	slug  string
	title string
}

// problemFor maps a typed error to its RFC 7807 (type, title) pair.
//
// It MUST mirror statusFor's groupings one-for-one. An error mapped to a
// 4xx here but missing from this table falls through to internal-error,
// and the caller is told it hit a server bug when it hit a business
// rule — a defect this fleet has already shipped once, in
// order-management, where the status was right and the body was not.
// errors_test.go enforces the pairing.
func problemFor(err error) problemInfo {
	switch {
	case errors.Is(err, usecases.ErrOrderNotFound):
		return problemInfo{"network-order-not-found", "Network order not found"}

	case errors.Is(err, networkorder.ErrConfirmBeforeAcknowledge):
		return problemInfo{"confirm-before-acknowledge", "Shipment cannot be confirmed before the order is acknowledged"}

	case errors.Is(err, shared.ErrUnknownProduct):
		return problemInfo{"unknown-product", "No SKU is mapped to that network product id"}

	case errors.Is(err, shared.ErrEmptyNetworkRef):
		return problemInfo{"empty-network-ref", "Network ref must not be empty"}
	case errors.Is(err, shared.ErrEmptyNetworkLineRef):
		return problemInfo{"empty-network-line-ref", "Network line ref must not be empty"}
	case errors.Is(err, shared.ErrEmptyNetworkProductId):
		return problemInfo{"empty-network-product-id", "Network product id must not be empty"}
	case errors.Is(err, shared.ErrNonPositiveQuantity):
		return problemInfo{"non-positive-quantity", "Quantity must be greater than zero"}
	case errors.Is(err, shared.ErrNoLines):
		return problemInfo{"order-without-lines", "Demand must have at least one line"}

	default:
		return problemInfo{"internal-error", "An unexpected internal error occurred"}
	}
}
