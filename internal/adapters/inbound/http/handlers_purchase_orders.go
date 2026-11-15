package http

import (
	"net/http"
	"sort"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/network-fulfillment/internal/application/usecases"
)

// GET /purchase-orders/{poNumber} (ADR 0036) is a KEYED READ of the same
// NetworkOrder as GET /network-orders/{networkRef}, for a caller that knows the
// network's purchase-order number and not the NetworkRef this context minted
// for it (ADR 0026). It is not a listing: no query parameter, no paging.

const (
	// maxPurchaseOrderNumberLength bounds the path segment. The number is
	// opaque, so this is a sanity limit, not a format.
	maxPurchaseOrderNumberLength = 128

	// purchaseOrderNumberRule is the 400 detail and what apis/openapi.yaml
	// documents for the poNumber parameter.
	purchaseOrderNumberRule = "the purchase order number must be 1 to 128 characters, each a letter, a digit, '-', '.', '_' or '~'"
)

var problemInvalidPurchaseOrderNumber = problemInfo{"invalid-purchase-order-number", "Invalid purchase order number"}

// validPurchaseOrderNumber reports whether s can be a purchase-order number as
// far as this adapter may tell: 1 to maxPurchaseOrderNumberLength characters of
// the RFC 3986 unreserved set. No other shape is assumed or parsed (the number
// is another context's identifier); a percent-escape, a space or a separator
// is simply not a path segment this route can name.
func validPurchaseOrderNumber(s string) bool {
	if s == "" || len(s) > maxPurchaseOrderNumberLength {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '-', c == '.', c == '_', c == '~':
		default:
			return false
		}
	}
	return true
}

// firstQueryParameter returns the alphabetically first query parameter of r,
// or "" when there is none: this route declares no parameter at all.
func firstQueryParameter(r *http.Request) string {
	var names []string
	for name := range r.URL.Query() {
		names = append(names, name)
	}
	if len(names) == 0 {
		return ""
	}
	sort.Strings(names)
	return names[0]
}

func (s *Server) handleGetNetworkOrderByPurchaseOrder(w http.ResponseWriter, r *http.Request) {
	if name := firstQueryParameter(r); name != "" {
		writeProblem(w, http.StatusBadRequest, problemUnknownParam, "query parameter "+name+" is not declared by this route", r.URL.Path)
		return
	}
	poNumber := chi.URLParam(r, "poNumber")
	if !validPurchaseOrderNumber(poNumber) {
		writeProblem(w, http.StatusBadRequest, problemInvalidPurchaseOrderNumber, purchaseOrderNumberRule, r.URL.Path)
		return
	}

	ref, found, err := s.PurchaseOrders.FindNetworkRefByPONumber(r.Context(), poNumber)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if !found {
		writeError(w, r, usecases.ErrPurchaseOrderNotFound)
		return
	}
	// The map knows the number, but the order may not be stored yet (the
	// adapter records the map row when it polls, the use case then saves the
	// order): that is this route's not-found too, not the sibling's.
	s.writeNetworkOrder(w, r, ref, usecases.ErrPurchaseOrderNotFound)
}
