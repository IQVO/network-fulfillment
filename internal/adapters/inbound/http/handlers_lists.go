package http

import (
	"net/http"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/inbound/http/page"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
)

// Collection GETs are keyset-paginated (fleet REST guideline section 3):
// `limit` + opaque `cursor`, body {items, nextCursor}, 400 invalid-limit /
// invalid-cursor / unknown-query-parameter.

var (
	problemInvalidLimit  = problemInfo{"invalid-limit", "Invalid limit"}
	problemInvalidCursor = problemInfo{"invalid-cursor", "Invalid cursor"}
	problemUnknownParam  = problemInfo{"unknown-query-parameter", "Unknown query parameter"}
)

// pageParams reads limit and cursor, answering the 400 itself when either
// (or an undeclared query parameter) is invalid.
func pageParams(w http.ResponseWriter, r *http.Request, filters ...string) (int, *contract.Keyset, bool) {
	if name := page.Unknown(r, filters...); name != "" {
		writeProblem(w, http.StatusBadRequest, problemUnknownParam, "query parameter "+name+" is not declared by this collection", r.URL.Path)
		return 0, nil, false
	}
	limit, err := page.Limit(r)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, problemInvalidLimit, err.Error(), r.URL.Path)
		return 0, nil, false
	}
	pos, err := page.After(r)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, problemInvalidCursor, err.Error(), r.URL.Path)
		return 0, nil, false
	}
	if pos == nil {
		return limit, nil, true
	}
	return limit, &contract.Keyset{Sort: pos.Sort, ID: pos.ID}, true
}

// handleListUnanswered lists orders still awaiting an answer, one keyset
// page at a time, ordered by (acknowledgeBy, networkRef). Unanswered is the
// only list worth exposing: it is the working set the sweep acts on and the
// only one whose size is an operational signal.
func (s *Server) handleListUnanswered(w http.ResponseWriter, r *http.Request) {
	limit, after, ok := pageParams(w, r)
	if !ok {
		return
	}
	if after != nil {
		if _, err := time.Parse(time.RFC3339Nano, after.Sort); err != nil {
			writeProblem(w, http.StatusBadRequest, problemInvalidCursor, page.ErrInvalidCursor.Error(), r.URL.Path)
			return
		}
	}
	orders, err := s.Orders.ListUnansweredPage(r.Context(), after, page.FetchSize(limit))
	if err != nil {
		writeError(w, r, err)
		return
	}
	got := page.Of(orders, limit, func(o *networkorder.NetworkOrder) page.Position {
		return page.Position{Sort: o.AcknowledgeBy().UTC().Format(time.RFC3339Nano), ID: string(o.NetworkRef())}
	})
	now := s.Clock.Now()
	out := page.Page[networkOrderResponse]{Items: make([]networkOrderResponse, 0, len(got.Items)), NextCursor: got.NextCursor}
	for _, o := range got.Items {
		out.Items = append(out.Items, toNetworkOrderResponse(o, now))
	}
	writeJSON(w, http.StatusOK, out)
}

// handleListCapabilityOffers lists the currently-advertised CapabilityOffers
// (ADR 0001 §8), one keyset page at a time, ordered by (sku, siteId).
func (s *Server) handleListCapabilityOffers(w http.ResponseWriter, r *http.Request) {
	limit, after, ok := pageParams(w, r)
	if !ok {
		return
	}
	offers, err := s.Offers.ListPage(r.Context(), after, page.FetchSize(limit))
	if err != nil {
		writeError(w, r, err)
		return
	}
	got := page.Of(offers, limit, func(o capabilityoffer.CapabilityOffer) page.Position {
		return page.Position{Sort: string(o.SKU()), ID: string(o.SiteId())}
	})
	out := page.Page[capabilityOfferResponse]{Items: make([]capabilityOfferResponse, 0, len(got.Items)), NextCursor: got.NextCursor}
	for _, o := range got.Items {
		out.Items = append(out.Items, toCapabilityOfferResponse(o))
	}
	writeJSON(w, http.StatusOK, out)
}
