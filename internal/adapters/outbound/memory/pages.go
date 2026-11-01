package memory

import (
	"context"
	"sort"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/capabilityoffer"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
)

// ListUnansweredPage is one keyset page of ListUnanswered, ordered by
// (acknowledgeBy, networkRef) exactly like the Postgres repository.
func (r *NetworkOrderRepo) ListUnansweredPage(ctx context.Context, after *contract.Keyset, limit int) ([]*networkorder.NetworkOrder, error) {
	all, err := r.ListUnanswered(ctx) // already sorted by (acknowledgeBy, networkRef)
	if err != nil {
		return nil, err
	}
	var at time.Time
	if after != nil {
		if at, err = time.Parse(time.RFC3339Nano, after.Sort); err != nil {
			return nil, err
		}
	}
	out := make([]*networkorder.NetworkOrder, 0, limit)
	for _, o := range all {
		if after != nil && !orderAfter(o, at, after.ID) {
			continue
		}
		if len(out) == limit {
			break
		}
		out = append(out, o)
	}
	return out, nil
}

func orderAfter(o *networkorder.NetworkOrder, at time.Time, ref string) bool {
	if !o.AcknowledgeBy().Equal(at) {
		return o.AcknowledgeBy().After(at)
	}
	return string(o.NetworkRef()) > ref
}

// ListPage is one keyset page of the offers, ordered by (sku, siteId)
// exactly like the Postgres repository.
func (r *CapabilityOfferRepo) ListPage(ctx context.Context, after *contract.Keyset, limit int) ([]capabilityoffer.CapabilityOffer, error) {
	all, err := r.ListAll(ctx)
	if err != nil {
		return nil, err
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].SKU() != all[j].SKU() {
			return all[i].SKU() < all[j].SKU()
		}
		return all[i].SiteId() < all[j].SiteId()
	})
	out := make([]capabilityoffer.CapabilityOffer, 0, limit)
	for _, o := range all {
		sku, site := string(o.SKU()), string(o.SiteId())
		if after != nil && (sku < after.Sort || (sku == after.Sort && site <= after.ID)) {
			continue
		}
		if len(out) == limit {
			break
		}
		out = append(out, o)
	}
	return out, nil
}
