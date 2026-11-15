package main_test

import (
	"context"
	"fmt"

	"github.com/cucumber/godog"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// Steps of features/purchase_order_lookup.feature (ADR 0036). The purchase
// order -> network ref map is written by the live retail-network adapter in
// production; here a Given records it, as the adapter does when it first sees a
// purchase order.

func (w *world) purchaseOrderWasFiledUnder(poNumber, ref string) error {
	w.poIndex.Register(poNumber, shared.NetworkRef(ref))
	return nil
}

func (w *world) iRequestPurchaseOrder(ctx context.Context, poNumber string) error {
	return w.iGET(ctx, "/purchase-orders/"+poNumber)
}

// theSameOrderIsReadByNetworkRef proves the two routes answer one
// representation: the body just recorded must equal, byte for byte, what
// GET /network-orders/{networkRef} answers for the ref. The recorded response
// is restored, so later steps still assert on the purchase-order read.
func (w *world) theSameOrderIsReadByNetworkRef(ctx context.Context, ref string) error {
	status, body, headers := w.status, w.body, w.headers
	defer func() { w.status, w.body, w.headers = status, body, headers }()

	if err := w.iRequestNetworkOrder(ctx, ref); err != nil {
		return err
	}
	if w.status != 200 {
		return fmt.Errorf("GET /network-orders/%s = %d, want 200", ref, w.status)
	}
	if string(w.body) != string(body) {
		return fmt.Errorf("by purchase order: %s\nby network ref: %s", body, w.body)
	}
	return nil
}

func (w *world) registerPurchaseOrderSteps(sc *godog.ScenarioContext) {
	sc.Step(`^purchase order \"([^\"]*)\" was filed under network order \"([^\"]*)\"$`, w.purchaseOrderWasFiledUnder)
	sc.Step(`^I request purchase order \"([^\"]*)\"$`, w.iRequestPurchaseOrder)
	sc.Step(`^the same order is read by network ref \"([^\"]*)\"$`, w.theSameOrderIsReadByNetworkRef)
}
