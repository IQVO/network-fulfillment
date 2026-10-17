package usecases

import (
	"context"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
)

// freeHold cancels the order-management hold recorded on o, when it is still
// open, and records the outcome on the aggregate (ADR 0024 §2). The caller
// saves the aggregate together with the rejection it is making, in one atomic
// scope, so the rejection and the settled hold can never diverge.
//
// An order with no recorded hold (untranslatable demand, or a hold the sweep
// already settled) has nothing to cancel: the call is a no-op. A failed cancel
// is returned and nothing is recorded, so the caller retries it.
func freeHold(ctx context.Context, planner ports.FulfillmentPlanner, o *networkorder.NetworkOrder, at time.Time) error {
	local := o.LocalOrderId()
	if local == nil || !o.HoldOpen() {
		return nil
	}
	if err := planner.CancelHeldOrder(ctx, *local); err != nil {
		return err
	}
	return o.SettleHold(networkorder.HoldCancelled, at)
}
