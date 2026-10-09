package main_test

import (
	"context"
	"fmt"
	"time"

	"github.com/cucumber/godog"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

type offerBody struct {
	SKU                string `json:"sku"`
	SiteID             string `json:"siteId"`
	AdvertisedQuantity int    `json:"advertisedQuantity"`
	Basis              string `json:"basis"`
	ComputedAt         string `json:"computedAt"`
}

func (w *world) offerList() ([]offerBody, error) {
	var list struct {
		CapabilityOffers []offerBody `json:"capabilityOffers"`
	}
	if err := w.decode(&list); err != nil {
		return nil, err
	}
	return list.CapabilityOffers, nil
}

// ---- Given: the inputs to ADR 0001 §8's formula -------------------------------

func (w *world) capabilityOffersAreDisabled() error {
	w.offersEnabled = false
	return nil
}

func (w *world) inventoryReportsUsable(qty int, sku string) error {
	w.inventory.usable[shared.SKU(sku)] = qty
	return nil
}

func (w *world) inventoryIsUnreachableFor(sku string) error {
	w.inventory.failing[shared.SKU(sku)] = true
	return nil
}

func (w *world) nextCutoffIn(minutes int) error {
	w.pathCap.scheduled = true
	w.pathCap.cutoffIn = time.Duration(minutes) * time.Minute
	return nil
}

func (w *world) pathHasCycleTime(pathID string, minutes int) error {
	w.pathCap.paths = append(w.pathCap.paths, contract.EligiblePath{
		PathId: pathID, CycleTimeP95: time.Duration(minutes) * time.Minute, CycleTimeKnown: true,
	})
	return nil
}

func (w *world) pathHasUnknownCycleTime(pathID string) error {
	w.pathCap.paths = append(w.pathCap.paths, contract.EligiblePath{PathId: pathID})
	return nil
}

func (w *world) pathHasRemainingCapacity(pathID string, units int) error {
	w.capacity.units[pathID] = units
	return nil
}

// ---- When ----------------------------------------------------------------------

func (w *world) theCapabilityOffersAreRecomputed(ctx context.Context) error {
	_, err := w.recompute.Execute(ctx)
	return err
}

func (w *world) iListTheCapabilityOffers(ctx context.Context) error {
	return w.iGET(ctx, "/capability-offers")
}

// ---- Then ----------------------------------------------------------------------

func (w *world) theCapabilityOfferAdvertises(sku, site string, qty int, basis string) error {
	offers, err := w.offerList()
	if err != nil {
		return err
	}
	for _, o := range offers {
		if o.SKU == sku && o.SiteID == site {
			if o.AdvertisedQuantity != qty || o.Basis != basis {
				return fmt.Errorf("expected %s at %s to advertise %d (%s), got %d (%s)", sku, site, qty, basis, o.AdvertisedQuantity, o.Basis)
			}
			return nil
		}
	}
	return fmt.Errorf("no capability offer for %s at %s in %+v", sku, site, offers)
}

func (w *world) theCapabilityOffersListHas(expected int) error {
	offers, err := w.offerList()
	if err != nil {
		return err
	}
	if len(offers) != expected {
		return fmt.Errorf("expected %d capability offer(s), got %d: %+v", expected, len(offers), offers)
	}
	return nil
}

func (w *world) noOfferExceedsPhysicalStock() error {
	offers, err := w.offerList()
	if err != nil {
		return err
	}
	for _, o := range offers {
		if physical := w.inventory.usable[shared.SKU(o.SKU)]; o.AdvertisedQuantity > physical {
			return fmt.Errorf("offer for %s advertises %d, more than the %d physically available", o.SKU, o.AdvertisedQuantity, physical)
		}
	}
	return nil
}

func (w *world) everyOfferWasComputedNow() error {
	offers, err := w.offerList()
	if err != nil {
		return err
	}
	expected := w.clock.Now().UTC().Format(time.RFC3339)
	for _, o := range offers {
		if o.ComputedAt != expected {
			return fmt.Errorf("expected offer for %s to be computed at %s, got %s", o.SKU, expected, o.ComputedAt)
		}
	}
	return nil
}

func (w *world) registerOfferSteps(sc *godog.ScenarioContext) {
	sc.Step(`^capability offers are not enabled$`, w.capabilityOffersAreDisabled)
	sc.Step(`^inventory-storage reports (\d+) usable units? of "([^"]*)"$`, w.inventoryReportsUsable)
	sc.Step(`^inventory-storage is unreachable for "([^"]*)"$`, w.inventoryIsUnreachableFor)
	sc.Step(`^process-path-management reports the next cutoff in (\d+) minutes$`, w.nextCutoffIn)
	sc.Step(`^path "([^"]*)" is eligible with a cycle time of (\d+) minutes$`, w.pathHasCycleTime)
	sc.Step(`^path "([^"]*)" is eligible with an unknown cycle time$`, w.pathHasUnknownCycleTime)
	sc.Step(`^path "([^"]*)" has (\d+) units of remaining capacity$`, w.pathHasRemainingCapacity)

	sc.Step(`^the capability offers are recomputed$`, w.theCapabilityOffersAreRecomputed)
	sc.Step(`^I list the capability offers$`, w.iListTheCapabilityOffers)

	sc.Step(`^the capability offer for "([^"]*)" at "([^"]*)" advertises (\d+) with basis "([^"]*)"$`, w.theCapabilityOfferAdvertises)
	sc.Step(`^the capability offers list has (\d+) offers?$`, w.theCapabilityOffersListHas)
	sc.Step(`^no capability offer advertises more than the physical stock$`, w.noOfferExceedsPhysicalStock)
	sc.Step(`^every capability offer was computed at the current time$`, w.everyOfferWasComputedNow)
}
