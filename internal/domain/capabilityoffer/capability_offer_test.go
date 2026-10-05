package capabilityoffer

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

var now = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

func TestNew_RejectsEmptySKU(t *testing.T) {
	if _, err := New("", "site-1", 1, 1, BasisPhysical, now); !errors.Is(err, ErrEmptySKU) {
		t.Fatalf("err = %v, want ErrEmptySKU", err)
	}
}

func TestNew_RejectsEmptySiteId(t *testing.T) {
	if _, err := New("sku-1", "", 1, 1, BasisPhysical, now); !errors.Is(err, ErrEmptySiteId) {
		t.Fatalf("err = %v, want ErrEmptySiteId", err)
	}
}

func TestNew_RejectsNegativeQuantity(t *testing.T) {
	if _, err := New("sku-1", "site-1", -1, 10, BasisPhysical, now); !errors.Is(err, ErrNegativeQuantity) {
		t.Fatalf("err = %v, want ErrNegativeQuantity", err)
	}
}

func TestNew_RejectsAdvertisedExceedingPhysical(t *testing.T) {
	if _, err := New("sku-1", "site-1", 11, 10, BasisPhysical, now); !errors.Is(err, ErrAdvertisedExceedsPhysical) {
		t.Fatalf("err = %v, want ErrAdvertisedExceedsPhysical", err)
	}
}

func TestNew_RejectsUnknownBasis(t *testing.T) {
	if _, err := New("sku-1", "site-1", 5, 10, Basis("BOGUS"), now); !errors.Is(err, ErrUnknownBasis) {
		t.Fatalf("err = %v, want ErrUnknownBasis", err)
	}
}

func TestNew_ZeroQuantityIsValid(t *testing.T) {
	o, err := New("sku-1", "site-1", 0, 0, BasisPhysical, now)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if o.AdvertisedQuantity() != 0 {
		t.Fatalf("AdvertisedQuantity = %d, want 0", o.AdvertisedQuantity())
	}
}

func TestNew_AccessorsRoundTrip(t *testing.T) {
	o, err := New("sku-1", shared.SiteId("site-1"), 5, 10, BasisThroughputConstrained, now)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if o.SKU() != "sku-1" {
		t.Errorf("SKU() = %q, want sku-1", o.SKU())
	}
	if o.SiteId() != "site-1" {
		t.Errorf("SiteId() = %q, want site-1", o.SiteId())
	}
	if o.AdvertisedQuantity() != 5 {
		t.Errorf("AdvertisedQuantity() = %d, want 5", o.AdvertisedQuantity())
	}
	if o.Basis() != BasisThroughputConstrained {
		t.Errorf("Basis() = %q, want THROUGHPUT_CONSTRAINED", o.Basis())
	}
	if !o.ComputedAt().Equal(now) {
		t.Errorf("ComputedAt() = %v, want %v", o.ComputedAt(), now)
	}
}

func TestCompute_FallsBackToPhysicalWhenThroughputUnknown(t *testing.T) {
	o, err := Compute("sku-1", "site-1", 100, 0, false, now)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	if o.AdvertisedQuantity() != 100 {
		t.Errorf("AdvertisedQuantity = %d, want 100 (physical fallback)", o.AdvertisedQuantity())
	}
	if o.Basis() != BasisPhysical {
		t.Errorf("Basis = %q, want PHYSICAL", o.Basis())
	}
}

func TestCompute_FallsBackToPhysicalWhenThroughputExceedsIt(t *testing.T) {
	o, err := Compute("sku-1", "site-1", 50, 999, true, now)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	if o.AdvertisedQuantity() != 50 {
		t.Errorf("AdvertisedQuantity = %d, want 50 (physical is the binding constraint)", o.AdvertisedQuantity())
	}
	if o.Basis() != BasisPhysical {
		t.Errorf("Basis = %q, want PHYSICAL", o.Basis())
	}
}

func TestCompute_ThrottlesWhenThroughputIsBelowPhysical(t *testing.T) {
	o, err := Compute("sku-1", "site-1", 400, 120, true, now)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	if o.AdvertisedQuantity() != 120 {
		t.Errorf("AdvertisedQuantity = %d, want 120 (throughput constrained)", o.AdvertisedQuantity())
	}
	if o.Basis() != BasisThroughputConstrained {
		t.Errorf("Basis = %q, want THROUGHPUT_CONSTRAINED", o.Basis())
	}
}

func TestCompute_NegativeThroughputFeasibleClampsToZero(t *testing.T) {
	o, err := Compute("sku-1", "site-1", 400, -5, true, now)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	if o.AdvertisedQuantity() != 0 {
		t.Errorf("AdvertisedQuantity = %d, want 0", o.AdvertisedQuantity())
	}
	if o.Basis() != BasisThroughputConstrained {
		t.Errorf("Basis = %q, want THROUGHPUT_CONSTRAINED", o.Basis())
	}
}

func TestCompute_EqualThroughputAndPhysicalIsPhysicalBasis(t *testing.T) {
	// throughputFeasible >= physicalAvailable falls back to physical per
	// Compute's own doc comment -- an exact tie is not "constrained".
	o, err := Compute("sku-1", "site-1", 100, 100, true, now)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	if o.Basis() != BasisPhysical {
		t.Errorf("Basis = %q, want PHYSICAL on an exact tie", o.Basis())
	}
	if o.AdvertisedQuantity() != 100 {
		t.Errorf("AdvertisedQuantity = %d, want 100", o.AdvertisedQuantity())
	}
}
