package networkorder

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

func ackByLines(t *testing.T) []Line {
	t.Helper()
	l, err := NewLine("1", "listing-1", "sku-1", 2)
	if err != nil {
		t.Fatalf("NewLine: %v", err)
	}
	return []Line{l}
}

func TestReceiveBy_UsesTheNetworksDeadlineWhenItGaveOne(t *testing.T) {
	received := time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)
	networkDeadline := received.Add(3 * time.Hour)

	o, err := ReceiveBy("ref-1", "site-1", received.Add(48*time.Hour), ackByLines(t), received, networkDeadline)
	if err != nil {
		t.Fatalf("ReceiveBy: %v", err)
	}
	if !o.AcknowledgeBy().Equal(networkDeadline) {
		t.Fatalf("acknowledgeBy = %v, want the network's %v", o.AcknowledgeBy(), networkDeadline)
	}
	// The window is the network's: a sweep ten minutes after it is overdue,
	// five minutes before it is not.
	if !o.AcknowledgementOverdue(networkDeadline.Add(10 * time.Minute)) {
		t.Fatal("an order past the network's deadline must be overdue")
	}
	if o.AcknowledgementOverdue(networkDeadline.Add(-5 * time.Minute)) {
		t.Fatal("an order inside the network's deadline must not be overdue")
	}
}

func TestReceiveBy_AZeroDeadlineKeepsTheLocalWindow(t *testing.T) {
	received := time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)

	o, err := ReceiveBy("ref-1", "site-1", received.Add(48*time.Hour), ackByLines(t), received, time.Time{})
	if err != nil {
		t.Fatalf("ReceiveBy: %v", err)
	}
	if want := received.Add(AcknowledgementWindow); !o.AcknowledgeBy().Equal(want) {
		t.Fatalf("acknowledgeBy = %v, want %v", o.AcknowledgeBy(), want)
	}
}

func TestReceiveBy_KeepsTheSameGuardsAsReceive(t *testing.T) {
	received := time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)
	if _, err := ReceiveBy("", "site-1", received, ackByLines(t), received, received.Add(time.Hour)); !errors.Is(err, shared.ErrEmptyNetworkRef) {
		t.Fatalf("empty ref err = %v, want ErrEmptyNetworkRef", err)
	}
	if _, err := ReceiveBy("ref-1", "site-1", received, nil, received, received.Add(time.Hour)); !errors.Is(err, shared.ErrNoLines) {
		t.Fatalf("no lines err = %v, want ErrNoLines", err)
	}
}

func TestReceiveUntranslatableBy_DeadlineAndGuard(t *testing.T) {
	received := time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)

	o, err := ReceiveUntranslatableBy("ref-1", "site-1", received.Add(time.Hour), received, received.Add(2*time.Hour))
	if err != nil {
		t.Fatalf("ReceiveUntranslatableBy: %v", err)
	}
	if want := received.Add(2 * time.Hour); !o.AcknowledgeBy().Equal(want) {
		t.Fatalf("acknowledgeBy = %v, want %v", o.AcknowledgeBy(), want)
	}
	o, err = ReceiveUntranslatableBy("ref-1", "site-1", received.Add(time.Hour), received, time.Time{})
	if err != nil {
		t.Fatalf("ReceiveUntranslatableBy (no deadline): %v", err)
	}
	if want := received.Add(AcknowledgementWindow); !o.AcknowledgeBy().Equal(want) {
		t.Fatalf("acknowledgeBy = %v, want the local window %v", o.AcknowledgeBy(), want)
	}
	if _, err := ReceiveUntranslatableBy("", "site-1", received, received, time.Time{}); !errors.Is(err, shared.ErrEmptyNetworkRef) {
		t.Fatalf("empty ref err = %v, want ErrEmptyNetworkRef", err)
	}
}

func TestCheckConfirmable_AgreesWithConfirmShipmentAndChangesNothing(t *testing.T) {
	received := time.Date(2026, 10, 24, 9, 0, 0, 0, time.UTC)
	o, err := Receive("ref-1", "site-1", received.Add(time.Hour), ackByLines(t), received)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if err := o.CheckConfirmable(); !errors.Is(err, ErrConfirmBeforeAcknowledge) {
		t.Fatalf("NEW: CheckConfirmable = %v, want ErrConfirmBeforeAcknowledge", err)
	}
	if err := o.Acknowledge(); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	if err := o.CheckConfirmable(); err != nil {
		t.Fatalf("ACKNOWLEDGED: CheckConfirmable = %v, want nil", err)
	}
	if o.State() != StateAcknowledged {
		t.Fatalf("CheckConfirmable changed the state to %v", o.State())
	}
	if err := o.ConfirmShipment(); err != nil {
		t.Fatalf("ConfirmShipment: %v", err)
	}
	if err := o.CheckConfirmable(); !errors.Is(err, ErrConfirmBeforeAcknowledge) {
		t.Fatalf("CONFIRMED: CheckConfirmable = %v, want ErrConfirmBeforeAcknowledge", err)
	}
}
