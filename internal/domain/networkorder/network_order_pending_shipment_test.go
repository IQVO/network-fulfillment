package networkorder

import (
	"errors"
	"testing"
	"time"
)

// orderIn builds a rehydrated order in state, the way a repository would.
func orderIn(t *testing.T, state State) *NetworkOrder {
	t.Helper()
	return Rehydrate("po-1", "site-1", testNow().Add(48*time.Hour), testNow().Add(AcknowledgementWindow), testNow(),
		[]Line{mustLine(t)}, state, nil)
}

func TestNoteShipmentBeforeAcknowledge_RecordsTheFactWhileSubmitted(t *testing.T) {
	o := orderIn(t, StateSubmitted)
	if _, ok := o.PendingShipment(); ok {
		t.Fatal("a fresh order must carry no pending shipment")
	}

	at := testNow().Add(2 * time.Hour)
	if err := o.NoteShipmentBeforeAcknowledge("load-1", 3, at); err != nil {
		t.Fatalf("NoteShipmentBeforeAcknowledge on SUBMITTED: %v", err)
	}
	got, ok := o.PendingShipment()
	if !ok {
		t.Fatal("the shipment was not recorded")
	}
	if got.ShipmentRef != "load-1" || got.PackageCount != 3 || !got.NotedAt.Equal(at) {
		t.Fatalf("pending = %+v, want {load-1 3 %v}", got, at)
	}
	if o.State() != StateSubmitted {
		t.Fatalf("state = %v: noting a shipment must not move the order", o.State())
	}
}

func TestNoteShipmentBeforeAcknowledge_RefusesEveryStateButSubmitted(t *testing.T) {
	for _, state := range []State{StateNew, StateAcknowledged, StateRejected, StateConfirmed} {
		o := orderIn(t, state)
		err := o.NoteShipmentBeforeAcknowledge("load-1", 1, testNow())
		if !errors.Is(err, ErrConfirmBeforeAcknowledge) {
			t.Errorf("%s: err = %v, want ErrConfirmBeforeAcknowledge unchanged", state, err)
		}
		if _, ok := o.PendingShipment(); ok {
			t.Errorf("%s: a refused note must record nothing", state)
		}
	}
}

func TestNoteShipmentBeforeAcknowledge_TheFirstShipmentWins(t *testing.T) {
	o := orderIn(t, StateSubmitted)
	first := testNow().Add(time.Hour)
	if err := o.NoteShipmentBeforeAcknowledge("load-1", 2, first); err != nil {
		t.Fatalf("first note: %v", err)
	}

	// The same ref again (a redelivery) is a no-op success and keeps the
	// first instant and count.
	if err := o.NoteShipmentBeforeAcknowledge("load-1", 9, testNow().Add(5*time.Hour)); err != nil {
		t.Fatalf("the same ref again must be a no-op success, got %v", err)
	}
	// A different ref while one is pending is a no-op success too: the
	// caller logs it, the order keeps the first.
	if err := o.NoteShipmentBeforeAcknowledge("load-2", 4, testNow().Add(6*time.Hour)); err != nil {
		t.Fatalf("a different ref while one is pending must be a no-op success, got %v", err)
	}

	got, _ := o.PendingShipment()
	if got.ShipmentRef != "load-1" || got.PackageCount != 2 || !got.NotedAt.Equal(first) {
		t.Fatalf("pending = %+v, want the first shipment {load-1 2 %v} untouched", got, first)
	}
}

func TestNoteShipmentBeforeAcknowledge_StoresANonPositiveCountAsZero(t *testing.T) {
	for _, count := range []int{0, -1, -50} {
		o := orderIn(t, StateSubmitted)
		if err := o.NoteShipmentBeforeAcknowledge("load-1", count, testNow()); err != nil {
			t.Fatalf("count %d: %v", count, err)
		}
		if got, _ := o.PendingShipment(); got.PackageCount != 0 {
			t.Errorf("count %d stored as %d, want 0 (the confirm path maps it to the placeholder)", count, got.PackageCount)
		}
	}
}

func TestNoteShipmentBeforeAcknowledge_RefusesAnEmptyShipmentRef(t *testing.T) {
	o := orderIn(t, StateSubmitted)
	if err := o.NoteShipmentBeforeAcknowledge("", 1, testNow()); !errors.Is(err, ErrEmptyShipmentRef) {
		t.Fatalf("err = %v, want ErrEmptyShipmentRef", err)
	}
	if _, ok := o.PendingShipment(); ok {
		t.Fatal("a refused note must record nothing")
	}
}

func TestConfirmShipment_ClearsThePendingShipment(t *testing.T) {
	o := orderIn(t, StateSubmitted)
	if err := o.NoteShipmentBeforeAcknowledge("load-1", 2, testNow()); err != nil {
		t.Fatalf("note: %v", err)
	}
	if err := o.ConfirmAcknowledgement(); err != nil {
		t.Fatalf("ConfirmAcknowledgement: %v", err)
	}
	if _, ok := o.PendingShipment(); !ok {
		t.Fatal("acknowledging must keep the pending shipment: the sweep confirms it")
	}
	if err := o.ConfirmShipment(); err != nil {
		t.Fatalf("ConfirmShipment: %v", err)
	}
	if _, ok := o.PendingShipment(); ok {
		t.Fatal("ConfirmShipment must clear the pending shipment: it has been used")
	}
}

func TestConfirmShipment_RefusedKeepsThePendingShipment(t *testing.T) {
	o := orderIn(t, StateSubmitted)
	if err := o.NoteShipmentBeforeAcknowledge("load-1", 2, testNow()); err != nil {
		t.Fatalf("note: %v", err)
	}
	if err := o.ConfirmShipment(); !errors.Is(err, ErrConfirmBeforeAcknowledge) {
		t.Fatalf("ConfirmShipment on SUBMITTED: err = %v, want ErrConfirmBeforeAcknowledge", err)
	}
	if _, ok := o.PendingShipment(); !ok {
		t.Fatal("a refused confirmation must not consume the pending shipment")
	}
}

func TestDropPendingShipment_ClearsWithoutConfirming(t *testing.T) {
	o := orderIn(t, StateSubmitted)
	if err := o.NoteShipmentBeforeAcknowledge("load-1", 2, testNow()); err != nil {
		t.Fatalf("note: %v", err)
	}
	o.DropPendingShipment()
	if _, ok := o.PendingShipment(); ok {
		t.Fatal("DropPendingShipment must clear the fact")
	}
	if o.State() != StateSubmitted {
		t.Fatalf("state = %v: dropping must not confirm or move the order", o.State())
	}
	o.DropPendingShipment() // nothing to drop: still fine
}

func TestRestorePendingShipment_RebuildsAPersistedFact(t *testing.T) {
	o := orderIn(t, StateAcknowledged)
	at := testNow().Add(3 * time.Hour)
	o.RestorePendingShipment(PendingShipment{ShipmentRef: "load-9", PackageCount: 5, NotedAt: at})

	got, ok := o.PendingShipment()
	if !ok || got.ShipmentRef != "load-9" || got.PackageCount != 5 || !got.NotedAt.Equal(at) {
		t.Fatalf("restored pending = %+v ok=%v, want {load-9 5 %v}", got, ok, at)
	}
	// The restored fact is the same one ConfirmShipment consumes.
	if err := o.ConfirmShipment(); err != nil {
		t.Fatalf("ConfirmShipment: %v", err)
	}
	if _, ok := o.PendingShipment(); ok {
		t.Fatal("a restored pending shipment must clear on ConfirmShipment")
	}
}

func TestPendingShipment_ReturnsACopy(t *testing.T) {
	o := orderIn(t, StateSubmitted)
	if err := o.NoteShipmentBeforeAcknowledge("load-1", 2, testNow()); err != nil {
		t.Fatalf("note: %v", err)
	}
	got, _ := o.PendingShipment()
	got.ShipmentRef = "tampered"
	if again, _ := o.PendingShipment(); again.ShipmentRef != "load-1" {
		t.Fatal("mutating the returned value must not reach the aggregate")
	}
}
