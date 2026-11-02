package networkorder

import (
	"errors"
	"testing"
	"time"
)

// TestVersionLifecycleOnFreshAndRehydratedAggregates pins the contract
// the repositories rely on (migration 0013): a fresh aggregate starts at
// 0, RestoreVersion sets whatever the row carried, and AdvanceVersion
// moves it by exactly one per successful conditional write.
func TestVersionLifecycleOnFreshAndRehydratedAggregates(t *testing.T) {
	fresh := Rehydrate("po-1", "site-1",
		time.Now().Add(48*time.Hour), time.Now().Add(time.Hour), time.Now(),
		nil, StateNew, nil)
	if got := fresh.Version(); got != 0 {
		t.Fatalf("fresh Version() = %d, want 0", got)
	}
	fresh.RestoreVersion(7)
	if got := fresh.Version(); got != 7 {
		t.Fatalf("after RestoreVersion(7) = %d, want 7", got)
	}
	fresh.AdvanceVersion()
	if got := fresh.Version(); got != 8 {
		t.Fatalf("after AdvanceVersion = %d, want 8", got)
	}
}

// TestErrVersionConflictIsAMatch proves the sentinel stays wrapped-matchable,
// which is what the retry helper and the REST 409 mapping branch on.
func TestErrVersionConflictIsAMatch(t *testing.T) {
	wrapped := &transientError{ErrVersionConflict}
	if !errors.Is(wrapped, ErrVersionConflict) {
		t.Fatal("wrapped ErrVersionConflict must satisfy errors.Is")
	}
}

type transientError struct{ error }

func (e *transientError) Unwrap() error { return e.error }
