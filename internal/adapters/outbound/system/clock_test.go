package system

import (
	"testing"
	"time"
)

func TestClockNowIsUTCAndCurrent(t *testing.T) {
	before := time.Now()
	got := Clock{}.Now()
	after := time.Now()
	if got.Location() != time.UTC {
		t.Fatalf("location = %v, want UTC", got.Location())
	}
	if got.Before(before) || got.After(after) {
		t.Fatalf("Now() = %v, want within [%v, %v]", got, before, after)
	}
}
