package http

import (
	"strings"
	"testing"
)

// validPurchaseOrderNumber is a shape, not a format (ADR 0036): 1 to 128
// characters of the RFC 3986 unreserved set. Every byte is checked, so each
// edge of each range (the characters either side of 0-9, A-Z and a-z) is
// pinned, and so is the length bound on both sides.
func TestValidPurchaseOrderNumber_EveryByte(t *testing.T) {
	for b := 0; b < 256; b++ {
		want := b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' ||
			b == '-' || b == '.' || b == '_' || b == '~'
		for _, s := range []string{string([]byte{byte(b)}), "PO" + string([]byte{byte(b)}) + "1"} {
			if got := validPurchaseOrderNumber(s); got != want {
				t.Errorf("validPurchaseOrderNumber(%q) = %v, want %v", s, got, want)
			}
		}
	}
}

func TestValidPurchaseOrderNumber_Length(t *testing.T) {
	for n, want := range map[int]bool{0: false, 1: true, 2: true, 127: true, 128: true, 129: false, 1000: false} {
		if got := validPurchaseOrderNumber(strings.Repeat("a", n)); got != want {
			t.Errorf("a number of %d characters: valid = %v, want %v", n, got, want)
		}
	}
}

func TestValidPurchaseOrderNumber_AssumesNoFormat(t *testing.T) {
	for _, s := range []string{"PO-7f3c9a1e5b2d4086", "po-4711", "7f3c9a1e5b2d4086", "PO_1.2~3", "X"} {
		if !validPurchaseOrderNumber(s) {
			t.Errorf("validPurchaseOrderNumber(%q) = false: no shape of the number may be assumed", s)
		}
	}
}
