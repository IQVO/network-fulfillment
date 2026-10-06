package networkorder

import (
	"errors"
	"testing"
)

func TestParseState(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    State
		wantErr error
	}{
		{"new", "NEW", StateNew, nil},
		{"submitted", "SUBMITTED", StateSubmitted, nil},
		{"acknowledged", "ACKNOWLEDGED", StateAcknowledged, nil},
		{"rejected", "REJECTED", StateRejected, nil},
		{"confirmed", "CONFIRMED", StateConfirmed, nil},
		{"empty", "", "", ErrUnknownState},
		{"unknown", "SHIPPED", "", ErrUnknownState},
		{"lower case is not folded", "new", "", ErrUnknownState},
		{"padded is not trimmed", " NEW", "", ErrUnknownState},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseState(tt.in)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ParseState(%q) err = %v, want %v", tt.in, err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("ParseState(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
