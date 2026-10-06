package capabilityoffer

import (
	"errors"
	"testing"
)

func TestParseBasis(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    Basis
		wantErr error
	}{
		{"physical", "PHYSICAL", BasisPhysical, nil},
		{"throughput constrained", "THROUGHPUT_CONSTRAINED", BasisThroughputConstrained, nil},
		{"empty", "", "", ErrUnknownBasis},
		{"unknown", "BOGUS", "", ErrUnknownBasis},
		{"lower case is not folded", "physical", "", ErrUnknownBasis},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseBasis(tt.in)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ParseBasis(%q) err = %v, want %v", tt.in, err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("ParseBasis(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
