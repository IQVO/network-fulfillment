package main

import (
	"errors"
	"strings"
	"testing"
)

// TestRequireDatabaseURL pins the real-wiring rule for the A2A binary: it has
// no in-memory fallback, so a missing or blank DATABASE_URL is a boot error
// that names the variable.
func TestRequireDatabaseURL(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    string
		wantErr bool
	}{
		{name: "unset", raw: "", wantErr: true},
		{name: "blank", raw: "   ", wantErr: true},
		{name: "set", raw: "postgres://u:p@h:5432/db", want: "postgres://u:p@h:5432/db"},
		{name: "trimmed", raw: "  postgres://u:p@h:5432/db \n", want: "postgres://u:p@h:5432/db"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := requireDatabaseURL(tc.raw)
			if tc.wantErr {
				if !errors.Is(err, errMissingDatabaseURL) {
					t.Fatalf("err = %v, want errMissingDatabaseURL", err)
				}
				if !strings.Contains(err.Error(), "DATABASE_URL") {
					t.Fatalf("error %q must name DATABASE_URL", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}
