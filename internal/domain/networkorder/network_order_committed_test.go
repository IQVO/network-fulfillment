package networkorder

import "testing"

// TestCommittedLocalOrderId pins which hold id the read surfaces (REST, MCP)
// may show: only the local order of an order NF has committed to. The id is
// recorded earlier (NEW) and kept later (REJECTED) for the hold sweep, but the
// published contract says it is absent until acknowledged (ADR 0024 §1).
func TestCommittedLocalOrderId(t *testing.T) {
	cases := []struct {
		state State
		id    string
		want  string // "" = hidden
	}{
		{StateNew, "ord-1", ""},
		{StateRejected, "ord-1", ""},
		{StateSubmitted, "ord-1", "ord-1"},
		{StateAcknowledged, "ord-1", "ord-1"},
		{StateConfirmed, "ord-1", "ord-1"},
		{StateSubmitted, "", ""},
		{StateNew, "", ""},
	}
	for _, tc := range cases {
		name := string(tc.state) + "/" + tc.id
		t.Run(name, func(t *testing.T) {
			got := holdOrder(t, tc.state, tc.id, "").CommittedLocalOrderId()
			switch {
			case tc.want == "" && got != nil:
				t.Fatalf("CommittedLocalOrderId = %q, want hidden", *got)
			case tc.want != "" && (got == nil || string(*got) != tc.want):
				t.Fatalf("CommittedLocalOrderId = %v, want %q", got, tc.want)
			}
		})
	}
}
