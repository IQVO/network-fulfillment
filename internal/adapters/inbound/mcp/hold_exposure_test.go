package mcp

import (
	"testing"
	"time"
)

// ADR 0024 §1 records the OM hold on a NEW order and a REJECTED order keeps the
// id of the hold it freed. The read surface (docs/docs/mcp/tools.md: localOrderId is
// "set from SUBMITTED on") must not change: the MCP tool exposes the id only for
// an order NF has committed to.
func TestGetNetworkOrder_DoesNotExposeAHoldForAnOrderNotCommittedTo(t *testing.T) {
	h := newHarness(t)

	pending := h.seedOrder("po-new", base.Add(-time.Hour))
	if err := pending.RecordHold("ord-new"); err != nil {
		t.Fatalf("RecordHold: %v", err)
	}
	rejected := h.seedOrder("po-rejected", base.Add(-time.Hour))
	if err := rejected.RecordHold("ord-rejected"); err != nil {
		t.Fatalf("RecordHold: %v", err)
	}
	if err := rejected.Reject(); err != nil {
		t.Fatalf("Reject: %v", err)
	}
	for _, ref := range []string{"po-new", "po-rejected"} {
		out, err := h.deps.getNetworkOrder(h.ctx(), getNetworkOrderInput{NetworkRef: ref})
		if err != nil {
			t.Fatalf("getNetworkOrder(%s): %v", ref, err)
		}
		if out.LocalOrderId != nil {
			t.Fatalf("%s: localOrderId = %q exposed, want absent until SUBMITTED", ref, *out.LocalOrderId)
		}
	}
}
