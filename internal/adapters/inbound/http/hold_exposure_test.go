package http_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
)

// ADR 0024 §1 records the hold on a NEW order, and a REJECTED order keeps
// the id of the hold it freed. The REST contract has not changed:
// apis/openapi.yaml says localOrderId is "absent until acknowledged", so it
// must stay absent for an order NF has not committed to, whatever NF records
// internally about the hold.
func TestGetNetworkOrder_DoesNotExposeAHoldForAnOrderNotCommittedTo(t *testing.T) {
	e := newTestEnv(t)

	pending := e.seed(t, "po-new", time.Hour)
	if err := pending.RecordHold("ord-new"); err != nil {
		t.Fatalf("RecordHold: %v", err)
	}
	rejected := e.seed(t, "po-rejected", time.Hour)
	if err := rejected.RecordHold("ord-rejected"); err != nil {
		t.Fatalf("RecordHold: %v", err)
	}
	if err := rejected.Reject(); err != nil {
		t.Fatalf("Reject: %v", err)
	}
	for _, o := range []*networkorder.NetworkOrder{pending, rejected} {
		if err := e.orders.Save(context.Background(), o); err != nil {
			t.Fatalf("Save %s: %v", o.NetworkRef(), err)
		}

		rec := e.do(t, http.MethodGet, "/network-orders/"+string(o.NetworkRef()))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status = %d, want 200", o.NetworkRef(), rec.Code)
		}
		var raw map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
			t.Fatalf("%s: decode: %v", o.NetworkRef(), err)
		}
		if _, exposed := raw["localOrderId"]; exposed {
			t.Fatalf("%s: localOrderId = %v exposed; the contract says absent until acknowledged", o.NetworkRef(), raw["localOrderId"])
		}
	}
}
