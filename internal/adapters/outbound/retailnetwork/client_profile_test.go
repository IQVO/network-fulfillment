package retailnetwork

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

// ---- the node profile's two client operations (ADR 0027) ----

func TestClient_PutAsyncIsAPutWithTheKeyTheBodyAndTheNodeInThePath(t *testing.T) {
	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		ok(w, map[string]string{"transactionId": "tx-9"})
	})
	c, _, _ := testClient(t, f.srv.URL, nil)

	tx, err := c.putAsync(context.Background(), "declareNodeProfile", []string{"vendor", "nodes", "N-1", "profile"}, "nf-profile-FC-1-4", []byte(`{"profileRevision":4}`))
	if err != nil || tx != "tx-9" {
		t.Fatalf("putAsync = %q, %v", tx, err)
	}
	got := f.requests()
	if len(got) != 1 {
		t.Fatalf("requests = %d, want 1", len(got))
	}
	r := got[0]
	if r.Method != http.MethodPut || r.IdemKey != "nf-profile-FC-1-4" || r.ContentType != "application/json" || r.Body != `{"profileRevision":4}` {
		t.Errorf("write = %+v, want PUT with the key, JSON content type and the body untouched", r)
	}
	if r.Path != "/vendor/nodes/N-1/profile" {
		t.Errorf("path = %q, want /vendor/nodes/N-1/profile", r.Path)
	}
}

func TestClient_PutAsyncWithoutATransactionIdIsAnError(t *testing.T) {
	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		ok(w, map[string]string{})
	})
	c, _, _ := testClient(t, f.srv.URL, nil)
	if _, err := c.putAsync(context.Background(), "declareNodeProfile", []string{"vendor", "nodes", "N-1", "profile"}, "k", []byte("{}")); err == nil {
		t.Fatal("a 202 without a transactionId must be an error")
	}
}

func TestClient_GetNodeReadsTheNodeWithItsProfileRevision(t *testing.T) {
	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) {
		ok(w, map[string]any{"nodeId": "N-1", "vendorId": "V-WS", "vendorNodeCode": "FC-1", "nodeType": "FULFILLMENT_CENTER", "status": "ACTIVE", "profileRevision": 7})
	})
	c, _, _ := testClient(t, f.srv.URL, nil)

	node, err := c.getNode(context.Background(), "N-1")
	if err != nil || node.NodeID != "N-1" || node.ProfileRevision != 7 {
		t.Fatalf("getNode = %+v, %v, want N-1 at profile revision 7", node, err)
	}
	r := f.requests()[0]
	if r.Method != http.MethodGet || r.Path != "/vendor/nodes/N-1" || !strings.Contains(r.RawQuery, "vendorId=V-WS") {
		t.Errorf("request = %+v, want GET /vendor/nodes/N-1?vendorId=V-WS", r)
	}
}

func TestClient_GetNodeBeforeTheFirstProfileHasNoRevision(t *testing.T) {
	f := newFake(t, func(n int, w http.ResponseWriter, r *http.Request) {
		ok(w, map[string]any{"nodeId": "N-1", "vendorId": "V-WS", "vendorNodeCode": "FC-1", "nodeType": "FULFILLMENT_CENTER", "status": "PROPOSED"})
	})
	c, _, _ := testClient(t, f.srv.URL, nil)
	node, err := c.getNode(context.Background(), "N-1")
	if err != nil || node.ProfileRevision != 0 {
		t.Fatalf("getNode = %+v, %v, want revision 0 (absent)", node, err)
	}
}
