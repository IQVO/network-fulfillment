package inventoryclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

func TestUsableQuantity_ParsesResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/inventory/sku-1/usable" {
			t.Errorf("path = %q, want /inventory/sku-1/usable", r.URL.Path)
		}
		if r.Method != http.MethodGet {
			t.Errorf("method = %q, want GET", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"sku": "sku-1", "usable": 37})
	}))
	defer srv.Close()

	c := NewClient(srv.URL, nil)
	got, err := c.UsableQuantity(context.Background(), shared.SKU("sku-1"))
	if err != nil {
		t.Fatalf("UsableQuantity: %v", err)
	}
	if got != 37 {
		t.Fatalf("got %d, want 37", got)
	}
}

func TestUsableQuantity_EscapesSKUInPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/inventory/sku%2Fwith%2Fslash/usable" && r.URL.EscapedPath() != "/inventory/sku%2Fwith%2Fslash/usable" {
			t.Errorf("path = %q", r.URL.EscapedPath())
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"usable": 0})
	}))
	defer srv.Close()
	c := NewClient(srv.URL, nil)
	if _, err := c.UsableQuantity(context.Background(), shared.SKU("sku/with/slash")); err != nil {
		t.Fatalf("UsableQuantity: %v", err)
	}
}

func TestUsableQuantity_ErrorStatusReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, nil)
	if _, err := c.UsableQuantity(context.Background(), shared.SKU("sku-1")); err == nil {
		t.Fatal("expected an error for a 500 response")
	}
}

func TestUsableQuantity_MalformedBodyReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("not json"))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, nil)
	if _, err := c.UsableQuantity(context.Background(), shared.SKU("sku-1")); err == nil {
		t.Fatal("expected an error for a malformed response body")
	}
}

func TestNewClient_DefaultsHTTPClient(t *testing.T) {
	c := NewClient("http://example.invalid", nil)
	if c.HTTP == nil {
		t.Fatal("expected a default *http.Client to be set")
	}
}
