package productmaster

// In-package copy of the httptest client factory from client_test.go
// (package productmaster_test): the spec contract tests reflect this
// package's unexported gtinDTO, so they must be in-package.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/fleetrest"
)

func contractClientFor(t *testing.T, h http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	rest, err := fleetrest.New(fleetrest.Config{Dependency: "product-master", BaseURL: srv.URL, Sleep: func(context.Context, time.Duration) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	return New(rest)
}
