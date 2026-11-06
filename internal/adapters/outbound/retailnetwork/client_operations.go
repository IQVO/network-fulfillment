package retailnetwork

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

func (c *apiClient) vendorQuery() url.Values {
	return url.Values{"vendorId": {c.vendorID}}
}

// pollPage fetches one page of purchase orders. The filters are repeated on
// every page request; the cursor only says where the previous page stopped.
func (c *apiClient) pollPage(ctx context.Context, createdAfter time.Time, cursor string) (purchaseOrderPageDTO, error) {
	q := c.vendorQuery()
	q.Set("limit", strconv.Itoa(pollPageLimit))
	if !createdAfter.IsZero() {
		q.Set("createdAfter", createdAfter.UTC().Format(time.RFC3339Nano))
	}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	var page purchaseOrderPageDTO
	err := c.call(ctx, request{op: "pollPurchaseOrders", method: http.MethodGet, path: []string{segVendor, "purchase-orders"}, query: q}, &page)
	return page, err
}

func (c *apiClient) getPurchaseOrder(ctx context.Context, poNumber string) (purchaseOrderDTO, error) {
	var po purchaseOrderDTO
	err := c.call(ctx, request{op: "getPurchaseOrder", method: http.MethodGet, path: []string{segVendor, "purchase-orders", poNumber}, query: c.vendorQuery()}, &po)
	return po, err
}

func (c *apiClient) getTransaction(ctx context.Context, id string) (transactionDTO, error) {
	var tx transactionDTO
	err := c.call(ctx, request{op: "getTransaction", method: http.MethodGet, path: []string{segVendor, "transactions", id}, query: c.vendorQuery()}, &tx)
	return tx, err
}

// listNodes reads every page of the vendor's nodes.
func (c *apiClient) listNodes(ctx context.Context) ([]vendorNodeDTO, error) {
	return collectPages(ctx, "listVendorNodes", func(ctx context.Context, cursor string) (vendorNodeListDTO, error) {
		q := c.vendorQuery()
		q.Set("limit", strconv.Itoa(pollPageLimit))
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		var list vendorNodeListDTO
		err := c.call(ctx, request{op: "listVendorNodes", method: http.MethodGet, path: []string{segVendor, segNodes}, query: q}, &list)
		return list, err
	})
}

// postAsync sends an asynchronous write (202 {transactionId}) with its
// idempotency key. body is sent exactly as given.
func (c *apiClient) postAsync(ctx context.Context, op string, path []string, idemKey string, body []byte) (string, error) {
	return c.writeAsync(ctx, http.MethodPost, op, path, idemKey, body)
}

// putAsync is postAsync for the writes the Vendor API takes as a PUT (the node
// profile, ADR 0027).
func (c *apiClient) putAsync(ctx context.Context, op string, path []string, idemKey string, body []byte) (string, error) {
	return c.writeAsync(ctx, http.MethodPut, op, path, idemKey, body)
}

func (c *apiClient) writeAsync(ctx context.Context, method, op string, path []string, idemKey string, body []byte) (string, error) {
	var accepted transactionAcceptedDTO
	if err := c.call(ctx, request{op: op, method: method, path: path, idemKey: idemKey, body: body}, &accepted); err != nil {
		return "", err
	}
	if accepted.TransactionID == "" {
		return "", fmt.Errorf("retail-network: %s: the answer carries no transactionId", op)
	}
	return accepted.TransactionID, nil
}

// getNode reads one of the vendor's nodes, as the network knows it (its latest
// profile revision among other things).
func (c *apiClient) getNode(ctx context.Context, nodeID string) (vendorNodeDTO, error) {
	var node vendorNodeDTO
	err := c.call(ctx, request{op: "getNode", method: http.MethodGet, path: []string{segVendor, segNodes, nodeID}, query: c.vendorQuery()}, &node)
	return node, err
}

// requestLabels is the one synchronous write: 200 with the labels.
func (c *apiClient) requestLabels(ctx context.Context, idemKey string, body []byte) (labelResponseDTO, error) {
	var out labelResponseDTO
	err := c.call(ctx, request{op: "requestLabel", method: http.MethodPost, path: []string{segVendor, "shipping-labels"}, idemKey: idemKey, body: body}, &out)
	return out, err
}

// pollAll pages through every purchase order created after createdAfter.
func (c *apiClient) pollAll(ctx context.Context, createdAfter time.Time) ([]purchaseOrderDTO, error) {
	return collectPages(ctx, "pollPurchaseOrders", func(ctx context.Context, cursor string) (purchaseOrderPageDTO, error) {
		return c.pollPage(ctx, createdAfter, cursor)
	})
}

// collectPages follows nextCursor until it is absent. A page that repeats a
// cursor, or more pages than any real backlog, is an error rather than a loop.
func collectPages[T any](ctx context.Context, op string, fetch func(context.Context, string) (pageDTO[T], error)) ([]T, error) {
	var all []T
	seen := map[string]bool{}
	cursor := ""
	for range maxPollPages {
		page, err := fetch(ctx, cursor)
		if err != nil {
			return nil, err
		}
		all = append(all, page.Items...)
		if page.NextCursor == "" {
			return all, nil
		}
		if seen[page.NextCursor] {
			return nil, fmt.Errorf("retail-network: %s: the network repeated page cursor %q", op, page.NextCursor)
		}
		seen[page.NextCursor] = true
		cursor = page.NextCursor
	}
	return nil, fmt.Errorf("retail-network: %s: more than %d pages", op, maxPollPages)
}
