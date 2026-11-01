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

// pollPage fetches one page of purchase orders.
func (c *apiClient) pollPage(ctx context.Context, createdAfter time.Time, nextToken string) (purchaseOrderPageDTO, error) {
	q := c.vendorQuery()
	q.Set("limit", strconv.Itoa(pollPageLimit))
	if nextToken != "" {
		q.Set("nextToken", nextToken)
	} else if !createdAfter.IsZero() {
		q.Set("createdAfter", createdAfter.UTC().Format(time.RFC3339Nano))
	}
	var page purchaseOrderPageDTO
	_, err := c.call(ctx, request{op: "pollPurchaseOrders", method: http.MethodGet, path: []string{"vendor", "purchase-orders"}, query: q}, &page)
	return page, err
}

func (c *apiClient) getPurchaseOrder(ctx context.Context, poNumber string) (purchaseOrderDTO, error) {
	var po purchaseOrderDTO
	_, err := c.call(ctx, request{op: "getPurchaseOrder", method: http.MethodGet, path: []string{"vendor", "purchase-orders", poNumber}, query: c.vendorQuery()}, &po)
	return po, err
}

func (c *apiClient) getTransaction(ctx context.Context, id string) (transactionDTO, error) {
	var tx transactionDTO
	_, err := c.call(ctx, request{op: "getTransaction", method: http.MethodGet, path: []string{"vendor", "transactions", id}, query: c.vendorQuery()}, &tx)
	return tx, err
}

func (c *apiClient) listNodes(ctx context.Context) ([]vendorNodeDTO, error) {
	var list vendorNodeListDTO
	_, err := c.call(ctx, request{op: "listVendorNodes", method: http.MethodGet, path: []string{"vendor", "nodes"}, query: c.vendorQuery()}, &list)
	return list.Nodes, err
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
	if _, err := c.call(ctx, request{op: op, method: method, path: path, idemKey: idemKey, body: body}, &accepted); err != nil {
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
	_, err := c.call(ctx, request{op: "getNode", method: http.MethodGet, path: []string{"vendor", "nodes", nodeID}, query: c.vendorQuery()}, &node)
	return node, err
}

// requestLabels is the one synchronous write: 200 with the labels.
func (c *apiClient) requestLabels(ctx context.Context, idemKey string, body []byte) (labelResponseDTO, error) {
	var out labelResponseDTO
	_, err := c.call(ctx, request{op: "requestLabel", method: http.MethodPost, path: []string{"vendor", "shipping-labels"}, idemKey: idemKey, body: body}, &out)
	return out, err
}

// pollAll pages through every purchase order created after createdAfter. A page
// that repeats a token, or more pages than any real backlog, is an error rather
// than a loop.
func (c *apiClient) pollAll(ctx context.Context, createdAfter time.Time) ([]purchaseOrderDTO, error) {
	var all []purchaseOrderDTO
	seen := map[string]bool{}
	token := ""
	for range maxPollPages {
		page, err := c.pollPage(ctx, createdAfter, token)
		if err != nil {
			return nil, err
		}
		all = append(all, page.PurchaseOrders...)
		if page.NextToken == "" {
			return all, nil
		}
		if seen[page.NextToken] {
			return nil, fmt.Errorf("retail-network: pollPurchaseOrders: the network repeated page token %q", page.NextToken)
		}
		seen[page.NextToken] = true
		token = page.NextToken
	}
	return nil, fmt.Errorf("retail-network: pollPurchaseOrders: more than %d pages", maxPollPages)
}
