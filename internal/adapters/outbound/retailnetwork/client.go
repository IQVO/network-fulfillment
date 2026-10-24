package retailnetwork

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"
	gobreaker "github.com/sony/gobreaker/v2"

	"github.com/claudioed/network-fulfillment/internal/resilience"
)

// DependencyName labels the breaker's Prometheus series
// (circuit_breaker_state{dependency="retail-network"}), next to
// order-management's (ADR 0004 §5).
const DependencyName = "retail-network"

const (
	defaultCallTimeout = 10 * time.Second
	defaultMaxAttempts = 5
	// maxRetryAfter bounds how long one call will wait because the network
	// said 429. A longer wait is reported to the caller, whose own ticker is the
	// retry (the poller, the reconciliation), instead of parking a goroutine.
	maxRetryAfter  = 30 * time.Second
	backoffInitial = 100 * time.Millisecond
	backoffMax     = 2 * time.Second
	maxBodyBytes   = 8 << 20
	pollPageLimit  = 200
	maxPollPages   = 500
)

// apiResponse is one HTTP answer, body read.
type apiResponse struct {
	status int
	header http.Header
	body   []byte
}

// clientConfig is what newAPIClient needs.
type clientConfig struct {
	baseURL     string
	vendorID    string
	httpClient  *http.Client
	logger      *slog.Logger
	recorder    resilience.StateRecorder
	callTimeout time.Duration
	maxAttempts int
	cooldown    time.Duration
	now         func() time.Time
	// sleep waits d or until ctx ends. Tests replace it to run without waiting.
	sleep func(ctx context.Context, d time.Duration) error
}

// apiClient is the Vendor API over HTTP (ADR 0026 Decision 8): a per-attempt
// timeout, retries with jittered backoff for transport errors and 5xx, a wait
// for Retry-After on 429, no retry of any other 4xx, and one circuit breaker
// for the dependency.
type apiClient struct {
	base        *url.URL
	vendorID    string
	hc          *http.Client
	breaker     *gobreaker.CircuitBreaker[*apiResponse]
	logger      *slog.Logger
	callTimeout time.Duration
	maxAttempts int
	now         func() time.Time
	sleep       func(ctx context.Context, d time.Duration) error
}

func newAPIClient(cfg clientConfig) (*apiClient, error) {
	base, err := url.Parse(cfg.baseURL)
	if err != nil || base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("retail-network: NETWORK_BASE_URL %q is not an absolute http(s) URL", cfg.baseURL)
	}
	c := &apiClient{
		base: base, vendorID: cfg.vendorID, hc: cfg.httpClient, logger: cfg.logger,
		callTimeout: cfg.callTimeout, maxAttempts: cfg.maxAttempts, now: cfg.now, sleep: cfg.sleep,
	}
	if c.hc == nil {
		// A client of its own: a bulkhead, so this dependency's slow sockets
		// never share a pool with order-management's.
		c.hc = &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
	}
	if c.logger == nil {
		c.logger = slog.Default()
	}
	if c.callTimeout <= 0 {
		c.callTimeout = defaultCallTimeout
	}
	if c.maxAttempts <= 0 {
		c.maxAttempts = defaultMaxAttempts
	}
	if c.now == nil {
		c.now = time.Now
	}
	if c.sleep == nil {
		c.sleep = sleepContext
	}
	cooldown := cfg.cooldown
	if cooldown <= 0 {
		cooldown = resilience.DefaultTimeout
	}
	c.breaker = gobreaker.NewCircuitBreaker[*apiResponse](gobreaker.Settings{
		Name:        DependencyName,
		MaxRequests: resilience.DefaultMaxRequests,
		Interval:    resilience.DefaultInterval,
		Timeout:     cooldown,
		ReadyToTrip: resilience.ReadyToTrip,
		// The caller giving up is not the dependency's fault.
		IsExcluded:    func(err error) bool { return errors.Is(err, context.Canceled) },
		OnStateChange: resilience.RecordStateChange(DependencyName, cfg.recorder),
	})
	return c, nil
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// request is one call to the Vendor API.
type request struct {
	op      string // for logs and errors
	method  string
	path    []string // unescaped path elements after the base URL
	query   url.Values
	idemKey string // set on every write
	body    []byte
}

func (c *apiClient) url(rq request) string {
	elems := make([]string, len(rq.path))
	for i, e := range rq.path {
		elems[i] = url.PathEscape(e)
	}
	u := c.base.JoinPath(elems...)
	u.RawQuery = rq.query.Encode()
	return u.String()
}

// attempt makes one HTTP call through the breaker. A transport error, a timeout
// and a 5xx are failures of the dependency; any other answer, 429 included, is
// the dependency working.
func (c *apiClient) attempt(ctx context.Context, rq request) (*apiResponse, error) {
	return c.breaker.Execute(func() (*apiResponse, error) {
		callCtx, cancel := resilience.CallTimeout(ctx, c.callTimeout)
		defer cancel()

		var body io.Reader
		if rq.body != nil {
			body = bytes.NewReader(rq.body)
		}
		req, err := http.NewRequestWithContext(callCtx, rq.method, c.url(rq), body)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		if rq.body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if rq.idemKey != "" {
			req.Header.Set("Idempotency-Key", rq.idemKey)
		}
		resp, err := c.hc.Do(req)
		if err != nil {
			return nil, err
		}
		defer func() { _ = resp.Body.Close() }()
		raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
		if err != nil {
			return nil, err
		}
		out := &apiResponse{status: resp.StatusCode, header: resp.Header, body: raw}
		if resp.StatusCode >= http.StatusInternalServerError {
			return out, &serverError{status: resp.StatusCode}
		}
		return out, nil
	})
}

func isBreakerRejection(err error) bool {
	return errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests)
}

// do runs rq with the retry policy and returns the final answer: a non-5xx,
// non-429 response, or an error once the attempts are spent.
func (c *apiClient) do(ctx context.Context, rq request) (*apiResponse, error) {
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval, bo.MaxInterval, bo.MaxElapsedTime = backoffInitial, backoffMax, 0
	bo.Reset()

	var lastErr error
	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		resp, err := c.attempt(ctx, rq)
		if isBreakerRejection(err) {
			return nil, fmt.Errorf("%w: %v", ErrCircuitOpen, err)
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		wait := bo.NextBackOff()
		switch {
		case err == nil && resp.status == http.StatusTooManyRequests:
			ra, ok := retryAfter(resp.header, c.now())
			if !ok {
				ra = wait
			}
			if ra > maxRetryAfter || exceedsDeadline(ctx, ra, c.now()) {
				return nil, &RateLimitedError{Op: rq.op, RetryAfter: ra}
			}
			lastErr = &RateLimitedError{Op: rq.op, RetryAfter: ra}
			wait = ra
		case err == nil:
			return resp, nil
		default:
			lastErr = err
		}
		if attempt == c.maxAttempts {
			break
		}
		c.logger.Warn("retail-network call will be retried", "op", rq.op, "attempt", attempt, "wait", wait, "err", lastErr)
		if err := c.sleep(ctx, wait); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("retail-network: %s: gave up after %d attempts: %w", rq.op, c.maxAttempts, lastErr)
}

func exceedsDeadline(ctx context.Context, wait time.Duration, now time.Time) bool {
	deadline, ok := ctx.Deadline()
	return ok && now.Add(wait).After(deadline)
}

// call runs rq and decodes a successful JSON answer into out (when not nil). Any
// other answer becomes an *APIError carrying the problem document.
func (c *apiClient) call(ctx context.Context, rq request, out any) (*apiResponse, error) {
	resp, err := c.do(ctx, rq)
	if err != nil {
		return nil, err
	}
	if resp.status < 200 || resp.status > 299 {
		return resp, problemError(rq.op, resp)
	}
	if out != nil {
		if err := json.Unmarshal(resp.body, out); err != nil {
			return resp, fmt.Errorf("retail-network: %s: undecodable %d answer: %w", rq.op, resp.status, err)
		}
	}
	return resp, nil
}

// problemError builds the *APIError for a non-2xx answer. A body that is not a
// problem document still yields an error naming the status.
func problemError(op string, resp *apiResponse) error {
	var p struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Detail string `json:"detail"`
	}
	_ = json.Unmarshal(resp.body, &p)
	if p.Type == "" {
		p.Type = "http-" + strconv.Itoa(resp.status)
	}
	return &APIError{Op: op, Status: resp.status, Type: p.Type, Title: p.Title, Detail: p.Detail}
}

// ---- operations ----

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
	var accepted transactionAcceptedDTO
	if _, err := c.call(ctx, request{op: op, method: http.MethodPost, path: path, idemKey: idemKey, body: body}, &accepted); err != nil {
		return "", err
	}
	if accepted.TransactionID == "" {
		return "", fmt.Errorf("retail-network: %s: the answer carries no transactionId", op)
	}
	return accepted.TransactionID, nil
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
