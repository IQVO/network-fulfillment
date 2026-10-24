// Package fleetrest is the small HTTP client the inbound loop uses to call its
// sibling fleet contexts (inbound-receiving, product-master) over their REST
// APIs (ADR 0028). The calls are idempotent by construction (a GET, a PUT, a
// POST with an Idempotency-Key), so the same resilience as the Vendor API
// client applies: a timeout per attempt bounded by the caller's deadline,
// jittered exponential backoff for transport errors and 5xx, no retry of any
// other 4xx, and one circuit breaker per dependency
// (circuit_breaker_state{dependency=...}, ADR 0004).
package fleetrest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/cenkalti/backoff/v4"
	gobreaker "github.com/sony/gobreaker/v2"

	"github.com/claudioed/network-fulfillment/internal/resilience"
)

const (
	defaultCallTimeout = 10 * time.Second
	defaultMaxAttempts = 4
	backoffInitial     = 100 * time.Millisecond
	backoffMax         = 2 * time.Second
	maxBodyBytes       = 4 << 20
)

// ErrCircuitOpen: the breaker refused the call without trying it.
var ErrCircuitOpen = errors.New("fleet dependency circuit breaker open, call not attempted")

// Response is one HTTP answer, body read.
type Response struct {
	Status int
	Body   []byte
}

// Config is what New needs.
type Config struct {
	// Dependency labels the breaker's series, for example "inbound-receiving".
	Dependency string
	BaseURL    string
	HTTPClient *http.Client
	Recorder   resilience.StateRecorder
	// CallTimeout and MaxAttempts default to 10s and 4.
	CallTimeout time.Duration
	MaxAttempts int
	// Sleep waits d or until ctx ends; tests replace it to run without waiting.
	Sleep func(ctx context.Context, d time.Duration) error
}

// Client calls one sibling context.
type Client struct {
	dep         string
	base        *url.URL
	hc          *http.Client
	breaker     *gobreaker.CircuitBreaker[*Response]
	callTimeout time.Duration
	maxAttempts int
	sleep       func(ctx context.Context, d time.Duration) error
}

type serverError struct{ status int }

func (e *serverError) Error() string { return fmt.Sprintf("answered %d", e.status) }

// New builds the client; the base URL must be absolute.
func New(cfg Config) (*Client, error) {
	base, err := url.Parse(cfg.BaseURL)
	if err != nil || base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("%s: %q is not an absolute http(s) URL", cfg.Dependency, cfg.BaseURL)
	}
	c := &Client{dep: cfg.Dependency, base: base, hc: cfg.HTTPClient, callTimeout: cfg.CallTimeout, maxAttempts: cfg.MaxAttempts, sleep: cfg.Sleep}
	if c.hc == nil {
		c.hc = &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()} // a bulkhead of its own
	}
	if c.callTimeout <= 0 {
		c.callTimeout = defaultCallTimeout
	}
	if c.maxAttempts <= 0 {
		c.maxAttempts = defaultMaxAttempts
	}
	if c.sleep == nil {
		c.sleep = sleepContext
	}
	c.breaker = gobreaker.NewCircuitBreaker[*Response](gobreaker.Settings{
		Name:          cfg.Dependency,
		MaxRequests:   resilience.DefaultMaxRequests,
		Interval:      resilience.DefaultInterval,
		Timeout:       resilience.DefaultTimeout,
		ReadyToTrip:   resilience.ReadyToTrip,
		IsExcluded:    func(err error) bool { return errors.Is(err, context.Canceled) },
		OnStateChange: resilience.RecordStateChange(cfg.Dependency, cfg.Recorder),
	})
	return c, nil
}

func sleepContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Request is one call.
type Request struct {
	Method string
	Path   []string // unescaped path elements
	Query  url.Values
	Header map[string]string
	Body   []byte
}

func (c *Client) target(rq Request) string {
	elems := make([]string, len(rq.Path))
	for i, e := range rq.Path {
		elems[i] = url.PathEscape(e)
	}
	u := c.base.JoinPath(elems...)
	u.RawQuery = rq.Query.Encode()
	return u.String()
}

func (c *Client) attempt(ctx context.Context, rq Request) (*Response, error) {
	return c.breaker.Execute(func() (*Response, error) {
		callCtx, cancel := resilience.CallTimeout(ctx, c.callTimeout)
		defer cancel()
		var body io.Reader
		if rq.Body != nil {
			body = bytes.NewReader(rq.Body)
		}
		req, err := http.NewRequestWithContext(callCtx, rq.Method, c.target(rq), body)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		if rq.Body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		for k, v := range rq.Header {
			req.Header.Set(k, v)
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
		out := &Response{Status: resp.StatusCode, Body: raw}
		if resp.StatusCode >= http.StatusInternalServerError {
			return out, &serverError{status: resp.StatusCode}
		}
		return out, nil
	})
}

// Do runs rq with the retry policy and returns the final answer: any non-5xx
// response (the caller maps 4xx), or an error once the attempts are spent.
func (c *Client) Do(ctx context.Context, rq Request) (*Response, error) {
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval, bo.MaxInterval, bo.MaxElapsedTime = backoffInitial, backoffMax, 0
	bo.Reset()
	var lastErr error
	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		resp, err := c.attempt(ctx, rq)
		if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
			return nil, fmt.Errorf("%w: %s: %v", ErrCircuitOpen, c.dep, err)
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if attempt == c.maxAttempts {
			break
		}
		if err := c.sleep(ctx, bo.NextBackOff()); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("%s: %s %s: gave up after %d attempts: %w", c.dep, rq.Method, c.target(rq), c.maxAttempts, lastErr)
}
