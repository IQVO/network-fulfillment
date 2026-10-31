package main_test

import (
	"context"
	"io"
	"net/http"
	"strings"

	"github.com/cucumber/godog"
)

// Wave-2 steps: request-hardening scenarios that must send a raw body the
// existing steps (built on http.NoBody) cannot express. Everything else in
// features/request_hardening.feature reuses the suite's generic vocabulary.

// iSendWithBody issues a request carrying a literal body string against the
// OLTP server and records the response for the Then steps. The body is
// deliberately NOT assumed to be JSON: the NUL-hardening middleware rejects
// bytes before any handler parses them.
func (w *world) iSendWithBody(ctx context.Context, method, path, body string) error {
	req, err := http.NewRequestWithContext(ctx, method, w.apiServer().URL+path, strings.NewReader(body))
	if err != nil {
		return err
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	w.status, w.body, w.headers = resp.StatusCode, b, resp.Header
	return nil
}

func (w *world) registerHardeningSteps(sc *godog.ScenarioContext) {
	sc.Step(`^I send "(GET|POST|PUT|PATCH|DELETE)" to "([^"]*)" with body "([^"]*)"$`, w.iSendWithBody)
}
