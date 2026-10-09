package main_test

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"strconv"

	"github.com/cucumber/godog"
)

type reportRowBody map[string]any

type reportBody struct {
	Rows []reportRowBody `json:"rows"`
}

func (w *world) report() (reportBody, error) {
	var r reportBody
	return r, w.decode(&r)
}

// ---- When ----------------------------------------------------------------------

func (w *world) iRequestTheReport(ctx context.Context, from, to string) error {
	q := url.Values{"from": {from}, "to": {to}}
	return w.iGETFromReports(ctx, "/reports/acknowledgement?"+q.Encode())
}

func (w *world) iRequestTheReportWithQuery(ctx context.Context, rawQuery string) error {
	return w.iGETFromReports(ctx, "/reports/acknowledgement?"+rawQuery)
}

func (w *world) iRequestTheReportWithNoQuery(ctx context.Context) error {
	return w.iGETFromReports(ctx, "/reports/acknowledgement")
}

func (w *world) iRequestTheFreshness(ctx context.Context) error {
	return w.iGETFromReports(ctx, "/reports/acknowledgement/freshness")
}

// ---- Then ----------------------------------------------------------------------

func (w *world) theReportHasRows(expected int) error {
	r, err := w.report()
	if err != nil {
		return err
	}
	if len(r.Rows) != expected {
		return fmt.Errorf("expected %d report row(s), got %d: %s", expected, len(r.Rows), string(w.body))
	}
	return nil
}

func (w *world) theReportHasAnEmptyRowsArray() error {
	return w.theBodyContains(`"rows":[]`)
}

func (w *world) theReportRowHas(day, column string, expected float64) error {
	r, err := w.report()
	if err != nil {
		return err
	}
	for _, row := range r.Rows {
		if row["dayBucket"] != day {
			continue
		}
		got, ok := row[column].(float64)
		if !ok {
			return fmt.Errorf("report row %s has no numeric %q column: %v", day, column, row)
		}
		if math.Abs(got-expected) > 1e-9 {
			return fmt.Errorf("expected %s of day %s to be %v, got %v", column, day, expected, got)
		}
		return nil
	}
	return fmt.Errorf("no report row for day %s in %s", day, string(w.body))
}

func (w *world) theFreshnessLagIs(seconds float64) error {
	var f struct {
		LagSeconds *float64 `json:"lagSeconds"`
	}
	if err := w.decode(&f); err != nil {
		return err
	}
	if f.LagSeconds == nil {
		return fmt.Errorf("expected a lagSeconds field, got %s", string(w.body))
	}
	if math.Abs(*f.LagSeconds-seconds) > 1e-9 {
		return fmt.Errorf("expected freshness lag of %v seconds, got %v", seconds, *f.LagSeconds)
	}
	return nil
}

func (w *world) theReportsHealthIs(status string) error {
	var h struct {
		Status string `json:"status"`
	}
	if err := w.decode(&h); err != nil {
		return err
	}
	if h.Status != status {
		return fmt.Errorf("expected status %q, got %q", status, h.Status)
	}
	return nil
}

func (w *world) registerReportSteps(sc *godog.ScenarioContext) {
	sc.Step(`^I request the acknowledgement report from "([^"]*)" to "([^"]*)"$`, w.iRequestTheReport)
	sc.Step(`^I request the acknowledgement report with query "([^"]*)"$`, w.iRequestTheReportWithQuery)
	sc.Step(`^I request the acknowledgement report with no query$`, w.iRequestTheReportWithNoQuery)
	sc.Step(`^I request the report freshness$`, w.iRequestTheFreshness)

	sc.Step(`^the report has (\d+) rows?$`, w.theReportHasRows)
	sc.Step(`^the report has an empty rows array$`, w.theReportHasAnEmptyRowsArray)
	sc.Step(`^the report row for day "([^"]*)" has "([^"]*)" equal to ([0-9.]+)$`, func(day, column, value string) error {
		expected, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return err
		}
		return w.theReportRowHas(day, column, expected)
	})
	sc.Step(`^the freshness lag is ([0-9.]+) seconds$`, func(value string) error {
		expected, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return err
		}
		return w.theFreshnessLagIs(expected)
	})
	sc.Step(`^the reports service status is "([^"]*)"$`, w.theReportsHealthIs)
}
