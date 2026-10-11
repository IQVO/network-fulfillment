package main_test

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cucumber/godog"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// ---- response shapes (the wire contract in apis/openapi.yaml) ---------------

type lineBody struct {
	NetworkLineRef   string `json:"networkLineRef"`
	NetworkProductID string `json:"networkProductId"`
	SKU              string `json:"sku"`
	Quantity         int    `json:"quantity"`
}

type orderBody struct {
	NetworkRef             string     `json:"networkRef"`
	SiteID                 string     `json:"siteId"`
	State                  string     `json:"state"`
	RequiredShipBy         string     `json:"requiredShipBy"`
	AcknowledgeBy          string     `json:"acknowledgeBy"`
	ReceivedAt             string     `json:"receivedAt"`
	LocalOrderID           *string    `json:"localOrderId"`
	AcknowledgementOverdue bool       `json:"acknowledgementOverdue"`
	Lines                  []lineBody `json:"lines"`
}

type inboundStatusBody struct {
	NetworkMode string  `json:"networkMode"`
	Polls       int     `json:"polls"`
	Received    int     `json:"received"`
	Failed      int     `json:"failed"`
	Since       *string `json:"since"`
	Unanswered  int     `json:"unanswered"`
	Overdue     int     `json:"overdue"`
}

func (w *world) order() (orderBody, error) {
	var o orderBody
	return o, w.decode(&o)
}

// ---- Given: dictionary and demand --------------------------------------------

func (w *world) dictionaryMaps(productID, sku string) error {
	w.dict.Add(shared.NetworkProductId(productID), shared.SKU(sku))
	return nil
}

// seedDemand puts a unit of demand into the stub network, as NETWORK_SEED_FILE
// does in a local run (ADR 0012). The poller picks it up on its next pass.
func (w *world) seedDemand(ref, site string, dueInHours int, lines []contract.InboundLine) {
	d := contract.InboundDemand{
		NetworkRef:     shared.NetworkRef(ref),
		SiteId:         shared.SiteId(site),
		RequiredShipBy: w.clock.Now().Add(time.Duration(dueInHours) * time.Hour),
		Lines:          lines,
	}
	w.demands[ref] = d
	w.gateway.Seed(d)
}

func (w *world) networkHasDemand(ref string, qty int, productID string) error {
	w.seedDemand(ref, "site-1", 48, []contract.InboundLine{
		{NetworkLineRef: "1", NetworkProductId: shared.NetworkProductId(productID), Quantity: qty},
	})
	return nil
}

func (w *world) networkHasDemandWithLines(ref, site string, dueInHours int, table *godog.Table) error {
	lines, err := linesFromTable(table)
	if err != nil {
		return err
	}
	w.seedDemand(ref, site, dueInHours, lines)
	return nil
}

func linesFromTable(table *godog.Table) ([]contract.InboundLine, error) {
	if len(table.Rows) == 0 {
		return nil, fmt.Errorf("the demand table needs a header row: line | product | quantity")
	}
	col := map[string]int{}
	for i, cell := range table.Rows[0].Cells {
		col[cell.Value] = i
	}
	lines := make([]contract.InboundLine, 0, len(table.Rows)-1)
	for _, row := range table.Rows[1:] {
		qty, err := strconv.Atoi(row.Cells[col["quantity"]].Value)
		if err != nil {
			return nil, fmt.Errorf("quantity %q is not a number", row.Cells[col["quantity"]].Value)
		}
		lines = append(lines, contract.InboundLine{
			NetworkLineRef:   shared.NetworkLineRef(row.Cells[col["line"]].Value),
			NetworkProductId: shared.NetworkProductId(row.Cells[col["product"]].Value),
			Quantity:         qty,
		})
	}
	return lines, nil
}

// theNetworkDeliversAgain re-lists demand the network already delivered: a
// poll-based inbound leg WILL see the same purchase order twice (ADR 0001 §5).
func (w *world) theNetworkDeliversAgain(ref string) error {
	d, ok := w.demands[ref]
	if !ok {
		return fmt.Errorf("no demand %q was ever seeded in this scenario", ref)
	}
	w.gateway.Seed(d)
	return nil
}

// aNetworkOrderInState drives a fixture order through the real use cases to
// the requested lifecycle state, then verifies it got there.
func (w *world) aNetworkOrderInState(ctx context.Context, ref, state string) error {
	d := contract.InboundDemand{
		NetworkRef:     shared.NetworkRef(ref),
		SiteId:         "site-1",
		RequiredShipBy: w.clock.Now().Add(48 * time.Hour),
		Lines:          []contract.InboundLine{{NetworkLineRef: "1", NetworkProductId: "ASIN-1", Quantity: 2}},
	}
	w.demands[ref] = d
	if err := w.driveToState(ctx, d, state); err != nil {
		return err
	}
	o, err := w.orders.FindByRef(ctx, d.NetworkRef)
	if err != nil {
		return err
	}
	if o == nil || string(o.State()) != state {
		return fmt.Errorf("fixture order %q should be %s, but is %v", ref, state, o)
	}
	return nil
}

func (w *world) driveToState(ctx context.Context, d contract.InboundDemand, state string) error {
	switch state {
	case "NEW":
		// order-management is down at receipt, so the order is recorded but
		// can never be answered.
		w.planner.unavailable = true
		defer func() { w.planner.unavailable = false }()
		if _, err := w.receive.Execute(ctx, d); err == nil {
			return fmt.Errorf("expected receipt to fail while order-management is down")
		}
		return nil
	case "REJECTED":
		w.planner.feasible = false
		defer func() { w.planner.feasible = true }()
		_, err := w.receive.Execute(ctx, d)
		return err
	case "SUBMITTED", "ACKNOWLEDGED", "CONFIRMED":
		return w.driveSubmittedOnwards(ctx, d, state)
	default:
		return fmt.Errorf("unknown lifecycle state %q", state)
	}
}

func (w *world) driveSubmittedOnwards(ctx context.Context, d contract.InboundDemand, state string) error {
	if _, err := w.receive.Execute(ctx, d); err != nil {
		return err
	}
	if state == "SUBMITTED" {
		return nil
	}
	if _, err := w.reconcile.Execute(ctx); err != nil {
		return err
	}
	if state == "ACKNOWLEDGED" {
		return nil
	}
	if err := w.iConfirmShipment(ctx, string(d.NetworkRef)); err != nil {
		return err
	}
	return w.theResponseStatusIs(http.StatusNoContent)
}

// ---- When ----------------------------------------------------------------------

func (w *world) thePollerRunsAPass(ctx context.Context) error {
	w.poller.PollOnce(ctx)
	return nil
}

func (w *world) iRequestNetworkOrder(ctx context.Context, ref string) error {
	return w.iGET(ctx, "/network-orders/"+ref)
}

func (w *world) iListTheUnansweredNetworkOrders(ctx context.Context) error {
	return w.iGET(ctx, "/network-orders")
}

func (w *world) iRequestTheInboundStatus(ctx context.Context) error {
	return w.iGET(ctx, "/inbound-status")
}

func (w *world) iConfirmShipment(ctx context.Context, ref string) error {
	return w.record(ctx, w.apiServer(), http.MethodPost, "/network-orders/"+ref+"/shipment-confirmation")
}

// ---- Then: one network order --------------------------------------------------

func (w *world) theNetworkOrderStateIs(state string) error {
	o, err := w.order()
	if err != nil {
		return err
	}
	if o.State != state {
		return fmt.Errorf("expected network order state %s, got %s", state, o.State)
	}
	return nil
}

func (w *world) theNetworkOrderReportsReference(ref, site string) error {
	o, err := w.order()
	if err != nil {
		return err
	}
	if o.NetworkRef != ref || o.SiteID != site {
		return fmt.Errorf("expected network order %q for site %q, got %q for %q", ref, site, o.NetworkRef, o.SiteID)
	}
	return nil
}

func (w *world) theNetworkOrderHasALine(lineRef, productID, sku string, qty int) error {
	o, err := w.order()
	if err != nil {
		return err
	}
	want := lineBody{NetworkLineRef: lineRef, NetworkProductID: productID, SKU: sku, Quantity: qty}
	for _, l := range o.Lines {
		if l == want {
			return nil
		}
	}
	return fmt.Errorf("expected line %+v among %+v", want, o.Lines)
}

func (w *world) theAcknowledgementWindowIsHoursFromReceipt(hours int) error {
	o, err := w.order()
	if err != nil {
		return err
	}
	received, err := time.Parse(time.RFC3339, o.ReceivedAt)
	if err != nil {
		return err
	}
	ackBy, err := time.Parse(time.RFC3339, o.AcknowledgeBy)
	if err != nil {
		return err
	}
	if got := ackBy.Sub(received); got != time.Duration(hours)*time.Hour {
		return fmt.Errorf("expected acknowledgeBy %d hours after receivedAt, got %s", hours, got)
	}
	return nil
}

func (w *world) theNetworkOrderWasReceivedAt(expected string) error {
	o, err := w.order()
	if err != nil {
		return err
	}
	if o.ReceivedAt != expected {
		return fmt.Errorf("expected receivedAt %s, got %s", expected, o.ReceivedAt)
	}
	return nil
}

func (w *world) theRequiredShipByIsKeptAsSent() error {
	o, err := w.order()
	if err != nil {
		return err
	}
	expected := w.demands[o.NetworkRef].RequiredShipBy.UTC().Format(time.RFC3339)
	if o.RequiredShipBy != expected {
		return fmt.Errorf("expected the network's requiredShipBy %s to be kept as sent, got %s", expected, o.RequiredShipBy)
	}
	return nil
}

func (w *world) theNetworkOrderHasALocalOrder() error {
	o, err := w.order()
	if err != nil {
		return err
	}
	if o.LocalOrderID == nil || *o.LocalOrderID == "" {
		return fmt.Errorf("expected a localOrderId, got none")
	}
	return nil
}

func (w *world) theNetworkOrderHasNoLocalOrder() error {
	var raw map[string]any
	if err := w.decode(&raw); err != nil {
		return err
	}
	if v, present := raw["localOrderId"]; present {
		return fmt.Errorf("expected localOrderId to be absent, got %v", v)
	}
	return nil
}

func (w *world) theNetworkOrderIsOverdue(overdue bool) error {
	o, err := w.order()
	if err != nil {
		return err
	}
	if o.AcknowledgementOverdue != overdue {
		return fmt.Errorf("expected acknowledgementOverdue=%t, got %t", overdue, o.AcknowledgementOverdue)
	}
	return nil
}

var (
	orderKeys = []string{"networkRef", "siteId", "state", "requiredShipBy", "acknowledgeBy", "receivedAt", "localOrderId", "acknowledgementOverdue", "lines"}
	lineKeys  = []string{"networkLineRef", "networkProductId", "sku", "quantity"}
)

// theNetworkOrderCarriesOnlyDocumentedProperties is the PII rule (ADR 0001
// hard rule 2): ship-to name, address and phone stop in this context, so
// NOTHING beyond the documented schema may appear in a response.
func (w *world) theNetworkOrderCarriesOnlyDocumentedProperties() error {
	var raw map[string]any
	if err := w.decode(&raw); err != nil {
		return err
	}
	if err := onlyKeys("network order", raw, orderKeys); err != nil {
		return err
	}
	lines, _ := raw["lines"].([]any)
	for _, l := range lines {
		line, _ := l.(map[string]any)
		if err := onlyKeys("network order line", line, lineKeys); err != nil {
			return err
		}
	}
	return nil
}

func onlyKeys(what string, got map[string]any, allowed []string) error {
	ok := map[string]bool{}
	for _, k := range allowed {
		ok[k] = true
	}
	for k := range got {
		if !ok[k] {
			return fmt.Errorf("%s exposes undocumented property %q", what, k)
		}
	}
	return nil
}

// ---- Then: the unanswered list -----------------------------------------------

func (w *world) unansweredRefs() ([]string, error) {
	var list struct {
		NetworkOrders []orderBody `json:"networkOrders"`
	}
	if err := w.decode(&list); err != nil {
		return nil, err
	}
	refs := make([]string, 0, len(list.NetworkOrders))
	for _, o := range list.NetworkOrders {
		refs = append(refs, o.NetworkRef)
	}
	return refs, nil
}

var quoted = regexp.MustCompile(`"([^"]*)"`)

func (w *world) theUnansweredOrdersAre(quotedList string) error {
	var want []string
	for _, m := range quoted.FindAllStringSubmatch(quotedList, -1) {
		want = append(want, m[1])
	}
	got, err := w.unansweredRefs()
	if err != nil {
		return err
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		return fmt.Errorf("expected unanswered orders %v, got %v", want, got)
	}
	return nil
}

func (w *world) theUnansweredListIsEmpty() error {
	refs, err := w.unansweredRefs()
	if err != nil {
		return err
	}
	if len(refs) != 0 {
		return fmt.Errorf("expected no unanswered orders, got %v", refs)
	}
	return w.theBodyContains(`"networkOrders":[]`)
}

// ---- Then: the inbound status ------------------------------------------------

func (w *world) theInboundStatusReports(counter string, expected int) error {
	var s inboundStatusBody
	if err := w.decode(&s); err != nil {
		return err
	}
	got := map[string]int{"polls": s.Polls, "received": s.Received, "failed": s.Failed, "unanswered": s.Unanswered, "overdue": s.Overdue}[counter]
	if got != expected {
		return fmt.Errorf("expected inbound status %s=%d, got %d (%s)", counter, expected, got, string(w.body))
	}
	return nil
}

func (w *world) theInboundStatusNetworkModeIs(mode string) error {
	var s inboundStatusBody
	if err := w.decode(&s); err != nil {
		return err
	}
	if s.NetworkMode != mode {
		return fmt.Errorf("expected networkMode %q, got %q", mode, s.NetworkMode)
	}
	return nil
}

func (w *world) theInboundStatusHasNoWatermark() error {
	var raw map[string]any
	if err := w.decode(&raw); err != nil {
		return err
	}
	if v, present := raw["since"]; present {
		return fmt.Errorf("expected the watermark (since) to be absent, got %v", v)
	}
	return nil
}

func (w *world) theInboundStatusWatermarkIsNow() error {
	var s inboundStatusBody
	if err := w.decode(&s); err != nil {
		return err
	}
	expected := w.clock.Now().UTC().Format(time.RFC3339)
	if s.Since == nil || *s.Since != expected {
		return fmt.Errorf("expected the watermark to be %s, got %v", expected, s.Since)
	}
	return nil
}

func (w *world) registerOrderSteps(sc *godog.ScenarioContext) {
	sc.Step(`^the product dictionary maps "([^"]*)" to "([^"]*)"$`, w.dictionaryMaps)
	sc.Step(`^the network has demand "([^"]*)" for (\d+) units? of "([^"]*)"$`, w.networkHasDemand)
	sc.Step(`^the network has demand "([^"]*)" for site "([^"]*)" due in (\d+) hours with lines:$`, w.networkHasDemandWithLines)
	sc.Step(`^the network delivers purchase order "([^"]*)" again$`, w.theNetworkDeliversAgain)
	sc.Step(`^a network order "([^"]*)" in state "([^"]*)"$`, w.aNetworkOrderInState)

	sc.Step(`^the poller runs a pass$`, w.thePollerRunsAPass)
	sc.Step(`^I request network order "([^"]*)"$`, w.iRequestNetworkOrder)
	sc.Step(`^I list the unanswered network orders$`, w.iListTheUnansweredNetworkOrders)
	sc.Step(`^I request the inbound status$`, w.iRequestTheInboundStatus)
	sc.Step(`^I confirm shipment of network order "([^"]*)"$`, w.iConfirmShipment)

	sc.Step(`^the network order state is "([^"]*)"$`, w.theNetworkOrderStateIs)
	sc.Step(`^the network order is "([^"]*)" for site "([^"]*)"$`, w.theNetworkOrderReportsReference)
	sc.Step(`^the network order has line "([^"]*)" for product "([^"]*)" translated to SKU "([^"]*)" with quantity (\d+)$`, w.theNetworkOrderHasALine)
	sc.Step(`^the acknowledgement window closes (\d+) hours after receipt$`, w.theAcknowledgementWindowIsHoursFromReceipt)
	sc.Step(`^the network order was received at "([^"]*)"$`, w.theNetworkOrderWasReceivedAt)
	sc.Step(`^the network order keeps the required ship-by the network set$`, w.theRequiredShipByIsKeptAsSent)
	sc.Step(`^the network order has a local order$`, w.theNetworkOrderHasALocalOrder)
	sc.Step(`^the network order has no local order$`, w.theNetworkOrderHasNoLocalOrder)
	sc.Step(`^the network order is overdue$`, func() error { return w.theNetworkOrderIsOverdue(true) })
	sc.Step(`^the network order is not overdue$`, func() error { return w.theNetworkOrderIsOverdue(false) })
	sc.Step(`^the network order carries only the documented properties$`, w.theNetworkOrderCarriesOnlyDocumentedProperties)

	sc.Step(`^the unanswered orders, soonest deadline first, are (.+)$`, w.theUnansweredOrdersAre)
	sc.Step(`^the unanswered list is empty$`, w.theUnansweredListIsEmpty)

	sc.Step(`^the inbound status reports (polls|received|failed|unanswered|overdue) (\d+)$`, w.theInboundStatusReports)
	sc.Step(`^the inbound status reports network mode "([^"]*)"$`, w.theInboundStatusNetworkModeIs)
	sc.Step(`^the inbound status has no watermark$`, w.theInboundStatusHasNoWatermark)
	sc.Step(`^the inbound status watermark is the time of the last pass$`, w.theInboundStatusWatermarkIsNow)
}
