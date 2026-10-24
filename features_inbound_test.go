package main_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cucumber/godog"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/retailnetwork"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// The inbound intake steps (features/inbound_shipment_intake.feature). The use
// case and the stub gateway are the real ones; the record of shipments,
// product-master and inbound-receiving are in-process doubles.

const bddInboundSite shared.SiteId = "site-1"

type inboundWorld struct {
	// the receipt return
	manifest contract.InboundShipment
	facts    contract.ReceiptFacts
	queue    *bddReceiptQueue
	returns  *usecases.ReturnInboundReceipts
	down     bool

	gateway   *retailnetwork.StubGateway
	repo      *bddInboundRepo
	book      map[string]contract.GtinResolution
	receiving *bddReceiving
	intake    *usecases.IntakeInboundShipments
}

type bddInboundRow struct {
	rec    contract.InboundRecord
	status contract.InboundStatus
	err    string
	next   time.Time
}

type bddInboundRepo struct {
	rows    map[string]*bddInboundRow
	order   []string
	cursors map[shared.SiteId]time.Time
}

func (r *bddInboundRepo) Record(_ context.Context, s contract.InboundShipment, asn string, now time.Time) (bool, error) {
	if _, ok := r.rows[s.Ref]; ok {
		return false, nil
	}
	r.rows[s.Ref] = &bddInboundRow{rec: contract.InboundRecord{Shipment: s, AsnNumber: asn}, status: contract.InboundPending, next: now}
	r.order = append(r.order, s.Ref)
	return true, nil
}

func (r *bddInboundRepo) Due(_ context.Context, now time.Time, limit int) ([]contract.InboundRecord, error) {
	var out []contract.InboundRecord
	for _, ref := range r.order {
		if row := r.rows[ref]; row.status == contract.InboundPending && !row.next.After(now) && len(out) < limit {
			out = append(out, row.rec)
		}
	}
	return out, nil
}

func (r *bddInboundRepo) MarkRegistered(_ context.Context, ref string, _ time.Time) error {
	r.rows[ref].status = contract.InboundRegistered
	return nil
}

func (r *bddInboundRepo) MarkRetry(_ context.Context, ref, msg string, next time.Time) error {
	row := r.rows[ref]
	row.rec.Attempts++
	row.err, row.next = msg, next
	return nil
}

func (r *bddInboundRepo) MarkRejected(_ context.Context, ref, msg string) error {
	r.rows[ref].status, r.rows[ref].err = contract.InboundRejected, msg
	return nil
}

func (r *bddInboundRepo) Cursor(_ context.Context, site shared.SiteId) (time.Time, error) {
	return r.cursors[site], nil
}

func (r *bddInboundRepo) AdvanceCursor(_ context.Context, site shared.SiteId, to time.Time) error {
	if to.After(r.cursors[site]) {
		r.cursors[site] = to
	}
	return nil
}

type bddBook struct{ w *inboundWorld }

func (b bddBook) ResolveGtin(_ context.Context, gtin string) (contract.GtinResolution, error) {
	if r, ok := b.w.book[gtin]; ok {
		return r, nil
	}
	return contract.GtinResolution{}, contract.ErrGtinNotFound
}

type bddReceiving struct {
	asns    map[string]contract.AsnRegistration
	asked   int
	refused map[string]string
}

func (r *bddReceiving) RegisterAsn(_ context.Context, a contract.AsnRegistration) error {
	r.asked++
	if slug := r.refused[a.AsnNumber]; slug != "" {
		return &contract.AsnRefusedError{Problem: slug, Detail: "refused in the suite"}
	}
	r.asns[a.AsnNumber] = a
	return nil
}

// inbound returns the scenario's inbound world, built on first use.
func (w *world) inbound() *inboundWorld {
	if w.inboundW != nil {
		return w.inboundW
	}
	iw := &inboundWorld{
		gateway:   retailnetwork.NewStubGateway(nil),
		repo:      &bddInboundRepo{rows: map[string]*bddInboundRow{}, cursors: map[shared.SiteId]time.Time{}},
		book:      map[string]contract.GtinResolution{},
		receiving: &bddReceiving{asns: map[string]contract.AsnRegistration{}, refused: map[string]string{}},
	}
	iw.intake = &usecases.IntakeInboundShipments{
		Sites: []shared.SiteId{bddInboundSite}, Gateway: iw.gateway, Shipments: iw.repo,
		Resolver: bddBook{iw}, Registrar: iw.receiving, Clock: w.clock,
	}
	w.inboundW = iw
	return iw
}

func (w *world) productMasterDeclares(gtin, sku string) error {
	w.inbound().book[gtin] = contract.GtinResolution{GTIN: gtin, SKU: sku, Level: "EACH", UnitsPerPack: 1, Active: true}
	return nil
}

func (w *world) theNetworkHasShipment(ref, seller string, palletQty int, mixedA int, gtinA string, mixedB int, gtinB string) error {
	// The step text names the pallet carton's GTIN as the first one declared.
	w.inbound().gateway.SeedInbound(contract.InboundShipment{
		Ref: ref, OwnerRef: seller, SiteId: bddInboundSite, ShippedAt: w.clock.Now().Add(-time.Hour),
		Cartons: []contract.InboundCarton{
			{SSCC: "006141410000000012", ParentSSCC: "006141410000000036", Kind: contract.CartonHomogeneousCase,
				Contents: []contract.InboundContent{{GTIN: "00614141000036", QtyEaches: int64(palletQty)}}},
			{SSCC: "006141410000000029", Kind: contract.CartonMixed,
				Contents: []contract.InboundContent{{GTIN: gtinA, QtyEaches: int64(mixedA)}, {GTIN: gtinB, QtyEaches: int64(mixedB)}}},
		},
	})
	return nil
}

func (w *world) theInboundIntakeRuns() error {
	_, err := w.inbound().intake.Execute(context.Background())
	return err
}

func (w *world) receivingRefuses(asn, why string) error {
	w.inbound().receiving.refused[asn] = strings.ToLower(strings.ReplaceAll(why, " ", "-"))
	return nil
}

func (w *world) receivingHolds(asn, owner string) error {
	a, ok := w.inbound().receiving.asns[asn]
	if !ok {
		return fmt.Errorf("inbound-receiving holds no ASN %q (it holds %d)", asn, len(w.inbound().receiving.asns))
	}
	if a.OwnerRef != owner {
		return fmt.Errorf("ASN %s ownerRef = %q, want %q", asn, a.OwnerRef, owner)
	}
	return nil
}

func (w *world) asnHasLine(asn string, lineNo int, sku string, qty int64) error {
	for _, l := range w.inbound().receiving.asns[asn].Lines {
		if l.LineNo == lineNo {
			if l.SKU != sku || l.ExpectedQty != qty {
				return fmt.Errorf("line %d of %s = %s x%d, want %s x%d", lineNo, asn, l.SKU, l.ExpectedQty, sku, qty)
			}
			return nil
		}
	}
	return fmt.Errorf("ASN %s has no line %d", asn, lineNo)
}

func (w *world) asnHasCartons(asn string, n int) error {
	a := w.inbound().receiving.asns[asn]
	if len(a.Cartons) != n || a.Cartons[0].ParentSSCC == "" || a.Cartons[1].ParentSSCC != "" {
		return fmt.Errorf("ASN %s cartons = %+v, want %d with only the first on a pallet", asn, a.Cartons, n)
	}
	return nil
}

func (w *world) shipmentIs(ref string, status contract.InboundStatus) error {
	row, ok := w.inbound().repo.rows[ref]
	if !ok || row.status != status {
		return fmt.Errorf("shipment %s status = %v, want %s", ref, row, status)
	}
	return nil
}

func (w *world) shipmentIsBecause(ref string, status contract.InboundStatus, because string) error {
	if err := w.shipmentIs(ref, status); err != nil {
		return err
	}
	if got := w.inbound().repo.rows[ref].err; !strings.Contains(got, because) {
		return fmt.Errorf("shipment %s failure = %q, want it to contain %q", ref, got, because)
	}
	return nil
}

func (w *world) receivingWasAskedToRegister(n int) error {
	if got := w.inbound().receiving.asked; got != n {
		return fmt.Errorf("inbound-receiving was asked %d times, want %d", got, n)
	}
	return nil
}

func (w *world) registerInboundSteps(sc *godog.ScenarioContext) {
	sc.Step(`^product-master declares each GTIN "([^"]*)" as "([^"]*)"$`, w.productMasterDeclares)
	sc.Step(`^the network has shipment "([^"]*)" from seller "([^"]*)" with a pallet carton of (\d+) of "[^"]*" and a mixed carton of (\d+) of "([^"]*)" and (\d+) of "([^"]*)"$`, w.theNetworkHasShipment)
	sc.Step(`^the inbound intake runs$`, w.theInboundIntakeRuns)
	sc.Step(`^inbound-receiving refuses ASN "([^"]*)" as a (.+)$`, w.receivingRefuses)
	sc.Step(`^inbound-receiving holds ASN "([^"]*)" owned by "([^"]*)"$`, w.receivingHolds)
	sc.Step(`^ASN "([^"]*)" has line (\d+) "([^"]*)" for (\d+) eaches$`, w.asnHasLine)
	sc.Step(`^ASN "([^"]*)" has (\d+) cartons and the first is on a pallet$`, w.asnHasCartons)
	sc.Step(`^shipment "([^"]*)" is "(PENDING|REGISTERED|REJECTED)"$`, func(ref, s string) error { return w.shipmentIs(ref, contract.InboundStatus(s)) })
	sc.Step(`^shipment "([^"]*)" is "(PENDING|REGISTERED|REJECTED)" because "([^"]*)"$`, func(ref, s, b string) error {
		return w.shipmentIsBecause(ref, contract.InboundStatus(s), b)
	})
	sc.Step(`^inbound-receiving was asked to register (\d+) ASNs?$`, w.receivingWasAskedToRegister)
	w.registerReceiptSteps(sc)
}

// ---- the receipt return (features/inbound_receipt_return.feature) ----

// bddReceiptQueue is the one-receipt projection the steps close.
type bddReceiptQueue struct {
	facts   *contract.ReceiptFacts
	closed  bool
	sent    bool
	next    time.Time
	retries int
}

func (q *bddReceiptQueue) RecordCartonReceived(context.Context, string, contract.ReceivedCartonFact) error {
	return nil
}
func (q *bddReceiptQueue) RecordFinding(context.Context, string, contract.CartonFinding) error {
	return nil
}
func (q *bddReceiptQueue) RecordClosed(context.Context, string, contract.ReceiptClosedFact, time.Time) error {
	return nil
}
func (q *bddReceiptQueue) DueReceipts(_ context.Context, now time.Time, _ int) ([]contract.ReceiptFacts, error) {
	if !q.closed || q.sent || q.next.After(now) {
		return nil, nil
	}
	f := *q.facts
	f.Attempts = q.retries
	return []contract.ReceiptFacts{f}, nil
}
func (q *bddReceiptQueue) MarkReceiptSent(context.Context, string, time.Time) error {
	q.sent = true
	return nil
}
func (q *bddReceiptQueue) MarkReceiptRetry(_ context.Context, _ string, _ string, next time.Time) error {
	q.retries++
	q.next = next
	return nil
}
func (q *bddReceiptQueue) MarkReceiptRejected(context.Context, string, string) error { return nil }

// downGateway fails every receipt while the network is down, and otherwise
// hands over to the real stub gateway.
type downGateway struct {
	*retailnetwork.StubGateway
	iw *inboundWorld
}

func (g downGateway) SubmitInboundReceipt(ctx context.Context, r contract.InboundReceipt) error {
	if g.iw.down {
		return fmt.Errorf("the network is down")
	}
	return g.StubGateway.SubmitInboundReceipt(ctx, r)
}

func (w *world) receiptWorld() *inboundWorld {
	iw := w.inbound()
	if iw.queue != nil {
		return iw
	}
	iw.manifest = contract.InboundShipment{Ref: "SHP-1", SiteId: bddInboundSite}
	iw.facts = contract.ReceiptFacts{Shipment: iw.manifest, ReceiptID: "rcpt-1", ClosedAt: w.clock.Now()}
	iw.queue = &bddReceiptQueue{facts: &iw.facts, next: w.clock.Now()}
	iw.returns = &usecases.ReturnInboundReceipts{Receipts: iw.queue, Gateway: downGateway{iw.gateway, iw}, Clock: w.clock}
	return iw
}

func (w *world) manifestLists(ref, a, b, c string) error {
	iw := w.receiptWorld()
	iw.facts.Shipment.Ref = ref
	for _, sscc := range []string{a, b, c} {
		iw.facts.Shipment.Cartons = append(iw.facts.Shipment.Cartons, contract.InboundCarton{SSCC: sscc, Kind: contract.CartonMixed})
	}
	return nil
}

func (w *world) dockReceived(sscc string, qty int64, gtin, condition string) error {
	iw := w.receiptWorld()
	iw.facts.Cartons = append(iw.facts.Cartons, contract.ReceivedCartonFact{SSCC: sscc, Contents: []contract.ReceivedContent{{GTIN: gtin, Qty: qty, Condition: map[string]string{"good": "Good", "damaged": "Damaged"}[condition]}}})
	return nil
}

func (w *world) dockFoundOffManifest(sscc string) error {
	iw := w.receiptWorld()
	iw.facts.Findings = append(iw.facts.Findings, contract.CartonFinding{Kind: contract.FindingUnexpectedCarton, SSCC: sscc})
	return nil
}

func (w *world) dockRaised(kind, sscc string) error {
	iw := w.receiptWorld()
	iw.facts.Findings = append(iw.facts.Findings, contract.CartonFinding{Kind: kind, SSCC: sscc})
	return nil
}

func (w *world) receivingCloses(string) error {
	w.receiptWorld().queue.closed = true
	return nil
}

func (w *world) receiptReturnRuns() error {
	_, err := w.receiptWorld().returns.Execute(context.Background())
	return err
}

func (w *world) networkIsDown() error { w.receiptWorld().down = true; return nil }
func (w *world) networkIsBack() error { w.receiptWorld().down = false; return nil }

func (w *world) networkReceipt(ref string) (contract.InboundReceipt, bool) {
	return w.receiptWorld().gateway.InboundReceipt(ref)
}

func (w *world) networkHolds(ref string, n int) error {
	r, ok := w.networkReceipt(ref)
	if !ok || len(r.Cartons) != n {
		return fmt.Errorf("receipt for %s = %+v (held: %v), want %d cartons", ref, r, ok, n)
	}
	return nil
}

func (w *world) networkHoldsNone(ref string) error {
	if _, ok := w.networkReceipt(ref); ok {
		return fmt.Errorf("the network holds a receipt for %s", ref)
	}
	return nil
}

func (w *world) receiptCarton(sscc string) (contract.ReceiptCarton, error) {
	r, _ := w.networkReceipt("SHP-1")
	for _, c := range r.Cartons {
		if c.SSCC == sscc {
			return c, nil
		}
	}
	return contract.ReceiptCarton{}, fmt.Errorf("the receipt has no carton %s: %+v", sscc, r.Cartons)
}

func (w *world) cartonIsWithItems(sscc, status string, qty int64, gtin string) error {
	c, err := w.receiptCarton(sscc)
	if err != nil {
		return err
	}
	if string(c.Status) != status || len(c.Items) != 1 || c.Items[0].GTIN != gtin || c.Items[0].QtyEaches != qty {
		return fmt.Errorf("carton %s = %+v, want %s with %d of %s", sscc, c, status, qty, gtin)
	}
	return nil
}

func (w *world) cartonIsWithoutItems(sscc, status string) error {
	c, err := w.receiptCarton(sscc)
	if err != nil {
		return err
	}
	if string(c.Status) != status || len(c.Items) != 0 {
		return fmt.Errorf("carton %s = %+v, want %s with no items", sscc, c, status)
	}
	return nil
}

func (w *world) cartonHasCodes(sscc, codes string) error {
	c, err := w.receiptCarton(sscc)
	if err != nil {
		return err
	}
	if got := strings.Join(c.ComplianceCodes, ","); got != codes {
		return fmt.Errorf("carton %s codes = %q, want %q", sscc, got, codes)
	}
	return nil
}

func (w *world) networkWasSent(n int) error {
	// The stub keeps the first receipt of a shipment; a second send would not show,
	// so count what the queue handed over.
	if !w.receiptWorld().queue.sent && n > 0 {
		return fmt.Errorf("no receipt was reported")
	}
	if _, ok := w.networkReceipt("SHP-1"); ok != (n == 1) {
		return fmt.Errorf("receipt held = %v, want %d", ok, n)
	}
	return nil
}

func (w *world) registerReceiptSteps(sc *godog.ScenarioContext) {
	sc.Step(`^the manifest of shipment "([^"]*)" lists cartons "([^"]*)", "([^"]*)" and "([^"]*)"$`, w.manifestLists)
	sc.Step(`^the dock received carton "([^"]*)" with (\d+) eaches of "([^"]*)" in (good|damaged) condition$`, w.dockReceived)
	sc.Step(`^the dock found carton "([^"]*)" that is not on the manifest$`, w.dockFoundOffManifest)
	sc.Step(`^the dock raised "([^"]*)" on carton "([^"]*)"$`, w.dockRaised)
	sc.Step(`^receiving closes on shipment "([^"]*)"$`, w.receivingCloses)
	sc.Step(`^the receipt return runs$`, w.receiptReturnRuns)
	sc.Step(`^the network is down$`, w.networkIsDown)
	sc.Step(`^the network is back$`, w.networkIsBack)
	sc.Step(`^the network holds a receipt for "([^"]*)" with (\d+) cartons$`, w.networkHolds)
	sc.Step(`^the network holds no receipt for "([^"]*)"$`, w.networkHoldsNone)
	sc.Step(`^carton "([^"]*)" of the receipt is "([A-Z]+)" with (\d+) eaches of "([^"]*)"$`, w.cartonIsWithItems)
	sc.Step(`^carton "([^"]*)" of the receipt is "([A-Z]+)" with no items$`, w.cartonIsWithoutItems)
	sc.Step(`^carton "([^"]*)" of the receipt has the compliance codes "([^"]*)"$`, w.cartonHasCodes)
	sc.Step(`^the network was sent (\d+) receipt$`, w.networkWasSent)
}
