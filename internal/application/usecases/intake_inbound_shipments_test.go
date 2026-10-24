package usecases_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

var intakeNow = time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)

type movableClock struct{ t time.Time }

func (c *movableClock) Now() time.Time { return c.t }

type intakeGateway struct {
	bySite map[shared.SiteId][]contract.InboundShipment
	err    error
	since  map[shared.SiteId]time.Time
}

func (g *intakeGateway) PollInboundShipments(_ context.Context, site shared.SiteId, since time.Time) ([]contract.InboundShipment, error) {
	if g.since == nil {
		g.since = map[shared.SiteId]time.Time{}
	}
	g.since[site] = since
	return g.bySite[site], g.err
}

func (g *intakeGateway) SubmitInboundReceipt(context.Context, contract.InboundReceipt) error {
	return nil
}

type intakeRow struct {
	rec       contract.InboundRecord
	status    contract.InboundStatus
	lastError string
	next      time.Time
}

type intakeRepo struct {
	rows    map[string]*intakeRow
	order   []string
	cursors map[shared.SiteId]time.Time
	recErr  error
}

func newIntakeRepo() *intakeRepo {
	return &intakeRepo{rows: map[string]*intakeRow{}, cursors: map[shared.SiteId]time.Time{}}
}

func (r *intakeRepo) Record(_ context.Context, s contract.InboundShipment, asn string, now time.Time) (bool, error) {
	if r.recErr != nil {
		return false, r.recErr
	}
	if _, ok := r.rows[s.Ref]; ok {
		return false, nil
	}
	r.rows[s.Ref] = &intakeRow{rec: contract.InboundRecord{Shipment: s, AsnNumber: asn}, status: contract.InboundPending, next: now}
	r.order = append(r.order, s.Ref)
	return true, nil
}

func (r *intakeRepo) Due(_ context.Context, now time.Time, limit int) ([]contract.InboundRecord, error) {
	var out []contract.InboundRecord
	for _, ref := range r.order {
		row := r.rows[ref]
		if row.status == contract.InboundPending && !row.next.After(now) && len(out) < limit {
			out = append(out, row.rec)
		}
	}
	return out, nil
}

func (r *intakeRepo) MarkRegistered(_ context.Context, ref string, _ time.Time) error {
	r.rows[ref].status = contract.InboundRegistered
	return nil
}

func (r *intakeRepo) MarkRetry(_ context.Context, ref, msg string, next time.Time) error {
	row := r.rows[ref]
	row.rec.Attempts++
	row.lastError, row.next = msg, next
	return nil
}

func (r *intakeRepo) MarkRejected(_ context.Context, ref, msg string) error {
	r.rows[ref].status, r.rows[ref].lastError = contract.InboundRejected, msg
	return nil
}

func (r *intakeRepo) Cursor(_ context.Context, site shared.SiteId) (time.Time, error) {
	return r.cursors[site], nil
}

func (r *intakeRepo) AdvanceCursor(_ context.Context, site shared.SiteId, to time.Time) error {
	if to.After(r.cursors[site]) {
		r.cursors[site] = to
	}
	return nil
}

type gtinBook map[string]contract.GtinResolution

func (b gtinBook) ResolveGtin(_ context.Context, gtin string) (contract.GtinResolution, error) {
	if r, ok := b[gtin]; ok {
		return r, nil
	}
	return contract.GtinResolution{}, contract.ErrGtinNotFound
}

type recordingRegistrar struct {
	got []contract.AsnRegistration
	err error
}

func (r *recordingRegistrar) RegisterAsn(_ context.Context, a contract.AsnRegistration) error {
	r.got = append(r.got, a)
	return r.err
}

const (
	gEachA = "00614141000036"
	gEachB = "00614141000043"
	gCase  = "10614141000019"
)

func book() gtinBook {
	return gtinBook{
		gEachA: {GTIN: gEachA, SKU: "SKU-A", Level: "EACH", UnitsPerPack: 1, Active: true},
		gEachB: {GTIN: gEachB, SKU: "SKU-B", Level: "EACH", UnitsPerPack: 1, Active: true},
		gCase:  {GTIN: gCase, SKU: "SKU-A", Level: "CASE", UnitsPerPack: 12, Active: true},
	}
}

func shipment(ref string, at time.Time, cartons ...contract.InboundCarton) contract.InboundShipment {
	return contract.InboundShipment{Ref: ref, OwnerRef: "S-100", SiteId: "FC-1", ShippedAt: at, Cartons: cartons}
}

func carton(sscc, parent string, kind contract.CartonKind, contents ...contract.InboundContent) contract.InboundCarton {
	return contract.InboundCarton{SSCC: sscc, ParentSSCC: parent, Kind: kind, Contents: contents}
}

func content(gtin string, qty int64) contract.InboundContent {
	return contract.InboundContent{GTIN: gtin, QtyEaches: qty}
}

type intakeFixture struct {
	uc    *usecases.IntakeInboundShipments
	gw    *intakeGateway
	repo  *intakeRepo
	reg   *recordingRegistrar
	clock *movableClock
}

func newIntake(shipments ...contract.InboundShipment) *intakeFixture {
	gw := &intakeGateway{bySite: map[shared.SiteId][]contract.InboundShipment{"FC-1": shipments}}
	repo, reg, clock := newIntakeRepo(), &recordingRegistrar{}, &movableClock{t: intakeNow}
	return &intakeFixture{
		uc: &usecases.IntakeInboundShipments{
			Sites: []shared.SiteId{"FC-1"}, Gateway: gw, Shipments: repo, Resolver: book(), Registrar: reg, Clock: clock,
		},
		gw: gw, repo: repo, reg: reg, clock: clock,
	}
}

func TestIntake_RegistersTheAsnWithHierarchicalCartonsAndRolledUpLines(t *testing.T) {
	two := carton("S2", "", contract.CartonMixed, content(gEachB, 5), content(gEachA, 6))
	f := newIntake(shipment("SHP-1", intakeNow.Add(-time.Hour),
		carton("S1", "PAL", contract.CartonHomogeneousCase, content(gEachA, 12)), two))
	res, err := f.uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res != (usecases.IntakeResult{Polled: 1, New: 1, Registered: 1}) {
		t.Fatalf("result = %+v", res)
	}
	expectAsnShape(t, f.reg.got[0])
	if got := f.repo.rows["SHP-1"].status; got != contract.InboundRegistered {
		t.Errorf("status = %s", got)
	}
}

func expectAsnShape(t *testing.T, a contract.AsnRegistration) {
	t.Helper()
	if a.AsnNumber != "NF-SHP-1" || a.OwnerRef != "S-100" || a.SupplierRef != "S-100" {
		t.Errorf("asn header = %+v", a)
	}
	wantLines := []contract.AsnLine{{LineNo: 1, SKU: "SKU-A", ExpectedQty: 18}, {LineNo: 2, SKU: "SKU-B", ExpectedQty: 5}}
	if fmt.Sprint(a.Lines) != fmt.Sprint(wantLines) {
		t.Errorf("lines = %v, want %v (per-SKU roll-up in eaches, numbered in order of first appearance)", a.Lines, wantLines)
	}
	if c := a.Cartons[0]; c.SSCC != "S1" || c.ParentSSCC != "PAL" || c.Kind != contract.CartonHomogeneousCase || c.Contents[0].LineNo != 1 || c.Contents[0].SKU != "SKU-A" || c.Contents[0].Qty != 12 {
		t.Errorf("carton 0 = %+v", c)
	}
	if c := a.Cartons[1]; c.Contents[0].LineNo != 2 || c.Contents[1].LineNo != 1 {
		t.Errorf("mixed carton contents are tied to their lines: %+v", c.Contents)
	}
}

func TestIntake_AReplayOfThePollIsANoOp(t *testing.T) {
	f := newIntake(shipment("SHP-1", intakeNow.Add(-time.Hour), carton("S1", "", contract.CartonHomogeneousCase, content(gEachA, 12))))
	for range 3 {
		if _, err := f.uc.Execute(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if len(f.reg.got) != 1 {
		t.Errorf("registrations = %d, want 1: a registered shipment is not attempted again", len(f.reg.got))
	}
	res, _ := f.uc.Execute(context.Background())
	if res.Polled != 1 || res.New != 0 || res.Registered != 0 {
		t.Errorf("replay result = %+v", res)
	}
}

func TestIntake_TheCursorIsTheNewestShippedAtAndNeverMovesBack(t *testing.T) {
	older, newer := intakeNow.Add(-2*time.Hour), intakeNow.Add(-time.Hour)
	f := newIntake(
		shipment("A", newer, carton("S1", "", contract.CartonMixed, content(gEachA, 1))),
		shipment("B", older, carton("S2", "", contract.CartonMixed, content(gEachA, 1))),
	)
	if _, err := f.uc.Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.repo.cursors["FC-1"]; !got.Equal(newer) {
		t.Fatalf("cursor = %v, want %v", got, newer)
	}
	f.gw.bySite["FC-1"] = []contract.InboundShipment{shipment("C", older, carton("S3", "", contract.CartonMixed, content(gEachA, 1)))}
	if _, err := f.uc.Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := f.repo.cursors["FC-1"]; !got.Equal(newer) {
		t.Errorf("cursor moved back to %v", got)
	}
	if got := f.gw.since["FC-1"]; !got.Equal(newer) {
		t.Errorf("the poll was asked from %v, want the stored cursor %v", got, newer)
	}
}

func TestIntake_APollFailureKeepsTheCursorAndStillRetriesWhatIsDue(t *testing.T) {
	f := newIntake()
	f.repo.rows["OLD"] = &intakeRow{rec: contract.InboundRecord{Shipment: shipment("OLD", intakeNow, carton("S1", "", contract.CartonMixed, content(gEachA, 1))), AsnNumber: "NF-OLD"}, status: contract.InboundPending, next: intakeNow}
	f.repo.order = []string{"OLD"}
	f.gw.err = errors.New("network down")
	res, err := f.uc.Execute(context.Background())
	if err == nil || !strings.Contains(err.Error(), "network down") {
		t.Fatalf("err = %v, want the poll failure", err)
	}
	if res.Registered != 1 {
		t.Errorf("result = %+v: a poll failure must not stop the registration of recorded shipments", res)
	}
	if !f.repo.cursors["FC-1"].IsZero() {
		t.Errorf("cursor advanced to %v after a failed poll", f.repo.cursors["FC-1"])
	}
}

func TestIntake_ARecordFailureLeavesTheCursorSoTheShipmentComesBack(t *testing.T) {
	f := newIntake(shipment("A", intakeNow, carton("S1", "", contract.CartonMixed, content(gEachA, 1))))
	f.repo.recErr = errors.New("db down")
	if _, err := f.uc.Execute(context.Background()); err == nil {
		t.Fatal("want the record failure")
	}
	if !f.repo.cursors["FC-1"].IsZero() {
		t.Errorf("cursor = %v, want untouched", f.repo.cursors["FC-1"])
	}
}

func TestIntake_AnUnknownGtinIsARecordedFailureRetriedWithBackoffNeverAGuess(t *testing.T) {
	unknown := "00000000000000"
	f := newIntake(shipment("SHP-1", intakeNow.Add(-time.Hour), carton("S1", "", contract.CartonMixed, content(unknown, 3))))
	res, err := f.uc.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Retrying != 1 || res.Registered != 0 || len(f.reg.got) != 0 {
		t.Fatalf("result = %+v, registrations = %d", res, len(f.reg.got))
	}
	row := f.repo.rows["SHP-1"]
	if row.status != contract.InboundPending || !strings.Contains(row.lastError, unknown) || !strings.Contains(row.lastError, "GTIN not found") {
		t.Errorf("row = %+v", row)
	}
	if want := intakeNow.Add(time.Minute); !row.next.Equal(want) {
		t.Errorf("next attempt = %v, want %v (first backoff is a minute)", row.next, want)
	}
	// Not due yet: the next pass leaves it alone.
	if res, _ = f.uc.Execute(context.Background()); res.Retrying != 0 {
		t.Errorf("attempted before its backoff elapsed: %+v", res)
	}
}

func TestIntake_AShipmentWaitsForTheProductAndRegistersWhenItIsDeclared(t *testing.T) {
	unknown := "00000000000000"
	f := newIntake(shipment("SHP-1", intakeNow.Add(-time.Hour), carton("S1", "", contract.CartonMixed, content(unknown, 3))))
	books := book()
	f.uc.Resolver = books
	if _, err := f.uc.Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	books[unknown] = contract.GtinResolution{GTIN: unknown, SKU: "SKU-N", Level: "EACH", UnitsPerPack: 1, Active: true}
	f.clock.t = intakeNow.Add(time.Minute)
	res, err := f.uc.Execute(context.Background())
	if err != nil || res.Registered != 1 || f.repo.rows["SHP-1"].status != contract.InboundRegistered {
		t.Fatalf("result = %+v, %v", res, err)
	}
}

func TestIntake_RefusalsRejectOrRetryByTheirKind(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		status   contract.InboundStatus
		retrying int
		rejected int
	}{
		{"a verdict on the data parks the shipment", &contract.AsnRefusedError{Problem: "duplicate-sscc", Detail: "x"}, contract.InboundRejected, 0, 1},
		{"unknown-sku is retried: the projection may lag", &contract.AsnRefusedError{Problem: "unknown-sku", Retryable: true}, contract.InboundPending, 1, 0},
		{"a transport failure is retried", errors.New("connection refused"), contract.InboundPending, 1, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newIntake(shipment("SHP-1", intakeNow, carton("S1", "", contract.CartonMixed, content(gEachA, 1))))
			f.reg.err = tc.err
			res, err := f.uc.Execute(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if f.repo.rows["SHP-1"].status != tc.status || res.Retrying != tc.retrying || res.Rejected != tc.rejected {
				t.Errorf("status = %s, result = %+v", f.repo.rows["SHP-1"].status, res)
			}
		})
	}
}

func TestIntake_DataThatCanNeverBeRegisteredIsRejectedWithoutCallingInboundReceiving(t *testing.T) {
	cases := map[string]contract.InboundShipment{
		"no cartons":             shipment("E", intakeNow),
		"a case GTIN as content": shipment("C", intakeNow, carton("S1", "", contract.CartonHomogeneousCase, content(gCase, 12))),
	}
	for name, s := range cases {
		t.Run(name, func(t *testing.T) {
			f := newIntake(s)
			res, err := f.uc.Execute(context.Background())
			if err != nil || res.Rejected != 1 || len(f.reg.got) != 0 || f.repo.rows[s.Ref].status != contract.InboundRejected {
				t.Errorf("result = %+v, %v, registrations = %d", res, err, len(f.reg.got))
			}
		})
	}
}

func TestIntake_BatchSizeBoundsAPass(t *testing.T) {
	var ships []contract.InboundShipment
	for i := range 5 {
		ships = append(ships, shipment(fmt.Sprintf("S%d", i), intakeNow, carton(fmt.Sprintf("C%d", i), "", contract.CartonMixed, content(gEachA, 1))))
	}
	f := newIntake(ships...)
	f.uc.BatchSize = 2
	res, _ := f.uc.Execute(context.Background())
	if res.Registered != 2 {
		t.Errorf("registered = %d, want 2", res.Registered)
	}
	res, _ = f.uc.Execute(context.Background())
	if res.Registered != 2 {
		t.Errorf("second pass registered = %d, want 2", res.Registered)
	}
}

func TestIntakeBackoff(t *testing.T) {
	for attempts, want := range map[int]time.Duration{
		0: time.Minute, 1: time.Minute, 2: 2 * time.Minute, 3: 4 * time.Minute, 6: 32 * time.Minute, 7: time.Hour, 8: time.Hour, 50: time.Hour,
	} {
		if got := usecases.IntakeBackoff(attempts); got != want {
			t.Errorf("IntakeBackoff(%d) = %v, want %v", attempts, got, want)
		}
	}
}

func TestAsnNumberFor(t *testing.T) {
	long := strings.Repeat("x", 61)
	odd := "SHP/1 é"
	cases := map[string]string{"SHP-1001": "NF-SHP-1001", strings.Repeat("x", 60): "NF-" + strings.Repeat("x", 60)}
	for in, want := range cases {
		if got := usecases.AsnNumberFor(in); got != want {
			t.Errorf("AsnNumberFor(%q) = %q, want %q", in, got, want)
		}
	}
	for _, in := range []string{long, odd, ""} {
		got := usecases.AsnNumberFor(in)
		if len(got) != 43 || !strings.HasPrefix(got, "NF-") || got != usecases.AsnNumberFor(in) {
			t.Errorf("AsnNumberFor(%q) = %q: want NF- plus 40 hex digits, deterministic", in, got)
		}
	}
	if usecases.AsnNumberFor("a/b") == usecases.AsnNumberFor("a_b") {
		t.Error("two different references share an ASN number")
	}
}

func TestIntake_ASellerlessShipmentGetsASupplierRefAndNoOwner(t *testing.T) {
	s := shipment("SHP-1", intakeNow, carton("S1", "", contract.CartonMixed, content(gEachA, 1)))
	s.OwnerRef = ""
	f := newIntake(s)
	if _, err := f.uc.Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	if a := f.reg.got[0]; a.SupplierRef != "NETWORK" || a.OwnerRef != "" {
		t.Errorf("asn = %+v", a)
	}
}
