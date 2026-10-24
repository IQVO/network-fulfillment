package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	ce "github.com/cloudevents/sdk-go/v2/event"
)

type projectionSpy struct {
	cartons  []contract.ReceivedCartonFact
	findings []contract.CartonFinding
	closed   []contract.ReceiptClosedFact
	ids      []string
	err      error
	closedAt time.Time
}

func (p *projectionSpy) RecordCartonReceived(_ context.Context, id string, f contract.ReceivedCartonFact) error {
	p.ids = append(p.ids, id)
	p.cartons = append(p.cartons, f)
	return p.err
}

func (p *projectionSpy) RecordFinding(_ context.Context, id string, f contract.CartonFinding) error {
	p.ids = append(p.ids, id)
	p.findings = append(p.findings, f)
	return p.err
}

func (p *projectionSpy) RecordClosed(_ context.Context, id string, f contract.ReceiptClosedFact, now time.Time) error {
	p.ids = append(p.ids, id)
	p.closed = append(p.closed, f)
	p.closedAt = now
	return p.err
}

func (p *projectionSpy) DueReceipts(context.Context, time.Time, int) ([]contract.ReceiptFacts, error) {
	return nil, nil
}
func (p *projectionSpy) MarkReceiptSent(context.Context, string, time.Time) error { return nil }
func (p *projectionSpy) MarkReceiptRetry(context.Context, string, string, time.Time) error {
	return nil
}
func (p *projectionSpy) MarkReceiptRejected(context.Context, string, string) error { return nil }

// event builds a structured CloudEvents message the way inbound-receiving does
// (snake_case data, its own type and source).
func event(t *testing.T, id, typ string, data any) []byte {
	t.Helper()
	e := ce.New("1.0")
	e.SetID(id)
	e.SetSource("/warehouse/inbound-receiving")
	e.SetType(typ)
	e.SetSubject("rcpt-1")
	e.SetTime(time.Date(2026, 10, 25, 9, 30, 0, 0, time.UTC))
	if err := e.SetData("application/json", data); err != nil {
		t.Fatal(err)
	}
	raw, err := e.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func newSpyConsumer() (*ReceiptConsumer, *projectionSpy) {
	spy := &projectionSpy{}
	return &ReceiptConsumer{Projection: spy, Now: func() time.Time { return time.Date(2026, 10, 25, 10, 0, 0, 0, time.UTC) }}, spy
}

func TestReceiptConsumer_ProjectsTheThreeEventTypes(t *testing.T) {
	c, spy := newSpyConsumer()
	ctx := context.Background()
	if err := c.Handle(ctx, "ASN-1", event(t, "e1", TypeReceiptCartonReceived, map[string]any{
		"receipt_id": "rcpt-1", "asn_number": "ASN-1", "sscc": "006141410000000012", "sealed": true, "lpn": "006141410000000012",
		"contents":    []map[string]any{{"line_no": 1, "sku": "S", "gtin": "00614141000036", "qty": 12, "condition": "Damaged"}},
		"received_at": "2026-10-25T09:10:00Z",
	})); err != nil {
		t.Fatal(err)
	}
	if err := c.Handle(ctx, "ASN-1", event(t, "e2", TypeCartonDiscrepancyRaised, map[string]any{
		"receipt_id": "rcpt-1", "sscc": "006141410000000043", "kind": "UnexpectedCarton", "expected": "x", "actual": "y", "at": "2026-10-25T09:20:00Z",
	})); err != nil {
		t.Fatal(err)
	}
	if err := c.Handle(ctx, "ASN-1", event(t, "e3", TypeReceiptClosed, map[string]any{
		"receipt_id": "rcpt-1", "asn_number": "ASN-1", "closed_at": "2026-10-25T09:30:00Z", "discrepancies": []any{},
	})); err != nil {
		t.Fatal(err)
	}
	expectProjected(t, spy, c.Now())
}

func expectProjected(t *testing.T, spy *projectionSpy, now time.Time) {
	t.Helper()
	expectProjectedCarton(t, spy)
	expectProjectedFinding(t, spy)
	expectProjectedClose(t, spy, now)
	if got := spy.ids; len(got) != 3 || got[0] != "e1" || got[1] != "e2" || got[2] != "e3" {
		t.Errorf("claimed ids = %v: the CloudEvents id is the dedupe key", got)
	}
}

func expectProjectedCarton(t *testing.T, spy *projectionSpy) {
	t.Helper()
	if len(spy.cartons) != 1 {
		t.Fatalf("cartons = %+v", spy.cartons)
	}
	c := spy.cartons[0]
	if c.AsnNumber != "ASN-1" || c.SSCC != "006141410000000012" || c.ReceiptID != "rcpt-1" {
		t.Errorf("carton = %+v", c)
	}
	if c.Contents[0] != (contract.ReceivedContent{GTIN: "00614141000036", Qty: 12, Condition: "Damaged"}) {
		t.Errorf("carton contents = %+v", c.Contents)
	}
}

func expectProjectedFinding(t *testing.T, spy *projectionSpy) {
	t.Helper()
	if len(spy.findings) != 1 {
		t.Fatalf("findings = %+v", spy.findings)
	}
	if f := spy.findings[0]; f.AsnNumber != "ASN-1" || f.Kind != "UnexpectedCarton" || f.SSCC != "006141410000000043" {
		t.Errorf("finding = %+v (the ASN is the Kafka key: the event data has none)", f)
	}
}

func expectProjectedClose(t *testing.T, spy *projectionSpy, now time.Time) {
	t.Helper()
	if len(spy.closed) != 1 {
		t.Fatalf("closed = %+v", spy.closed)
	}
	if c := spy.closed[0]; c.AsnNumber != "ASN-1" || !c.ClosedAt.Equal(time.Date(2026, 10, 25, 9, 30, 0, 0, time.UTC)) {
		t.Errorf("closed = %+v", c)
	}
	if spy.closedAt != now {
		t.Errorf("claimed at %v, want the consumer's clock %v", spy.closedAt, now)
	}
}

func TestReceiptConsumer_TheKafkaKeyStandsInWhenTheDataHasNoAsn(t *testing.T) {
	c, spy := newSpyConsumer()
	if err := c.Handle(context.Background(), "ASN-K", event(t, "e1", TypeReceiptClosed, map[string]any{"receipt_id": "r", "closed_at": "2026-10-25T09:30:00Z"})); err != nil {
		t.Fatal(err)
	}
	if spy.closed[0].AsnNumber != "ASN-K" {
		t.Errorf("asn = %q", spy.closed[0].AsnNumber)
	}
}

func TestReceiptConsumer_ALabellessCartonIsKeyedByItsLpn(t *testing.T) {
	c, spy := newSpyConsumer()
	if err := c.Handle(context.Background(), "ASN-1", event(t, "e1", TypeReceiptCartonReceived, map[string]any{
		"receipt_id": "r", "asn_number": "ASN-1", "lpn": "LPN-9", "contents": []any{}, "received_at": "2026-10-25T09:10:00Z"})); err != nil {
		t.Fatal(err)
	}
	if spy.cartons[0].SSCC != "" || spy.cartons[0].LPN != "LPN-9" {
		t.Errorf("carton = %+v", spy.cartons[0])
	}
}

func TestReceiptConsumer_OtherTypesAreIgnoredAndBadEventsAreRefused(t *testing.T) {
	c, spy := newSpyConsumer()
	ctx := context.Background()
	if err := c.Handle(ctx, "ASN-1", event(t, "e1", "com.warehouse.wms.inbound-receiving.asn.ASNRegistered", map[string]any{"asn_number": "ASN-1"})); err != nil || len(spy.ids) != 0 {
		t.Errorf("an unknown type must be skipped: %v %v", err, spy.ids)
	}
	if err := c.Handle(ctx, "ASN-1", []byte(`{"flat":"envelope"}`)); !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Errorf("err = %v, want ErrNotCloudEvent", err)
	}
	for name, raw := range map[string][]byte{
		"carton without a carton":    event(t, "e2", TypeReceiptCartonReceived, map[string]any{"receipt_id": "r", "asn_number": "A"}),
		"finding without a kind":     event(t, "e3", TypeCartonDiscrepancyRaised, map[string]any{"receipt_id": "r"}),
		"closed without an asn":      event(t, "e4", TypeReceiptClosed, map[string]any{"receipt_id": "r"}),
		"data that is not an object": event(t, "e5", TypeReceiptClosed, "text"),
	} {
		key := "A"
		if name == "closed without an asn" {
			key = ""
		}
		if err := c.Handle(ctx, key, raw); !errors.Is(err, errBadReceiptEvent) {
			t.Errorf("%s: err = %v, want errBadReceiptEvent", name, err)
		}
	}
	if len(spy.ids) != 0 {
		t.Errorf("a refused event reached the projection: %v", spy.ids)
	}
}

func TestReceiptConsumer_AProjectionFailureIsReturnedSoTheMessageIsRetried(t *testing.T) {
	c, spy := newSpyConsumer()
	spy.err = errors.New("db down")
	err := c.Handle(context.Background(), "A", event(t, "e1", TypeReceiptClosed, map[string]any{"receipt_id": "r", "asn_number": "A", "closed_at": "2026-10-25T09:30:00Z"}))
	if !errors.Is(err, spy.err) {
		t.Fatalf("err = %v", err)
	}
}
