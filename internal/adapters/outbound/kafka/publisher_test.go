package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// fakeWriter captures the messages handed to WriteMessages so a test can
// assert on the published envelope without a live broker.
type fakeWriter struct {
	msgs []kafkago.Message
	err  error
}

func (w *fakeWriter) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	if w.err != nil {
		return w.err
	}
	w.msgs = append(w.msgs, msgs...)
	return nil
}

// goldenCase is one published event type: the domain event plus the exact
// CloudEvents JSON (all §3 attributes + data) it must serialize to on the
// given stream. typeSuffix/version are the wire versioning (ADR 0016):
// "" / 1 for every v1 type, ".v2" / 2 for NetworkOrderAcknowledged.
type goldenCase struct {
	name       string
	event      shared.DomainEvent
	data       string // exact JSON of the `data` member (payload shape unchanged)
	typeSuffix string
	version    int
}

func goldenCases(at time.Time) []goldenCase {
	return []goldenCase{
		{
			name: "NetworkOrderReceived",
			event: shared.NetworkOrderReceived{NetworkRef: "po-1", SiteId: "site-1", RequiredShipBy: at.Add(48 * time.Hour),
				AcknowledgeBy: at.Add(24 * time.Hour), LineCount: 2, At: at},
			data:    `{"networkRef":"po-1","siteId":"site-1","requiredShipBy":"2026-09-25T08:00:00Z","acknowledgeBy":"2026-09-24T08:00:00Z","lineCount":2,"at":"2026-09-23T08:00:00Z"}`,
			version: 1,
		},
		{
			name:    "NetworkOrderSubmitted",
			event:   shared.NetworkOrderSubmitted{NetworkRef: "po-1", SiteId: "site-1", LocalOrderId: "ord-1", ReceivedAt: at.Add(-time.Minute), At: at},
			data:    `{"networkRef":"po-1","siteId":"site-1","localOrderId":"ord-1","receivedAt":"2026-09-23T07:59:00Z","at":"2026-09-23T08:00:00Z"}`,
			version: 1,
		},
		{
			// Breaking change of meaning (ADR 0016): the settle, published as v2.
			name:       "NetworkOrderAcknowledged",
			event:      shared.NetworkOrderAcknowledged{NetworkRef: "po-1", SiteId: "site-1", LocalOrderId: "ord-1", ReceivedAt: at.Add(-time.Minute), At: at},
			data:       `{"networkRef":"po-1","siteId":"site-1","localOrderId":"ord-1","receivedAt":"2026-09-23T07:59:00Z","at":"2026-09-23T08:00:00Z"}`,
			typeSuffix: ".v2",
			version:    2,
		},
		{
			name:    "NetworkOrderRejected",
			event:   shared.NetworkOrderRejected{NetworkRef: "po-1", SiteId: "site-1", Reason: shared.RejectionReasonUntranslatableSKU, At: at},
			data:    `{"networkRef":"po-1","siteId":"site-1","reason":"UNTRANSLATABLE_SKU","at":"2026-09-23T08:00:00Z"}`,
			version: 1,
		},
		{
			name:    "NetworkOrderShipmentConfirmed",
			event:   shared.NetworkOrderShipmentConfirmed{NetworkRef: "po-1", SiteId: "site-1", LocalOrderId: "ord-1", At: at},
			data:    `{"networkRef":"po-1","siteId":"site-1","localOrderId":"ord-1","at":"2026-09-23T08:00:00Z"}`,
			version: 1,
		},
		{
			name:    "AcknowledgementDeadlineAtRisk",
			event:   shared.AcknowledgementDeadlineAtRisk{NetworkRef: "po-1", SiteId: "site-1", AcknowledgeBy: at.Add(-time.Hour), At: at},
			data:    `{"networkRef":"po-1","siteId":"site-1","acknowledgeBy":"2026-09-23T07:00:00Z","at":"2026-09-23T08:00:00Z"}`,
			version: 1,
		},
	}
}

// wantCloudEvent is the exact structured-mode JSON a published event must
// have: every required attribute, the full type string (with its `.vN`
// suffix for a versioned type), the dataschema for stream, and the
// unchanged payload.
func wantCloudEvent(eventName, typeSuffix, stream string, version int, data string) string {
	return `{"specversion":"1.0","id":"11111111-1111-4111-8111-111111111111",` +
		`"source":"/warehouse/network-fulfillment",` +
		`"type":"com.warehouse.wes.network-fulfillment.networkorder.` + eventName + typeSuffix + `",` +
		`"subject":"po-1","time":"2026-09-23T08:00:00Z","datacontenttype":"application/json",` +
		`"dataschema":"urn:warehouse:network-fulfillment:` + stream + `:` + eventName + `:v` + strconv.Itoa(version) + `",` +
		`"data":` + data + `}`
}

const fixedID = "11111111-1111-4111-8111-111111111111"

// assertGoldenMessage checks one produced message against the golden
// CloudEvent, key and content-type header.
func assertGoldenMessage(t *testing.T, msg kafkago.Message, wantTopic, want string) {
	t.Helper()
	if msg.Topic != wantTopic {
		t.Errorf("topic = %q, want %q", msg.Topic, wantTopic)
	}
	if string(msg.Key) != "po-1" {
		t.Errorf("key = %q, want po-1", string(msg.Key))
	}
	assertJSONEqual(t, msg.Value, want)
	if got := headerValue(msg.Headers, "content-type"); got != "application/cloudevents+json; charset=UTF-8" {
		t.Errorf("content-type header = %q", got)
	}
}

func TestPublisher_GoldenCloudEventPerEventType(t *testing.T) {
	at := time.Date(2026, 9, 23, 8, 0, 0, 0, time.UTC)
	for _, tt := range goldenCases(at) {
		t.Run(tt.name, func(t *testing.T) {
			w := &fakeWriter{}
			p := outboundkafka.NewPublisher(nil, func() string { return fixedID })
			p.Writer = w
			if err := p.Publish(context.Background(), tt.event); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			if len(w.msgs) != 1 {
				t.Fatalf("expected 1 message, got %d", len(w.msgs))
			}
			assertGoldenMessage(t, w.msgs[0], outboundkafka.Topic, wantCloudEvent(tt.name, tt.typeSuffix, "events", tt.version, tt.data))
		})
	}
}

// TestPublisher_AcknowledgedIsV2_SubmittedIsV1 pins the ADR 0016
// versioning literally per the fleet CloudEvents standard (§4): the
// changed-meaning NetworkOrderAcknowledged is a NEW `.v2` type with a
// `:v2` dataschema, NetworkOrderSubmitted is new and therefore v1, and
// the Encoded.EventType the outbox persists carries the same `.v2` type.
func TestPublisher_AcknowledgedIsV2_SubmittedIsV1(t *testing.T) {
	const base = "com.warehouse.wes.network-fulfillment.networkorder."
	at := time.Date(2026, 9, 23, 8, 0, 0, 0, time.UTC)
	for _, enc := range []outboundkafka.Encoder{
		outboundkafka.NewPublisher(nil, func() string { return fixedID }),
		outboundkafka.NewAnalyticsPublisher(nil, func() string { return fixedID }),
	} {
		out, err := enc.Encode(context.Background(),
			shared.NetworkOrderSubmitted{NetworkRef: "po-1", At: at},
			shared.NetworkOrderAcknowledged{NetworkRef: "po-1", At: at})
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		if out[0].EventType != base+"NetworkOrderSubmitted" {
			t.Errorf("Submitted EventType = %q, want the v1 type", out[0].EventType)
		}
		if out[1].EventType != base+"NetworkOrderAcknowledged.v2" {
			t.Errorf("Acknowledged EventType = %q, want the .v2 type", out[1].EventType)
		}
		for i, wantSchemaSuffix := range []string{":NetworkOrderSubmitted:v1", ":NetworkOrderAcknowledged:v2"} {
			var env struct {
				Type       string `json:"type"`
				DataSchema string `json:"dataschema"`
			}
			if err := json.Unmarshal(out[i].Value, &env); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if env.Type != out[i].EventType {
				t.Errorf("wire type %q != Encoded.EventType %q", env.Type, out[i].EventType)
			}
			if !strings.HasSuffix(env.DataSchema, wantSchemaSuffix) {
				t.Errorf("dataschema = %q, want suffix %q", env.DataSchema, wantSchemaSuffix)
			}
		}
	}
}

// TestPublisher_EncodeMintsIDOnceAndStampsFullType asserts the Encoded
// metadata the outbox persists: the full CloudEvents type, and an id
// minted exactly once per event (so the persisted bytes — and every
// redelivery of them — carry that one id).
func TestPublisher_EncodeMintsIDOnceAndStampsFullType(t *testing.T) {
	calls := 0
	p := outboundkafka.NewPublisher(nil, func() string { calls++; return fixedID })
	enc, err := p.Encode(context.Background(), shared.NetworkOrderReceived{NetworkRef: "po-1", At: time.Now()})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if calls != 1 {
		t.Fatalf("NewId called %d times, want 1", calls)
	}
	if enc[0].EventType != "com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderReceived" {
		t.Fatalf("EventType = %q", enc[0].EventType)
	}
}

func TestPublisher_RejectsEventWithEmptySubject(t *testing.T) {
	p := outboundkafka.NewPublisher(nil, func() string { return fixedID })
	w := &fakeWriter{}
	p.Writer = w
	if err := p.Publish(context.Background(), shared.NetworkOrderReceived{At: time.Now()}); err == nil {
		t.Fatal("expected an error for an empty subject (NetworkRef)")
	}
	if len(w.msgs) != 0 {
		t.Fatalf("expected no message written, got %d", len(w.msgs))
	}
}

func headerValue(headers []kafkago.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func assertJSONEqual(t *testing.T, got []byte, want string) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("unmarshal got %s: %v", got, err)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("unmarshal want: %v", err)
	}
	gb, _ := json.Marshal(g)
	wb, _ := json.Marshal(w)
	if string(gb) != string(wb) {
		t.Errorf("JSON mismatch\n got: %s\nwant: %s", gb, wb)
	}
}

func TestPublisher_PropagatesWriteError(t *testing.T) {
	boom := errors.New("broker down")
	p := outboundkafka.NewPublisher(nil, func() string { return "evt" })
	p.Writer = &fakeWriter{err: boom}

	err := p.Publish(context.Background(), shared.NetworkOrderReceived{NetworkRef: "po-1", At: time.Now()})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want wrapped broker down", err)
	}
}

// TestPublisher_RejectsNonDomainEvent asserts a value that does not
// implement shared.DomainEvent is refused rather than published without a
// usable type/time.
func TestPublisher_RejectsNonDomainEvent(t *testing.T) {
	p := outboundkafka.NewPublisher(nil, func() string { return "evt" })
	w := &fakeWriter{}
	p.Writer = w

	err := p.Publish(context.Background(), "not a domain event")
	if err == nil {
		t.Fatal("expected an error for a non-DomainEvent value")
	}
	if len(w.msgs) != 0 {
		t.Fatalf("expected no message written, got %d", len(w.msgs))
	}
}

// TestPublisher_Close verifies Close delegates to a *kafkago.Writer when
// one is in use, and is a no-op over a fake.
func TestPublisher_Close(t *testing.T) {
	p := outboundkafka.NewPublisher([]string{"localhost:9092"}, func() string { return "evt" })
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	fake := outboundkafka.NewPublisher(nil, func() string { return "evt" })
	fake.Writer = &fakeWriter{}
	if err := fake.Close(); err != nil {
		t.Fatalf("Close over fake writer: %v", err)
	}
}
