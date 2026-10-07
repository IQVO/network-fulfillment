package cloudevents_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/kafka/cloudevents"
)

func TestType_BuildsFleetConvention(t *testing.T) {
	got := cloudevents.Type("networkorder", "NetworkOrderReceived")
	want := "com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderReceived"
	if got != want {
		t.Fatalf("Type = %q, want %q", got, want)
	}
}

func TestType_VersionedAddsSuffixOnlyFromV2(t *testing.T) {
	const base = "com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderAcknowledged"
	if got := cloudevents.TypeVersioned("networkorder", "NetworkOrderAcknowledged", 1); got != base {
		t.Fatalf("v1 type = %q, want the unsuffixed %q", got, base)
	}
	if got := cloudevents.TypeVersioned("networkorder", "NetworkOrderAcknowledged", 2); got != base+".v2" {
		t.Fatalf("v2 type = %q, want %q", got, base+".v2")
	}
}

func TestNew_V2CarriesDotV2TypeAndV2Dataschema(t *testing.T) {
	raw, err := cloudevents.New(cloudevents.Spec{ID: "id-1", Entity: "networkorder", EventName: "NetworkOrderAcknowledged",
		Subject: "po-1", Time: time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC), Stream: cloudevents.StreamAnalytics, Version: 2, Data: map[string]any{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	e, err := cloudevents.Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if e.Type() != "com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderAcknowledged.v2" {
		t.Fatalf("type = %q", e.Type())
	}
	if e.DataSchema() != "urn:warehouse:network-fulfillment:analytics:NetworkOrderAcknowledged:v2" {
		t.Fatalf("dataschema = %q", e.DataSchema())
	}
}

func TestDataSchema_BuildsURN(t *testing.T) {
	got := cloudevents.DataSchema(cloudevents.StreamAnalytics, "NetworkOrderRejected", 1)
	want := "urn:warehouse:network-fulfillment:analytics:NetworkOrderRejected:v1"
	if got != want {
		t.Fatalf("DataSchema = %q, want %q", got, want)
	}
}

func TestNew_ProducesExactStructuredJSON(t *testing.T) {
	at := time.Date(2026, 9, 30, 12, 0, 0, 0, time.FixedZone("BRT", -3*3600))
	raw, err := cloudevents.New(cloudevents.Spec{
		ID:        "11111111-1111-4111-8111-111111111111",
		Entity:    "networkorder",
		EventName: "NetworkOrderReceived",
		Subject:   "po-1",
		Time:      at,
		Stream:    cloudevents.StreamEvents,
		Version:   1,
		Data:      map[string]any{"networkRef": "po-1"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	want := `{"specversion":"1.0","id":"11111111-1111-4111-8111-111111111111","source":"/warehouse/network-fulfillment","type":"com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderReceived","subject":"po-1","datacontenttype":"application/json","dataschema":"urn:warehouse:network-fulfillment:events:NetworkOrderReceived:v1","time":"2026-09-30T15:00:00Z","data":{"networkRef":"po-1"}}`
	assertJSONEqual(t, raw, want)
}

func TestNew_DefaultsVersionToOne(t *testing.T) {
	raw, err := cloudevents.New(cloudevents.Spec{ID: "id-1", Entity: "networkorder", EventName: "X", Subject: "s", Time: time.Now(), Stream: cloudevents.StreamEvents, Data: map[string]any{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	e, err := cloudevents.Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if e.DataSchema() != "urn:warehouse:network-fulfillment:events:X:v1" {
		t.Fatalf("dataschema = %q", e.DataSchema())
	}
}

func TestNew_RejectsEmptySubject(t *testing.T) {
	if _, err := cloudevents.New(cloudevents.Spec{ID: "id", Entity: "networkorder", EventName: "X", Time: time.Now(), Stream: cloudevents.StreamEvents}); err == nil {
		t.Fatal("expected an error for an empty subject")
	}
}

func TestNew_RejectsEmptyID(t *testing.T) {
	if _, err := cloudevents.New(cloudevents.Spec{Entity: "networkorder", EventName: "X", Subject: "s", Time: time.Now(), Stream: cloudevents.StreamEvents}); err == nil {
		t.Fatal("expected an error for an empty id")
	}
}

func TestNew_RejectsUnmarshalableData(t *testing.T) {
	if _, err := cloudevents.New(cloudevents.Spec{ID: "id", Entity: "networkorder", EventName: "X", Subject: "s", Time: time.Now(), Stream: cloudevents.StreamEvents, Data: func() {}}); err == nil {
		t.Fatal("expected an error for unmarshalable data")
	}
}

func TestContentTypeHeader(t *testing.T) {
	h := cloudevents.ContentTypeHeader()
	if h.Key != "content-type" || string(h.Value) != "application/cloudevents+json; charset=UTF-8" {
		t.Fatalf("header = %s: %s", h.Key, h.Value)
	}
}

func TestDecode_RoundTrip(t *testing.T) {
	at := time.Date(2026, 9, 30, 15, 0, 0, 0, time.UTC)
	raw, err := cloudevents.New(cloudevents.Spec{ID: "id-1", Entity: "networkorder", EventName: "NetworkOrderRejected", Subject: "po-9", Time: at, Stream: cloudevents.StreamAnalytics, Version: 1, Data: map[string]string{"reason": "R"}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	e, err := cloudevents.Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if e.ID() != "id-1" || e.Subject() != "po-9" || !e.Time().Equal(at) || e.Source() != cloudevents.Source {
		t.Fatalf("unexpected attributes: %+v", e)
	}
	var data map[string]string
	if err := e.DataAs(&data); err != nil || data["reason"] != "R" {
		t.Fatalf("DataAs = %v, %v", data, err)
	}
}

func TestDecode_RejectsLegacyAndInvalidMessages(t *testing.T) {
	cases := map[string]string{
		"legacy flat envelope": `{"event_id":"e1","event_type":"NetworkOrderReceived","occurred_at":"2026-09-30T15:00:00Z","source":"network-fulfillment","data":{}}`,
		"legacy analytics v1":  `{"event_id":"e1","event_type":"NetworkOrderReceived","occurred_at":"2026-09-30T15:00:00Z","source":"network-fulfillment","schema_version":1,"data":{}}`,
		"not json":             `not json`,
		"wrong specversion":    `{"specversion":"0.3","id":"1","source":"/x","type":"t"}`,
		"missing id":           `{"specversion":"1.0","source":"/x","type":"t"}`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := cloudevents.Decode([]byte(raw))
			if !errors.Is(err, cloudevents.ErrNotCloudEvent) {
				t.Fatalf("err = %v, want ErrNotCloudEvent", err)
			}
		})
	}
}

func assertJSONEqual(t *testing.T, got []byte, want string) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("unmarshal got: %v", err)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("unmarshal want: %v", err)
	}
	gb, _ := json.Marshal(g)
	wb, _ := json.Marshal(w)
	if string(gb) != string(wb) {
		t.Fatalf("JSON mismatch\n got: %s\nwant: %s", gb, wb)
	}
}
