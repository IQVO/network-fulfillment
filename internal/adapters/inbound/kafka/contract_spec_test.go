package kafka

// Shared support for the event consumer contract tests (E1-E4), which
// validate this repo's inbound Kafka edges against PINNED copies of the
// producing side's AsyncAPI spec (contracts/pinned.json; the
// pinned-contracts CI job fails when a copy drifts from the producer's
// develop). The pinned copies are byte-for-byte producer files: never
// edit them — refresh = copy the producer's file over the pin.

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	ce "github.com/cloudevents/sdk-go/v2/event"
	"gopkg.in/yaml.v3"

	"github.com/claudioed/network-fulfillment/internal/adapters/kafka/cloudevents"
)

// The pinned spec copies next to this consuming adapter.
const (
	pinnedInboundReceivingSpec = "testdata/inbound-receiving.asyncapi.yaml"
	pinnedOutboundShippingSpec = "testdata/outbound-shipping.asyncapi.yaml"
	pinnedAnalyticsSpec        = "testdata/network-fulfillment.asyncapi.yaml"
)

// pinnedAsyncAPI is the minimal slice of AsyncAPI 2.6 the contract tests
// read: channels (with their operations) and the components/messages the
// channel message $refs point at.
type pinnedAsyncAPI struct {
	AsyncAPI           string                   `yaml:"asyncapi"`
	DefaultContentType string                   `yaml:"defaultContentType"`
	Channels           map[string]pinnedChannel `yaml:"channels"`
	Components         pinnedComponents         `yaml:"components"`
}

type pinnedChannel struct {
	Subscribe *pinnedOperation `yaml:"subscribe"`
	Publish   *pinnedOperation `yaml:"publish"`
}

type pinnedOperation struct {
	OperationID string            `yaml:"operationId"`
	Message     *pinnedMessageRef `yaml:"message"`
}

// pinnedMessageRef is either a single $ref or a oneOf list of $refs.
type pinnedMessageRef struct {
	Ref   string             `yaml:"$ref"`
	OneOf []pinnedMessageRef `yaml:"oneOf"`
}

type pinnedComponents struct {
	Messages map[string]pinnedMessage `yaml:"messages"`
}

type pinnedMessage struct {
	Name        string          `yaml:"name"`
	ContentType string          `yaml:"contentType"`
	Examples    []pinnedExample `yaml:"examples"`
}

// pinnedExample is a message example: a full wire payload (a structured
// CloudEvents JSON document, as YAML) plus its Kafka headers.
type pinnedExample struct {
	Name    string            `yaml:"name"`
	Headers map[string]string `yaml:"headers"`
	Payload map[string]any    `yaml:"payload"`
}

// loadPinnedAsyncAPI reads and parses one pinned spec copy.
func loadPinnedAsyncAPI(t *testing.T, path string) *pinnedAsyncAPI {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read pinned spec %s (copy it from the producer, never hand-write it): %v", path, err)
	}
	var doc pinnedAsyncAPI
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse pinned spec %s: %v", path, err)
	}
	return &doc
}

// producerMessageRefs returns the components/messages names a topic's
// operation documents. Fleet convention is channel-perspective (a
// producer's OWN topics under 'subscribe:'), but it is NOT uniform —
// process-path-management documents its own topics under 'publish:' — so
// the pinned spec is read as the payload catalog for the topic: whichever
// of the two operations carries the message list is the producer's.
func (d *pinnedAsyncAPI) producerMessageRefs(t *testing.T, topic string) []string {
	t.Helper()
	ch, ok := d.Channels[topic]
	if !ok {
		t.Fatalf("pinned spec has no channel for topic %q", topic)
	}
	op := ch.Subscribe
	if op == nil {
		op = ch.Publish
	}
	if op == nil || op.Message == nil {
		t.Fatalf("pinned spec documents topic %q without a message-bearing operation", topic)
	}
	var refs []pinnedMessageRef
	if op.Message.Ref != "" {
		refs = append(refs, *op.Message)
	}
	refs = append(refs, op.Message.OneOf...)
	if len(refs) == 0 {
		t.Fatalf("pinned spec's operation on %q lists no messages", topic)
	}
	names := make([]string, 0, len(refs))
	for _, r := range refs {
		names = append(names, refName(t, r.Ref))
	}
	return names
}

// message resolves one components/messages entry, failing on a dangling
// $ref (a spec whose channel points at a message it does not define).
func (d *pinnedAsyncAPI) message(t *testing.T, name string) pinnedMessage {
	t.Helper()
	msg, ok := d.Components.Messages[name]
	if !ok {
		t.Fatalf("pinned spec references undefined components/messages/%s", name)
	}
	return msg
}

// documentedTypes maps every CloudEvents type a topic's message examples
// carry to the message that documents it. The examples are real wire
// documents the producer committed to, which makes them a stronger E1
// anchor than any schema-declared const.
func (d *pinnedAsyncAPI) documentedTypes(t *testing.T, topic string) map[string]string {
	t.Helper()
	types := map[string]string{}
	for _, name := range d.producerMessageRefs(t, topic) {
		msg := d.message(t, name)
		if len(msg.Examples) == 0 {
			t.Fatalf("pinned spec documents %s on %s without examples", name, topic)
		}
		for _, ex := range msg.Examples {
			if typ, _ := ex.Payload["type"].(string); typ != "" {
				types[typ] = name
			}
		}
	}
	if len(types) == 0 {
		t.Fatalf("pinned spec's examples on %s carry no CloudEvents types", topic)
	}
	return types
}

// goldenExamples returns a message's pinned examples.
func (d *pinnedAsyncAPI) goldenExamples(t *testing.T, name string) []pinnedExample {
	t.Helper()
	msg := d.message(t, name)
	if len(msg.Examples) == 0 {
		t.Fatalf("pinned spec documents %s without examples", name)
	}
	return msg.Examples
}

// refName extracts the last path segment of a local $ref
// ("#/components/messages/TaskCompleted" -> "TaskCompleted").
func refName(t *testing.T, ref string) string {
	t.Helper()
	i := strings.LastIndex(ref, "/")
	if i < 0 || i == len(ref)-1 {
		t.Fatalf("malformed local $ref %q", ref)
	}
	return ref[i+1:]
}

// goldenJSON marshals a pinned example's YAML-decoded payload into the
// JSON bytes a Kafka consumer actually receives (structured-mode
// CloudEvents: the message value IS the JSON envelope).
func goldenJSON(t *testing.T, payload map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal golden payload: %v", err)
	}
	return raw
}

// goldenKey returns the example's Kafka key: the fleet convention is
// key == the CloudEvents subject.
func goldenKey(payload map[string]any) string {
	key, _ := payload["subject"].(string)
	return key
}

// assertFleetEnvelope checks the CloudEvents 1.0 invariants every golden
// example must carry (E3): specversion 1.0, the identity attributes,
// datacontenttype application/json, and the producer's urn: dataschema —
// plus the structured-mode content-type when the example carries Kafka
// headers.
func assertFleetEnvelope(t *testing.T, ex pinnedExample, evt ce.Event, source, dataschema string) {
	t.Helper()
	if evt.SpecVersion() != cloudevents.SpecVersion {
		t.Errorf("%s: specversion %q, want %q", ex.Name, evt.SpecVersion(), cloudevents.SpecVersion)
	}
	if evt.ID() == "" || evt.Subject() == "" || evt.Source() == "" || evt.Time().IsZero() {
		t.Errorf("%s: id/source/subject/time must all be present (got %q/%q/%q/%v)", ex.Name, evt.ID(), evt.Source(), evt.Subject(), evt.Time())
	}
	if evt.Source() != source {
		t.Errorf("%s: source %q, want %q", ex.Name, evt.Source(), source)
	}
	if evt.DataMediaType() != cloudevents.DataContentType {
		t.Errorf("%s: datacontenttype %q, want %q", ex.Name, evt.DataMediaType(), cloudevents.DataContentType)
	}
	if dataschema != "" && evt.DataSchema() != dataschema {
		t.Errorf("%s: dataschema %q, want %q", ex.Name, evt.DataSchema(), dataschema)
	}
	if got := ex.Headers["content-type"]; got != "" && !strings.HasPrefix(got, "application/cloudevents+json") {
		t.Errorf("%s: Kafka content-type header %q, want application/cloudevents+json", ex.Name, got)
	}
}

// clonePayload deep-copies a golden example payload so a test can mutate
// it without touching the shared fixture.
func clonePayload(p map[string]any) map[string]any {
	return cloneMap(p).(map[string]any)
}

func cloneMap(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = cloneMap(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = cloneMap(val)
		}
		return out
	default:
		return v
	}
}
