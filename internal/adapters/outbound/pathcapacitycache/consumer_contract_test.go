package pathcapacitycache

// Event consumer contract tests (E1-E4) for the wes-work-planning edge:
// this cache's Consumer replays topic warehouse.work-planning.events
// (Topic, wired into kafka.ReaderConfig by NewConsumerForTopic) into its
// in-memory path-capacity read model, acting on PathCapacityChanged
// only. The contract validates against the PINNED copy of
// wes-work-planning's apis/asyncapi.yaml
// (testdata/wes-work-planning.asyncapi.yaml, registered in
// contracts/pinned.json). Golden fixtures are the producer spec's OWN
// examples.

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-fulfillment/internal/adapters/kafka/cloudevents"
)

// pinnedSpec is the pinned copy of wes-work-planning's asyncapi.
const pinnedSpec = "testdata/wes-work-planning.asyncapi.yaml"

// The minimal slice of the pinned AsyncAPI the contract tests read.
type pinnedDoc struct {
	Channels map[string]struct {
		Subscribe *pinnedOperation `yaml:"subscribe"`
		Publish   *pinnedOperation `yaml:"publish"`
	} `yaml:"channels"`
	Components struct {
		Messages map[string]pinnedMessage `yaml:"messages"`
	} `yaml:"components"`
}

type pinnedOperation struct {
	Message *struct {
		Ref   string      `yaml:"$ref"`
		OneOf []pinnedRef `yaml:"oneOf"`
	} `yaml:"message"`
}

type pinnedRef struct {
	Ref string `yaml:"$ref"`
}

type pinnedMessage struct {
	Examples []pinnedExample `yaml:"examples"`
}

type pinnedExample struct {
	Name    string            `yaml:"name"`
	Headers map[string]string `yaml:"headers"`
	Payload map[string]any    `yaml:"payload"`
}

// loadPinned reads and parses the pinned spec copy.
func loadPinned(t *testing.T) *pinnedDoc {
	t.Helper()
	raw, err := os.ReadFile(pinnedSpec)
	if err != nil {
		t.Fatalf("read pinned spec %s (copy it from the producer, never hand-write it): %v", pinnedSpec, err)
	}
	var doc pinnedDoc
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse pinned spec %s: %v", pinnedSpec, err)
	}
	return &doc
}

// producerMessageRefs returns the message names the topic's 'subscribe:'
// operation documents (wes-work-planning follows the fleet
// channel-perspective convention; 'publish:' is accepted for the repos
// that invert it).
func (d *pinnedDoc) producerMessageRefs(t *testing.T, topic string) []string {
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
	var refs []pinnedRef
	if op.Message.Ref != "" {
		refs = append(refs, pinnedRef{Ref: op.Message.Ref})
	}
	refs = append(refs, op.Message.OneOf...)
	names := make([]string, 0, len(refs))
	for _, r := range refs {
		i := len(r.Ref) - 1
		for ; i >= 0 && r.Ref[i] != '/'; i-- {
		}
		if i < 0 || i == len(r.Ref)-1 {
			t.Fatalf("malformed local $ref %q", r.Ref)
		}
		names = append(names, r.Ref[i+1:])
	}
	if len(names) == 0 {
		t.Fatalf("pinned spec's operation on %q lists no messages", topic)
	}
	return names
}

func (d *pinnedDoc) message(t *testing.T, name string) pinnedMessage {
	t.Helper()
	msg, ok := d.Components.Messages[name]
	if !ok {
		t.Fatalf("pinned spec references undefined components/messages/%s", name)
	}
	return msg
}

// goldenFor returns the message's first pinned example payload.
func (d *pinnedDoc) goldenFor(t *testing.T, name string) map[string]any {
	t.Helper()
	msg := d.message(t, name)
	if len(msg.Examples) == 0 {
		t.Fatalf("pinned spec documents %s without examples", name)
	}
	return msg.Examples[0].Payload
}

// applyGolden decodes and applies one golden payload through the REAL
// handle path, failing on any error.
func applyGolden(t *testing.T, c *Consumer, payload map[string]any) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal golden payload: %v", err)
	}
	if err := c.handle(kafkago.Message{Value: raw}); err != nil {
		t.Fatalf("handle golden example %v: %v", payload["type"], err)
	}
}

// goldenCapacity extracts the wire facts of a PathCapacityChanged
// example payload. Numeric coercion is tolerant: yaml.v3 decodes whole
// numbers as int while json.Unmarshal would give float64 — accept either.
func goldenCapacity(payload map[string]any) (pathId string, cutoffAt time.Time, remaining int, wireKnown bool) {
	data := payload["data"].(map[string]any)
	pathId, _ = data["path_id"].(string)
	cutoffAt, _ = time.Parse(time.RFC3339, data["cutoff_at"].(string))
	switch v := data["remaining_units"].(type) {
	case int:
		remaining = v
	case int64:
		remaining = int(v)
	case float64:
		remaining = int(v)
	}
	wireKnown, _ = data["known"].(bool)
	return pathId, cutoffAt, remaining, wireKnown
}

// E1: the topic this cache's reader actually subscribes to (the Topic
// constant NewConsumerForTopic wires into kafka.ReaderConfig) is
// documented by the producer's pinned spec, and the one CloudEvents type
// the decoder's switch routes on is documented on that channel.
func TestPinnedAsyncAPICoversSubscriptions_WorkPlanningEvents(t *testing.T) {
	doc := loadPinned(t)
	documented := map[string]bool{}
	for _, name := range doc.producerMessageRefs(t, Topic) {
		if typ, _ := doc.goldenFor(t, name)["type"].(string); typ != "" {
			documented[typ] = true
		}
	}
	if !documented[typePathCapacityChanged] {
		t.Errorf("pinned wes-work-planning spec does not document %s on %s (documented: %v)", typePathCapacityChanged, Topic, documented)
	}
}

// E2: every PathCapacityChanged example the producer's pinned spec
// documents decodes through the REAL cloudevents.Decode and applies to
// the read model: RemainingCapacity answers with the wire's own
// remaining_units for the wire's own (path, cutoff) key, and the known
// flag follows the wire's known field.
func TestGoldenEventsDecode_WorkPlanningEvents(t *testing.T) {
	doc := loadPinned(t)
	c := newTestConsumer(nil)
	for _, ex := range doc.message(t, "PathCapacityChanged").Examples {
		pathId, cutoffAt, remaining, wireKnown := goldenCapacity(ex.Payload)
		// RemainingCapacity is an EXACT (pathId, cutoffAt) match by design
		// (its doc comment: nearest-window correlation is a future caller's
		// responsibility), so query the wire's own cutoff key. The known flag
		// FOLLOWS THE WIRE: a FlowFed path example carries known:false (with
		// remaining_units 0 per the spec) and the cache must answer unknown.
		applyGolden(t, c, ex.Payload)
		got, known := c.RemainingCapacity(pathId, cutoffAt)
		if known != wireKnown {
			t.Fatalf("%s: known = %v, the wire says %v", ex.Name, known, wireKnown)
		}
		if known && got != remaining {
			t.Errorf("%s: RemainingCapacity(%s) = %d, the wire says %d", ex.Name, pathId, got, remaining)
		}
	}
}

// E3: every golden fixture of the routed message carries the
// fleet-mandatory CloudEvents 1.0 envelope — specversion 1.0, the
// identity attributes, datacontenttype application/json, and the
// producer's urn: dataschema.
func TestCloudEventsEnvelopeInvariants_WorkPlanning(t *testing.T) {
	doc := loadPinned(t)
	for _, ex := range doc.message(t, "PathCapacityChanged").Examples {
		raw, err := json.Marshal(ex.Payload)
		if err != nil {
			t.Fatalf("marshal golden payload: %v", err)
		}
		evt, err := cloudevents.Decode(raw)
		if err != nil {
			t.Fatalf("decode golden example %s: %v", ex.Name, err)
		}
		if evt.SpecVersion() != cloudevents.SpecVersion {
			t.Errorf("%s: specversion %q, want %q", ex.Name, evt.SpecVersion(), cloudevents.SpecVersion)
		}
		if evt.ID() == "" || evt.Subject() == "" || evt.Time().IsZero() {
			t.Errorf("%s: id/subject/time must all be present", ex.Name)
		}
		if evt.Source() != "/warehouse/wes-work-planning" {
			t.Errorf("%s: source %q, want /warehouse/wes-work-planning", ex.Name, evt.Source())
		}
		if evt.DataMediaType() != cloudevents.DataContentType {
			t.Errorf("%s: datacontenttype %q, want %q", ex.Name, evt.DataMediaType(), cloudevents.DataContentType)
		}
		if want := "urn:warehouse:wes-work-planning:events:PathCapacityChanged:v1"; evt.DataSchema() != want {
			t.Errorf("%s: dataschema %q, want %q", ex.Name, evt.DataSchema(), want)
		}
	}
}

// E4: this cache is a replayable read model on a shared topic — unknown
// ADDITIVE attributes in data must not fail the apply, an unknown event
// TYPE is silently skipped with no state change, and a message that is
// not a CloudEvent at all is a deterministic error wrapping
// ErrNotCloudEvent — the exact signal Run skips-and-logs (this cache has
// no DLQ: a poison message must not block the replay).
func TestUnknownAttributesTolerated_WorkPlanning(t *testing.T) {
	doc := loadPinned(t)
	golden := doc.goldenFor(t, "PathCapacityChanged")
	pathId, cutoffAt, remaining, _ := goldenCapacity(golden)

	// Unknown additive attributes in data — scalar and nested.
	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	c := newTestConsumer(nil)
	applyGolden(t, c, tolerant)
	got, known := c.RemainingCapacity(pathId, cutoffAt)
	if !known || got != remaining {
		t.Errorf("unknown data attributes changed the outcome: (%d, %v), want (%d, true)", got, known, remaining)
	}

	// Unknown event TYPE on the shared topic: silently skipped, state
	// unchanged.
	otherType := clonePayload(golden)
	otherType["type"] = "com.warehouse.wes.work-planning.workpool.WorkReleased"
	applyGolden(t, c, otherType)
	got, known = c.RemainingCapacity(pathId, cutoffAt)
	if !known || got != remaining {
		t.Errorf("unknown event type changed the state: (%d, %v), want (%d, true)", got, known, remaining)
	}

	// Not a CloudEvent: deterministic error wrapping ErrNotCloudEvent —
	// the skip-and-log signal.
	if err := c.handle(kafkago.Message{Value: []byte(`{"path_id":"legacy"}`)}); !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Errorf("retired flat envelope handled as %v, want ErrNotCloudEvent (the skip signal)", err)
	}
}

// clonePayload deep-copies a golden payload so a test can mutate it.
func clonePayload(p map[string]any) map[string]any {
	return cloneAny(p).(map[string]any)
}

func cloneAny(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = cloneAny(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = cloneAny(val)
		}
		return out
	default:
		return v
	}
}
