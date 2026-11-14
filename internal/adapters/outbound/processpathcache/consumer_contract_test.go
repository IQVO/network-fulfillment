package processpathcache

// Event consumer contract tests (E1-E4) for the process-path-management
// edge: this cache's Consumer replays topic
// warehouse.process-path-management.events (Topic, wired into
// kafka.ReaderConfig by NewConsumerForTopic) into its in-memory
// process-path capability read model. The contract validates against the
// PINNED copy of process-path-management's apis/asyncapi.yaml
// (testdata/process-path-management.asyncapi.yaml, registered in
// contracts/pinned.json). This producer documents its OWN topics under
// 'publish:' — the fleet's channel-perspective convention is not uniform,
// so the pinned spec is read as the topic's payload catalog. Golden
// fixtures are the producer spec's OWN examples.

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	kafkago "github.com/segmentio/kafka-go"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/network-fulfillment/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// pinnedSpec is the pinned copy of process-path-management's asyncapi.
const pinnedSpec = "testdata/process-path-management.asyncapi.yaml"

// pinnedProducerSource is this producer's CloudEvents source.
const pinnedProducerSource = "/warehouse/process-path-management"

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

// producerMessageRefs returns the message names the topic's operation
// documents, under whichever operation carries them (this producer uses
// 'publish:' for its own topics).
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

// E1: the topic this cache's reader actually subscribes to (the Topic
// constant NewConsumerForTopic wires into kafka.ReaderConfig) is
// documented by the producer's pinned spec, and every CloudEvents type
// the decoder's switch routes on is documented on that channel.
func TestPinnedAsyncAPICoversSubscriptions_ProcessPathEvents(t *testing.T) {
	doc := loadPinned(t)
	documented := map[string]bool{}
	for _, name := range doc.producerMessageRefs(t, Topic) {
		if typ, _ := doc.goldenFor(t, name)["type"].(string); typ != "" {
			documented[typ] = true
		}
	}
	for _, typ := range []string{typeCreated, typeUpdated, typeDeactivated, typeCPTScheduleChanged} {
		if !documented[typ] {
			t.Errorf("pinned process-path-management spec does not document %s on %s (documented: %v)", typ, Topic, documented)
		}
	}
}

// E2: the producer's golden examples decode through the REAL
// cloudevents.Decode and apply to the read model: a golden
// ProcessPathCreated example makes its path's cycle time available to
// NextCutoff, and a golden CPTScheduleChanged snapshot makes NextCutoff
// answer for the wire's own site with the wire's own cutoff.
func TestGoldenEventsDecode_ProcessPathEvents(t *testing.T) {
	doc := loadPinned(t)
	c := newTestConsumer(nil)

	created := doc.goldenFor(t, "ProcessPathCreated")
	applyGolden(t, c, created)
	applyGolden(t, c, doc.goldenFor(t, "ProcessPathUpdated"))
	applyGolden(t, c, doc.goldenFor(t, "ProcessPathDeactivated"))
	applyGolden(t, c, doc.goldenFor(t, "CPTScheduleChanged"))

	schedule := doc.goldenFor(t, "CPTScheduleChanged")
	data := schedule["data"].(map[string]any)
	site, _ := data["site_id"].(string)
	cutoff, _ := data["cutoffs"].([]any)[0].(map[string]any)
	localTime, _ := cutoff["local_time"].(string)

	after := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC) // a Monday
	next, ok := c.NextCutoff(shared.SiteId(site), after)
	if !ok {
		t.Fatalf("golden CPTScheduleChanged produced no next cutoff for site %s", site)
	}
	loc := zoneOfSchedule(t, data)
	if got := next.CutoffAt.In(loc).Format("15:04"); got != localTime {
		t.Errorf("next cutoff answers %s local, the wire's local_time is %q", got, localTime)
	}
	if next.CutoffAt.Weekday() == time.Sunday || next.CutoffAt.Weekday() == time.Saturday {
		t.Errorf("next cutoff landed on %s, outside the wire's weekdays", next.CutoffAt.Weekday())
	}

	// The golden ProcessPathCreated example's path must come back with
	// the wire's own cycle time (24h0m0s) and known=true.
	createdData := created["data"].(map[string]any)
	pathId, _ := createdData["path_id"].(string)
	found := false
	for _, p := range next.Paths {
		if p.PathId == pathId {
			found = true
			if !p.CycleTimeKnown || p.CycleTimeP95 != 24*time.Hour {
				t.Errorf("path %s came back known=%v cycle=%v, the wire says 24h known", pathId, p.CycleTimeKnown, p.CycleTimeP95)
			}
		}
	}
	if !found {
		t.Errorf("the golden example's path %q is not among the eligible paths %v", pathId, next.Paths)
	}
}

// zoneOfSchedule resolves the schedule payload's own timezone.
func zoneOfSchedule(t *testing.T, data map[string]any) *time.Location {
	t.Helper()
	tz, _ := data["timezone"].(string)
	loc, err := time.LoadLocation(tz)
	if err != nil {
		t.Fatalf("resolve the wire's timezone %q: %v", tz, err)
	}
	return loc
}

// E3: every golden fixture of the routed messages carries the
// fleet-mandatory CloudEvents 1.0 envelope — specversion 1.0, the
// identity attributes, datacontenttype application/json, and the
// producer's urn: dataschema.
func TestCloudEventsEnvelopeInvariants_ProcessPath(t *testing.T) {
	doc := loadPinned(t)
	for _, name := range []string{"ProcessPathCreated", "ProcessPathUpdated", "ProcessPathDeactivated", "CPTScheduleChanged"} {
		for _, ex := range doc.message(t, name).Examples {
			evt, err := cloudevents.Decode(mustJSON(t, ex.Payload))
			if err != nil {
				t.Fatalf("decode golden example %s/%s: %v", name, ex.Name, err)
			}
			assertProcessPathEnvelope(t, name, ex, evt)
		}
	}
}

// assertProcessPathEnvelope holds one golden example to the fleet's
// CloudEvents 1.0 envelope bar with this producer's identity.
func assertProcessPathEnvelope(t *testing.T, name string, ex pinnedExample, evt ce.Event) {
	t.Helper()
	if evt.SpecVersion() != cloudevents.SpecVersion {
		t.Errorf("%s/%s: specversion %q, want %q", name, ex.Name, evt.SpecVersion(), cloudevents.SpecVersion)
	}
	if evt.ID() == "" || evt.Subject() == "" || evt.Time().IsZero() {
		t.Errorf("%s/%s: id/subject/time must all be present", name, ex.Name)
	}
	if evt.Source() != pinnedProducerSource {
		t.Errorf("%s/%s: source %q, want %q", name, ex.Name, evt.Source(), pinnedProducerSource)
	}
	if evt.DataMediaType() != cloudevents.DataContentType {
		t.Errorf("%s/%s: datacontenttype %q, want %q", name, ex.Name, evt.DataMediaType(), cloudevents.DataContentType)
	}
	if want := "urn:warehouse:process-path-management:events:" + name + ":v1"; evt.DataSchema() != want {
		t.Errorf("%s/%s: dataschema %q, want %q", name, ex.Name, evt.DataSchema(), want)
	}
}

// E4: this cache is a replayable read model on a shared topic — unknown
// ADDITIVE attributes in data must not fail the apply (the golden
// ProcessPathCreated example's own match_prefix/eligibility/intake
// fields are the live case: this cache's DTO ignores them), an unknown
// event TYPE is silently skipped with no state change, and a message
// that is not a CloudEvent at all is a deterministic error wrapping
// ErrNotCloudEvent — the exact signal Run skips-and-logs (this cache
// has no DLQ: a poison message must not block the replay).
func TestUnknownAttributesTolerated_ProcessPath(t *testing.T) {
	doc := loadPinned(t)
	golden := doc.goldenFor(t, "CPTScheduleChanged")

	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	c := newTestConsumer(nil)
	applyGolden(t, c, tolerant)
	schedule := golden["data"].(map[string]any)
	site := shared.SiteId(schedule["site_id"].(string))
	if _, ok := c.NextCutoff(site, time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)); !ok {
		t.Error("unknown data attributes lost the applied schedule")
	}

	// Unknown event TYPE on the shared topic: silently skipped, state
	// unchanged.
	otherType := clonePayload(golden)
	otherType["type"] = "com.warehouse.wes.process-path-management.processpath.ProcessPathArchived"
	applyGolden(t, c, otherType)
	next, _ := c.NextCutoff(site, time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC))
	if next.CutoffAt.IsZero() {
		t.Error("unknown event type cleared the schedule")
	}

	// Not a CloudEvent: deterministic error wrapping ErrNotCloudEvent —
	// the skip-and-log signal.
	if err := c.handle(kafkago.Message{Value: []byte(`{"path_id":"legacy"}`)}); !errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Errorf("retired flat envelope handled as %v, want ErrNotCloudEvent (the skip signal)", err)
	}
}

// mustJSON marshals a golden payload into the JSON bytes a Kafka
// consumer actually receives.
func mustJSON(t *testing.T, payload map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal golden payload: %v", err)
	}
	return raw
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
