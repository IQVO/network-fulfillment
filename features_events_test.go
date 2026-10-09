package main_test

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	cev "github.com/cloudevents/sdk-go/v2/event"
	"github.com/cucumber/godog"

	"github.com/claudioed/network-fulfillment/internal/adapters/kafka/cloudevents"
)

// decodedMessage is one recorded Kafka message, decoded with the PRODUCTION
// CloudEvents decoder (the same one every consumer in the service uses).
type decodedMessage struct {
	stream string
	key    string
	event  cev.Event
}

var uuidShape = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// domainEventName recovers the domain event name from a CloudEvents `type`,
// ignoring the `.v2` suffix a breaking change carries (ADR 0016).
func domainEventName(ceType string) string {
	name := ceType[strings.LastIndex(ceType, ".")+1:]
	if strings.HasPrefix(name, "v") && len(name) == 2 {
		trimmed := strings.TrimSuffix(ceType, "."+name)
		return trimmed[strings.LastIndex(trimmed, ".")+1:]
	}
	return name
}

func (w *world) decoded(stream string) ([]decodedMessage, error) {
	var out []decodedMessage
	for _, m := range w.bus.messages {
		if stream != "" && m.stream != stream {
			continue
		}
		ev, err := cloudevents.Decode(m.msg.Value)
		if err != nil {
			return nil, err
		}
		out = append(out, decodedMessage{stream: m.stream, key: string(m.msg.Key), event: ev})
	}
	return out, nil
}

func (w *world) countEvents(name, ref string) (int, error) {
	msgs, err := w.decoded("events")
	if err != nil {
		return 0, err
	}
	n := 0
	for _, m := range msgs {
		if domainEventName(m.event.Type()) == name && m.event.Subject() == ref {
			n++
		}
	}
	return n, nil
}

func (w *world) theDomainEventWasPublished(name, ref string) error {
	n, err := w.countEvents(name, ref)
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("expected domain event %q to be published for %q, but it was not", name, ref)
	}
	return nil
}

func (w *world) theDomainEventWasPublishedTimes(name string, expected int, ref string) error {
	n, err := w.countEvents(name, ref)
	if err != nil {
		return err
	}
	if n != expected {
		return fmt.Errorf("expected domain event %q to be published %d time(s) for %q, got %d", name, expected, ref, n)
	}
	return nil
}

func (w *world) theDomainEventWasNotPublished(name, ref string) error {
	n, err := w.countEvents(name, ref)
	if err != nil {
		return err
	}
	if n != 0 {
		return fmt.Errorf("expected domain event %q NOT to be published for %q, but it was published %d time(s)", name, ref, n)
	}
	return nil
}

func (w *world) theRejectionCarriesReason(ref, reason string) error {
	msgs, err := w.decoded("events")
	if err != nil {
		return err
	}
	for _, m := range msgs {
		if domainEventName(m.event.Type()) != "NetworkOrderRejected" || m.event.Subject() != ref {
			continue
		}
		var data struct {
			Reason string `json:"reason"`
		}
		if err := m.event.DataAs(&data); err != nil {
			return err
		}
		if data.Reason != reason {
			return fmt.Errorf("expected NetworkOrderRejected for %q to carry reason %q, got %q", ref, reason, data.Reason)
		}
		return nil
	}
	return fmt.Errorf("no NetworkOrderRejected event was published for %q", ref)
}

// ---- CloudEvents 1.0 conformance (ADR 0008) ------------------------------------

func (w *world) checkEnvelope(m publishedMessage) error {
	ev, err := cloudevents.Decode(m.msg.Value)
	if err != nil {
		return err
	}
	problems := []string{}
	check := func(ok bool, what string) {
		if !ok {
			problems = append(problems, what)
		}
	}
	check(ev.SpecVersion() == "1.0", "specversion must be 1.0")
	check(uuidShape.MatchString(ev.ID()), "id must be a UUID, got "+ev.ID())
	check(ev.Source() == "/warehouse/network-fulfillment", "source must be /warehouse/network-fulfillment, got "+ev.Source())
	check(strings.HasPrefix(ev.Type(), "com.warehouse.wes.network-fulfillment.networkorder."), "type must be com.warehouse.wes.network-fulfillment.networkorder.<Name>, got "+ev.Type())
	check(ev.Subject() != "" && ev.Subject() == string(m.msg.Key), "subject must equal the aggregate (partition) key")
	check(!ev.Time().IsZero(), "time must be set")
	check(ev.DataContentType() == "application/json", "datacontenttype must be application/json")
	check(strings.HasPrefix(ev.DataSchema(), "urn:warehouse:network-fulfillment:"+m.stream+":"), "dataschema must name the "+m.stream+" stream, got "+ev.DataSchema())
	check(hasContentTypeHeader(m), "the Kafka content-type header must announce structured-mode CloudEvents")
	if len(problems) > 0 {
		return fmt.Errorf("message on the %s stream is not a valid CloudEvents 1.0 envelope: %s", m.stream, strings.Join(problems, "; "))
	}
	return nil
}

func hasContentTypeHeader(m publishedMessage) bool {
	for _, h := range m.msg.Headers {
		if h.Key == "content-type" && string(h.Value) == "application/cloudevents+json; charset=UTF-8" {
			return true
		}
	}
	return false
}

func (w *world) everyMessageIsAValidCloudEvent() error {
	if len(w.bus.messages) == 0 {
		return fmt.Errorf("no Kafka message was published, so there is nothing to validate")
	}
	for _, m := range w.bus.messages {
		if err := w.checkEnvelope(m); err != nil {
			return err
		}
	}
	return nil
}

func (w *world) aCloudEventWasPublished(ceType, schema, stream, ref string) error {
	msgs, err := w.decoded(stream)
	if err != nil {
		return err
	}
	seen := []string{}
	for _, m := range msgs {
		seen = append(seen, m.event.Type()+" "+m.event.DataSchema())
		if m.event.Type() == ceType && m.event.DataSchema() == schema && m.event.Subject() == ref {
			return nil
		}
	}
	return fmt.Errorf("expected a CloudEvent of type %q with dataschema %q for %q on the %s stream, got %v", ceType, schema, ref, stream, seen)
}

func (w *world) everyEventIsOnBothStreams() error {
	events, err := w.decoded("events")
	if err != nil {
		return err
	}
	analytics, err := w.decoded("analytics")
	if err != nil {
		return err
	}
	if len(events) == 0 || len(events) != len(analytics) {
		return fmt.Errorf("expected the same non-empty set of events on both streams, got %d on events and %d on analytics", len(events), len(analytics))
	}
	for i := range events {
		if events[i].event.Type() != analytics[i].event.Type() || events[i].event.Subject() != analytics[i].event.Subject() {
			return fmt.Errorf("stream mismatch at %d: %s vs %s", i, events[i].event.Type(), analytics[i].event.Type())
		}
	}
	return nil
}

func (w *world) everyMessageForIsKeyedBy(ref, key string) error {
	msgs, err := w.decoded("")
	if err != nil {
		return err
	}
	matched := 0
	for _, m := range msgs {
		if m.event.Subject() != ref {
			continue
		}
		matched++
		if m.key != key {
			return fmt.Errorf("expected every message for %q to be keyed %q for per-aggregate ordering, got key %q", ref, key, m.key)
		}
	}
	if matched == 0 {
		return fmt.Errorf("no message was published for %q", ref)
	}
	return nil
}

func (w *world) noTwoMessagesShareAnID() error {
	msgs, err := w.decoded("")
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, m := range msgs {
		if seen[m.event.ID()] {
			return fmt.Errorf("CloudEvents id %q was used twice", m.event.ID())
		}
		seen[m.event.ID()] = true
	}
	return nil
}

// ---- the analytics consumer's wire boundary ------------------------------------

const legacyFlatEnvelope = `{"eventId":"legacy-1","eventType":"NetworkOrderReceived","occurredAt":"2026-10-08T12:00:00Z","payload":{"networkRef":"po-legacy"}}`

func (w *world) aLegacyFlatEnvelopeReachesTheConsumer(ctx context.Context) error {
	w.err = w.consumer.HandleMessage(ctx, []byte(legacyFlatEnvelope))
	return nil
}

func (w *world) theConsumerRejectsItAsNotACloudEvent() error {
	if !errors.Is(w.err, cloudevents.ErrNotCloudEvent) {
		return fmt.Errorf("expected the consumer to reject the retired flat envelope as not a CloudEvent, got %v", w.err)
	}
	return nil
}

func (w *world) theLastAnalyticsMessageIsDeliveredAgain(ctx context.Context) error {
	for i := len(w.bus.messages) - 1; i >= 0; i-- {
		if w.bus.messages[i].stream == "analytics" {
			return w.consumer.HandleMessage(ctx, w.bus.messages[i].msg.Value)
		}
	}
	return fmt.Errorf("no analytics message has been published yet")
}

func (w *world) registerEventSteps(sc *godog.ScenarioContext) {
	sc.Step(`^the domain event "([^"]*)" was published for "([^"]*)"$`, w.theDomainEventWasPublished)
	sc.Step(`^the domain event "([^"]*)" was published (\d+) times? for "([^"]*)"$`, w.theDomainEventWasPublishedTimes)
	sc.Step(`^the domain event "([^"]*)" was not published for "([^"]*)"$`, w.theDomainEventWasNotPublished)
	sc.Step(`^the NetworkOrderRejected event for "([^"]*)" carries reason "([^"]*)"$`, w.theRejectionCarriesReason)

	sc.Step(`^every published message is a valid CloudEvents 1\.0 envelope$`, w.everyMessageIsAValidCloudEvent)
	sc.Step(`^a CloudEvent of type "([^"]*)" with dataschema "([^"]*)" was published on the "([^"]*)" stream for "([^"]*)"$`, w.aCloudEventWasPublished)
	sc.Step(`^every domain event was published on both the events and the analytics stream$`, w.everyEventIsOnBothStreams)
	sc.Step(`^every message for "([^"]*)" is keyed by "([^"]*)"$`, w.everyMessageForIsKeyedBy)
	sc.Step(`^no two published messages share a CloudEvents id$`, w.noTwoMessagesShareAnID)

	sc.Step(`^a legacy flat-envelope message reaches the analytics consumer$`, w.aLegacyFlatEnvelopeReachesTheConsumer)
	sc.Step(`^the analytics consumer rejects it as not a CloudEvent$`, w.theConsumerRejectsItAsNotACloudEvent)
	sc.Step(`^the last analytics message is delivered again$`, w.theLastAnalyticsMessageIsDeliveredAgain)
}
