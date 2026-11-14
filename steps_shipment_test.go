package main_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"

	"github.com/cucumber/godog"

	inboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/inbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// Steps of features/shipment_event_rules.feature: the event path of
// ConfirmNetworkOrderShipment (docs/docs/ddd/use-cases.md, ADR 0014 addendum).
// outbound-shipping's ShipmentConfirmed reaches the REAL consumer's Handle (the
// Kafka reader around it is the only thing left out), which joins
// orders[].order_id on NetworkOrder.localOrderId and calls the same use case
// the REST route calls.

// labelGateway is the network's label operation: it records how many labels
// each ask was for and can fail, so "the label is requested first" is visible.
type labelGateway struct {
	ports.NetworkGateway

	mu   sync.Mutex
	asks map[shared.NetworkRef][]int
	down bool
}

func (g *labelGateway) RequestLabel(ctx context.Context, ref shared.NetworkRef, packageCount int) ([]contract.LabelResult, error) {
	g.mu.Lock()
	g.asks[ref] = append(g.asks[ref], packageCount)
	down := g.down
	g.mu.Unlock()
	if down {
		return nil, errors.New("the network cannot issue labels")
	}
	return g.NetworkGateway.RequestLabel(ctx, ref, packageCount)
}

type shipmentWorld struct {
	w *world

	labels   *labelGateway
	consumer *inboundkafka.ShipmentConfirmedConsumer
	lastRaw  []byte
	lastKey  string
	lastErr  error
	handled  bool
	seq      int
}

func (s *shipmentWorld) reset() {
	w := s.w
	s.labels = &labelGateway{NetworkGateway: w.gateway, asks: map[shared.NetworkRef][]int{}}
	w.confirm.Gateway = s.labels
	s.consumer = &inboundkafka.ShipmentConfirmedConsumer{
		Orders:  w.orders,
		Confirm: w.confirm,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:     w.clock.Now,
	}
	s.lastRaw, s.lastKey, s.lastErr, s.handled = nil, "", nil, false
	s.seq = 0
}

// ---- Given: the network's labels ---------------------------------------------------

func (s *shipmentWorld) labelsDown() error {
	s.labels.mu.Lock()
	defer s.labels.mu.Unlock()
	s.labels.down = true
	return nil
}

func (s *shipmentWorld) labelsBack() error {
	s.labels.mu.Lock()
	defer s.labels.mu.Unlock()
	s.labels.down = false
	return nil
}

// ---- When: outbound-shipping's ShipmentConfirmed ------------------------------------

// shipmentEvent builds a CloudEvents 1.0 ShipmentConfirmed whose orders come
// from the table: `order` is "held order of <networkRef>" (the join key of that
// network order), "local order <id>" (an order no network order carries) or
// "no order id"; `packages` is blank when the producer does not send it yet.
func (s *shipmentWorld) shipmentEvent(ctx context.Context, load string, table *godog.Table) ([]byte, error) {
	var orders []map[string]any
	for _, row := range table.Rows[1:] {
		spec, packages := row.Cells[0].Value, strings.TrimSpace(row.Cells[1].Value)
		entry := map[string]any{}
		switch {
		case strings.HasPrefix(spec, "held order of "):
			ref := strings.TrimPrefix(spec, "held order of ")
			o, err := s.w.orders.FindByRef(ctx, shared.NetworkRef(ref))
			if err != nil || o == nil || o.LocalOrderId() == nil {
				return nil, fmt.Errorf("network order %q has no held order to join on: %w", ref, err)
			}
			entry["order_id"] = string(*o.LocalOrderId())
		case strings.HasPrefix(spec, "local order "):
			entry["order_id"] = strings.TrimPrefix(spec, "local order ")
		case spec == "no order id":
			entry["order_id"] = ""
		default:
			return nil, fmt.Errorf("unknown order spec %q", spec)
		}
		if packages != "" {
			n, err := strconv.Atoi(packages)
			if err != nil {
				return nil, fmt.Errorf("packages %q is not a number", packages)
			}
			entry["package_count"] = n
		}
		orders = append(orders, entry)
	}
	return s.envelope(inboundkafka.TypeShipmentConfirmed, map[string]any{
		"load_id": load, "site_id": "site-1", "cpt_id": "cpt-18", "carrier": "ACME", "trailer_id": "T-77",
		"seal_number": "S-1001", "departed_at": s.w.clock.Now().UTC().Format("2006-01-02T15:04:05Z"),
		"orders": orders, "version": 1,
	})
}

func (s *shipmentWorld) envelope(ceType string, data any) ([]byte, error) {
	s.seq++
	return json.Marshal(map[string]any{
		"specversion": "1.0", "id": fmt.Sprintf("ship-evt-%04d", s.seq), "source": "/warehouse/outbound-shipping",
		"type": ceType, "datacontenttype": "application/json", "data": data,
	})
}

func (s *shipmentWorld) deliver(ctx context.Context, key string, raw []byte) {
	s.lastKey, s.lastRaw = key, raw
	s.lastErr = s.consumer.Handle(ctx, key, raw)
	s.handled = true
}

func (s *shipmentWorld) shippingConfirmsLoad(ctx context.Context, load string, table *godog.Table) error {
	raw, err := s.shipmentEvent(ctx, load, table)
	if err != nil {
		return err
	}
	s.deliver(ctx, load, raw)
	return nil
}

func (s *shipmentWorld) shippingConfirmsLoadWithoutLoadID(ctx context.Context, key string, table *godog.Table) error {
	raw, err := s.shipmentEvent(ctx, "", table)
	if err != nil {
		return err
	}
	s.deliver(ctx, key, raw)
	return nil
}

func (s *shipmentWorld) deliveredAgain(ctx context.Context) error {
	if s.lastRaw == nil {
		return fmt.Errorf("no message was delivered yet")
	}
	s.deliver(ctx, s.lastKey, s.lastRaw)
	return nil
}

func (s *shipmentWorld) notACloudEvent(ctx context.Context) error {
	s.deliver(ctx, "load-1", []byte(`{"eventType":"ShipmentConfirmed","loadId":"load-1"}`))
	return nil
}

func (s *shipmentWorld) otherType(ctx context.Context) error {
	raw, err := s.envelope("com.warehouse.wms.outbound-shipping.outboundload.LoadSealed", map[string]any{"load_id": "load-1"})
	if err != nil {
		return err
	}
	s.deliver(ctx, "load-1", raw)
	return nil
}

// ---- Then ----------------------------------------------------------------------------

func (s *shipmentWorld) handledWithoutError() error {
	if !s.handled {
		return fmt.Errorf("no message was delivered")
	}
	if s.lastErr != nil {
		return fmt.Errorf("expected the consumer to handle the message, it answered %w", s.lastErr)
	}
	return nil
}

func (s *shipmentWorld) refusedWith(fragment string) error {
	if !s.handled {
		return fmt.Errorf("no message was delivered")
	}
	if s.lastErr == nil {
		return fmt.Errorf("expected the consumer to refuse the message, it handled it")
	}
	if !strings.Contains(s.lastErr.Error(), fragment) {
		return fmt.Errorf("expected the refusal to mention %q, got %w", fragment, s.lastErr)
	}
	return nil
}

func (s *shipmentWorld) labelsAsked(ref string, times int) error {
	s.labels.mu.Lock()
	defer s.labels.mu.Unlock()
	if got := len(s.labels.asks[shared.NetworkRef(ref)]); got != times {
		return fmt.Errorf("expected the network to be asked for labels %d time(s) for %q, was asked %d time(s)", times, ref, got)
	}
	return nil
}

func (s *shipmentWorld) lastLabelCount(ref string, count int) error {
	s.labels.mu.Lock()
	defer s.labels.mu.Unlock()
	asks := s.labels.asks[shared.NetworkRef(ref)]
	if len(asks) == 0 {
		return fmt.Errorf("the network was never asked for labels for %q", ref)
	}
	if got := asks[len(asks)-1]; got != count {
		return fmt.Errorf("expected the network to be asked for %d label(s) for %q, was asked for %d", count, ref, got)
	}
	return nil
}

func (w *world) registerShipmentSteps(sc *godog.ScenarioContext) {
	s := &shipmentWorld{w: w}
	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		s.reset()
		return ctx, nil
	})

	sc.Step(`^the network cannot issue labels$`, s.labelsDown)
	sc.Step(`^the network issues labels again$`, s.labelsBack)
	sc.Step(`^outbound-shipping confirms load "([^"]*)" carrying:$`, s.shippingConfirmsLoad)
	sc.Step(`^outbound-shipping confirms a load whose payload names none and whose Kafka key is "([^"]*)", carrying:$`, s.shippingConfirmsLoadWithoutLoadID)
	sc.Step(`^that shipment event is delivered again$`, s.deliveredAgain)
	sc.Step(`^a message that is not a CloudEvent reaches the shipment consumer$`, s.notACloudEvent)
	sc.Step(`^an outbound-shipping event of another type reaches the shipment consumer$`, s.otherType)
	sc.Step(`^the shipment consumer handled the message$`, s.handledWithoutError)
	sc.Step(`^the shipment consumer refused the message as "([^"]*)"$`, s.refusedWith)
	sc.Step(`^the network was asked for labels (\d+) times? for "([^"]*)"$`, func(times int, ref string) error { return s.labelsAsked(ref, times) })
	sc.Step(`^the network was asked for (\d+) labels? for "([^"]*)"$`, func(count int, ref string) error { return s.lastLabelCount(ref, count) })
}
