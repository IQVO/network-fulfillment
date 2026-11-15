package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-fulfillment/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// OutboundShippingTopic is outbound-shipping's integration topic (its
// asyncapi channel warehouse.outbound-shipping.events). Messages are keyed
// by the load id.
const OutboundShippingTopic = "warehouse.outbound-shipping.events"

// TypeShipmentConfirmed is the one CloudEvents type this consumer acts on:
// outbound-shipping's "this actually shipped" fact (its ADR 0002/0004).
// Everything else on the topic is ignored (forward compatibility).
const TypeShipmentConfirmed = "com.warehouse.wms.outbound-shipping.outboundload.ShipmentConfirmed"

// ShipmentConfirmedConsumer reads outbound-shipping's events under a
// consumer group of this service's own (OUTBOUND_SHIPPING_CONSUMER_GROUP)
// and confirms the network orders a shipment closed — the event path of
// ADR 0014's addendum, replacing the manual POST as the primary trigger
// while that endpoint stays one release as the operator fallback.
//
// The join is the persisted mapping ADR 0014 said was missing and the
// fleet then built: orders[].order_id IS order-management's OrderId, which
// NetworkOrder.localOrderId has stored since the demand loop (never a
// string convention — ADR 0001 Hard rule 4). An order_id no NetworkOrder
// carries is a local order raised from customer demand: skipped and
// logged, not an error. Delivery is at-least-once; neither redelivery nor
// the manual endpoint racing the event can double-submit, because every
// confirmation routes through ConfirmNetworkOrderShipment's
// (shipment-ref, order-id) idempotency key — load id plus order id — which
// subsumes the CloudEvents id as the dedupe key (a replayed event re-runs
// the same key, a re-published one carries new ids but the same load).
// A load can depart while its order is still SUBMITTED (the network has
// not settled the acknowledgement yet): that is a race, not a broken
// contract, so the shipment is recorded on the order through Defer and
// confirmed once the order is ACKNOWLEDGED (ADR 0014 addendum D7) — the
// message is committed, never dead-lettered. For an order in any other state
// the confirm error stands (NEW or REJECTED is a shipped order the network
// never took, an anomaly for an operator). Like the receipt consumer, a
// message that is not a CloudEvent, or whose data breaks outbound-shipping's
// contract, is dead-lettered to topic+".dlq" so one poison message never
// blocks the partition, and so is a confirmation that keeps failing; the
// manual endpoint is the operator's way to close the gap a dead-lettered
// confirmation left.
type ShipmentConfirmedConsumer struct {
	Reader  *kafkago.Reader
	Orders  ports.NetworkOrderRepo
	Confirm *usecases.ConfirmNetworkOrderShipment
	// Defer records a shipment that arrived before its order was ACKNOWLEDGED.
	// The composition root sets it; without one such a shipment fails like any
	// other confirmation error.
	Defer     *usecases.DeferShipmentConfirmation
	Logger    *slog.Logger
	Now       func() time.Time
	dlqWriter *kafkago.Writer
}

// shipmentConfirmedReadMaxWait keeps a quiet topic from blocking
// Reader.Close for the kafka-go default of 10s (the fleet rule for every
// reader).
const shipmentConfirmedReadMaxWait = 500 * time.Millisecond

// NewShipmentConfirmedConsumer builds the consumer for topic under
// groupID. A new group starts at the earliest offset: a load that departed
// while the service was down must still confirm its orders; every
// redelivered event is a no-op through the idempotency key.
func NewShipmentConfirmedConsumer(
	brokers []string, topic, groupID string,
	orders ports.NetworkOrderRepo,
	confirm *usecases.ConfirmNetworkOrderShipment,
	deferShipment *usecases.DeferShipmentConfirmation,
	logger *slog.Logger,
) *ShipmentConfirmedConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	return &ShipmentConfirmedConsumer{
		Reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers, Topic: topic, GroupID: groupID,
			StartOffset: kafkago.FirstOffset, MaxWait: shipmentConfirmedReadMaxWait,
		}),
		Orders:    orders,
		Confirm:   confirm,
		Defer:     deferShipment,
		Logger:    logger,
		Now:       time.Now,
		dlqWriter: newDLQWriter(brokers, topic+dlqTopicSuffix),
	}
}

// Run consumes until ctx ends (nil) or the reader fails for good (the error).
func (c *ShipmentConfirmedConsumer) Run(ctx context.Context) error {
	for {
		msg, err := c.Reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if err := c.handleMessage(ctx, msg); err != nil {
			if ctx.Err() != nil && errors.Is(err, context.Canceled) {
				return nil // shutdown mid-handle: uncommitted, redelivered, deduplicated
			}
			return err
		}
	}
}

// Close releases the reader and the dead-letter writer.
func (c *ShipmentConfirmedConsumer) Close() error {
	return errors.Join(c.Reader.Close(), c.dlqWriter.Close())
}

func (c *ShipmentConfirmedConsumer) handleMessage(ctx context.Context, msg kafkago.Message) error {
	return handleWithDeadLetter(ctx, c.Logger, "shipment-confirmed consumer", c.Handle, errBadShipmentEvent, c.Reader, c.dlq, msg)
}

func (c *ShipmentConfirmedConsumer) dlq(ctx context.Context, msg kafkago.Message, cause error) error {
	return c.dlqWriter.WriteMessages(ctx, deadLetterMessage(msg, c.Reader.Config().Topic, cause, c.Now()))
}

var errBadShipmentEvent = errors.New("shipment-confirmed consumer: the event data is not what outbound-shipping's contract says")

type shipmentConfirmedData struct {
	LoadId     string    `json:"load_id"`
	SiteId     string    `json:"site_id"`
	CptId      string    `json:"cpt_id"`
	Carrier    string    `json:"carrier"`
	TrailerId  string    `json:"trailer_id"`
	SealNumber string    `json:"seal_number"`
	DepartedAt time.Time `json:"departed_at"`
	Orders     []struct {
		// OrderId is order-management's OrderId: the join key on
		// NetworkOrder.localOrderId.
		OrderId string `json:"order_id"`
		// PackageCount is the ADDITIVE per-order package count the
		// producer contract does not carry yet (ADR 0014 addendum): it
		// decodes the day it ships, and 0 keeps ADR 0026's
		// single-package placeholder.
		PackageCount int `json:"package_count,omitempty"`
	} `json:"orders"`
	Version int `json:"version"`
}

// Handle decodes one message value and applies it, without retry. key is
// the Kafka key (the load id). An unknown type is a skip; a value that is
// not a CloudEvent wraps cloudevents.ErrNotCloudEvent.
func (c *ShipmentConfirmedConsumer) Handle(ctx context.Context, key string, raw []byte) error {
	env, err := cloudevents.Decode(raw)
	if err != nil {
		return fmt.Errorf("shipment-confirmed consumer: decode event: %w", err)
	}
	if env.Type() != TypeShipmentConfirmed {
		return nil
	}
	return c.confirm(ctx, key, env)
}

func (c *ShipmentConfirmedConsumer) confirm(ctx context.Context, key string, env ce.Event) error {
	var d shipmentConfirmedData
	if err := env.DataAs(&d); err != nil {
		return fmt.Errorf("%w: ShipmentConfirmed: %w", errBadShipmentEvent, err)
	}
	// The payload's load_id is the contract's source of truth, the Kafka
	// key (also the load id, like inbound-receiving's ASN fallback) its
	// fallback when the payload names none.
	loadId := d.LoadId
	if loadId == "" {
		loadId = key
	}
	if loadId == "" {
		return fmt.Errorf("%w: ShipmentConfirmed names no load_id", errBadShipmentEvent)
	}
	for _, order := range d.Orders {
		if order.OrderId == "" {
			return fmt.Errorf("%w: ShipmentConfirmed %s has an orders[] entry without order_id", errBadShipmentEvent, loadId)
		}
		if order.PackageCount < 0 {
			return fmt.Errorf("%w: ShipmentConfirmed %s has a negative package_count for %s", errBadShipmentEvent, loadId, order.OrderId)
		}
		o, err := c.Orders.FindByLocalOrderId(ctx, shared.LocalOrderId(order.OrderId))
		if err != nil {
			return err
		}
		if o == nil {
			// Not network-originated demand: a local order raised from
			// customer demand shares the shipment but has no NetworkOrder
			// to confirm. Skipped and logged, never an error — a mixed
			// load must not block the network's orders on it.
			c.Logger.Info("shipment-confirmed consumer: no network order for order_id; skipping",
				"ce_id", env.ID(), "load_id", loadId, "order_id", order.OrderId)
			continue
		}
		if err := c.confirmOrder(ctx, env, loadId, order.OrderId, o.NetworkRef(), order.PackageCount); err != nil {
			return err
		}
	}
	if len(d.Orders) == 0 {
		// Schema-legal (an empty array) but nothing to confirm; logged so
		// a producer regression is visible, not silent.
		c.Logger.Info("shipment-confirmed consumer: ShipmentConfirmed carries no orders", "ce_id", env.ID(), "load_id", loadId)
	}
	return nil
}

// confirmOrder confirms one order of the load. An order still SUBMITTED is
// not ready to be confirmed yet (ErrConfirmBeforeAcknowledge): the shipment is
// deferred onto the order and the next order is handled. When deferring is
// refused too, which is the case for every state but SUBMITTED, its error is
// returned and the retry-then-dead-letter path applies as it always did.
func (c *ShipmentConfirmedConsumer) confirmOrder(ctx context.Context, env ce.Event, loadId, orderId string, ref shared.NetworkRef, packageCount int) error {
	_, err := c.Confirm.ExecuteForShipment(ctx, ref, loadId, packageCount)
	if err == nil {
		return nil
	}
	if c.Defer != nil && errors.Is(err, networkorder.ErrConfirmBeforeAcknowledge) {
		if err = c.Defer.Execute(ctx, ref, loadId, packageCount); err == nil {
			c.Logger.Info("shipment-confirmed consumer: shipment confirmed before the order was acknowledged; deferred",
				"ce_id", env.ID(), "load_id", loadId, "order_id", orderId)
			return nil
		}
	}
	return fmt.Errorf("confirm %s for load %s: %w", ref, loadId, err)
}
