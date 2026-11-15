package main

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	inboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/inbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/postgres"
	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/system"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
)

// outboundShippingConsumerGroup reads OUTBOUND_SHIPPING_CONSUMER_GROUP,
// defaulting to network-fulfillment-outbound-shipping. Unlike the inbound
// loop's group (mandatory and live-only, because that loop IS the network
// adapter's inbound leg), this consumer runs in EVERY mode:
// warehouse.outbound-shipping.events is a fleet integration topic present
// wherever Kafka is, and a stub-network dev or kind run still confirms
// its orders when a fixture load departs. A default rather than a missing
// boot error keeps deployments that predate the consumer booting; infra
// pins the group by setting the variable, as warehouse-infra does for the
// receipt group.
func outboundShippingConsumerGroup(get func(string) string) string {
	if g := strings.TrimSpace(get("OUTBOUND_SHIPPING_CONSUMER_GROUP")); g != "" {
		return g
	}
	return "network-fulfillment-outbound-shipping"
}

// wireConfirmShipment builds the ONE confirmation use case both inbound
// paths share (ADR 0014 addendum): the ShipmentConfirmed consumer (event
// path, primary) and the manual REST endpoint (operator fallback, one
// release). Sharing the instance is not an optimization — the
// (shipment-ref, order-id) idempotency ledger behind it is what makes
// neither path double-submit, and both paths see it through this one
// construction.
func wireConfirmShipment(in infra, events ports.EventPublisher) *usecases.ConfirmNetworkOrderShipment {
	return &usecases.ConfirmNetworkOrderShipment{
		Orders:        in.orders,
		Gateway:       in.gateway,
		Events:        events,
		Clock:         system.Clock{},
		UnitOfWork:    postgres.NewUnitOfWork(in.pool),
		Confirmations: postgres.NewShipmentConfirmationLog(in.pool),
	}
}

// wireDeferShipment builds the use case the ShipmentConfirmed consumer falls
// back to when a load departs before its order is ACKNOWLEDGED (ADR 0014
// addendum D7): it records the shipment on the order, in the Postgres
// UnitOfWork like every other write, for runConfirmPending to settle.
func wireDeferShipment(in infra, logger *slog.Logger) *usecases.DeferShipmentConfirmation {
	return &usecases.DeferShipmentConfirmation{
		Orders:     in.orders,
		Clock:      system.Clock{},
		UnitOfWork: postgres.NewUnitOfWork(in.pool),
		Logger:     logger,
	}
}

// shipmentConfirmedLoop runs the ShipmentConfirmed consumer until its
// context ends. onDeath is called when the consumer stops on its own: the
// process must not keep running half deaf, so it asks for the whole
// shutdown — the same contract the inbound loop's receipt consumer has.
type shipmentConfirmedLoop struct {
	consumer *inboundkafka.ShipmentConfirmedConsumer
	logger   *slog.Logger
	wg       sync.WaitGroup
	onDeath  func()
	died     atomic.Bool
}

// startShipmentConfirmedLoop wires and starts the loop under a context of
// its own (the caller cancels it through the returned func, after serve
// has returned, and before the pool closes).
func startShipmentConfirmedLoop(
	ctx context.Context,
	requestShutdown func(),
	group string,
	orders ports.NetworkOrderRepo,
	confirm *usecases.ConfirmNetworkOrderShipment,
	deferShipment *usecases.DeferShipmentConfirmation,
	brokers []string,
	logger *slog.Logger,
) (*shipmentConfirmedLoop, context.CancelFunc) {
	loopCtx, cancel := context.WithCancel(ctx)
	loop := &shipmentConfirmedLoop{
		consumer: inboundkafka.NewShipmentConfirmedConsumer(
			brokers, inboundkafka.OutboundShippingTopic, group, orders, confirm, deferShipment, logger),
		logger:  logger,
		onDeath: requestShutdown,
	}
	loop.start(loopCtx)
	return loop, cancel
}

func (l *shipmentConfirmedLoop) start(ctx context.Context) {
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		if err := l.consumer.Run(ctx); err != nil && ctx.Err() == nil {
			l.logger.Error("the shipment-confirmed consumer stopped on its own; shutting down", "err", err)
			l.died.Store(true)
			if l.onDeath != nil {
				l.onDeath()
			}
		}
	}()
}

// stop cancels the loop, waits for it bounded, and releases the consumer.
func (l *shipmentConfirmedLoop) stop(cancel context.CancelFunc, wait time.Duration) {
	cancel()
	if l == nil {
		return
	}
	done := make(chan struct{})
	go func() { l.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(wait):
		l.logger.Warn("the shipment-confirmed consumer did not stop before the shutdown deadline")
	}
	if err := l.consumer.Close(); err != nil {
		l.logger.Warn("closing the shipment-confirmed consumer failed", "err", err)
	}
}

func (l *shipmentConfirmedLoop) failed() bool { return l != nil && l.died.Load() }
