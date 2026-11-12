package memory

import (
	"context"
	"sync"
	"time"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// ShipmentConfirmationLog is an in-memory
// ports.ShipmentConfirmationLog (test double; the service runs on the
// Postgres adapter). Unlike the Postgres one it cannot JOIN a caller's
// transaction — in-memory use cases have no real unit of work — so Record
// claims under its own lock, which is atomic for every in-memory caller.
type ShipmentConfirmationLog struct {
	mu      sync.Mutex
	claimed map[string]struct{}
}

// NewShipmentConfirmationLog constructs an empty ShipmentConfirmationLog.
func NewShipmentConfirmationLog() *ShipmentConfirmationLog {
	return &ShipmentConfirmationLog{claimed: make(map[string]struct{})}
}

func key(shipmentRef, orderId string) string { return shipmentRef + "\x00" + orderId }

// Exists reports whether the pair was already confirmed.
func (l *ShipmentConfirmationLog) Exists(_ context.Context, shipmentRef string, orderId string) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.claimed[key(shipmentRef, orderId)]
	return ok, nil
}

// Record claims the pair; a duplicate claim is a no-op, as in Postgres.
func (l *ShipmentConfirmationLog) Record(_ context.Context, shipmentRef string, orderId string, _ shared.NetworkRef, _ time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.claimed[key(shipmentRef, orderId)] = struct{}{}
	return nil
}
