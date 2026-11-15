package memory

import (
	"context"
	"sync"

	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

var _ ports.PurchaseOrderIndex = (*PurchaseOrderIndex)(nil)

// PurchaseOrderIndex is an in-memory ports.PurchaseOrderIndex. In production
// the map is written by the live retail-network adapter when it first sees a
// purchase order; a test records it with Register.
type PurchaseOrderIndex struct {
	mu   sync.RWMutex
	refs map[string]shared.NetworkRef
}

func NewPurchaseOrderIndex() *PurchaseOrderIndex {
	return &PurchaseOrderIndex{refs: make(map[string]shared.NetworkRef)}
}

// Register records that the purchase order with this number was filed under ref.
// Like the Postgres map, a number keeps the first ref it was given.
func (i *PurchaseOrderIndex) Register(poNumber string, ref shared.NetworkRef) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if _, taken := i.refs[poNumber]; !taken {
		i.refs[poNumber] = ref
	}
}

func (i *PurchaseOrderIndex) FindNetworkRefByPONumber(_ context.Context, poNumber string) (shared.NetworkRef, bool, error) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	ref, ok := i.refs[poNumber]
	return ref, ok, nil
}
