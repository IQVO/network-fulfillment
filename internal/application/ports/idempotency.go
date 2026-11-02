package ports

import (
	"context"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
)

// IdempotencyStore keeps Idempotency-Key records (fleet REST guideline
// section 2). The key alone identifies a record; the fingerprint (method +
// path + body hash) decides replay (same) or
// contract.ErrIdempotencyKeyReused (different).
type IdempotencyStore interface {
	Claim(ctx context.Context, key, fingerprint string) (contract.IdempotencyClaim, error)
	Complete(ctx context.Context, key string, resp contract.StoredResponse) error
	Release(ctx context.Context, key string) error
}
