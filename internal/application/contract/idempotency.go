package contract

import "errors"

// ErrIdempotencyKeyReused is what an IdempotencyStore answers when a key is
// claimed again with a different request fingerprint (method + path + body
// hash). The HTTP adapter maps it to 409 idempotency-key-reused.
var ErrIdempotencyKeyReused = errors.New("idempotency key was already used with a different request")

// StoredResponse is the deterministic answer kept for an Idempotency-Key and
// replayed byte for byte on a retry.
type StoredResponse struct {
	Status      int
	ContentType string
	Location    string
	Body        []byte
}

// IdempotencyClaim is the outcome of IdempotencyStore.Claim.
//
//   - Owned: the caller now owns the key and must Complete or Release it.
//   - Pending: another request holds the key with the same fingerprint and
//     has not finished; the caller waits and claims again.
//   - Replay: the key finished with the same fingerprint; replay it.
type IdempotencyClaim struct {
	Owned   bool
	Pending bool
	Replay  *StoredResponse
}
