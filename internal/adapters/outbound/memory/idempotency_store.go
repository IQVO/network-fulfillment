package memory

import (
	"context"
	"sync"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
)

// IdempotencyStore is the in-memory ports.IdempotencyStore, for tests and
// any wiring without Postgres. Same semantics as the Postgres store: the
// key alone identifies a record, the fingerprint decides replay or reuse.
type IdempotencyStore struct {
	mu   sync.Mutex
	recs map[string]*idemRecord
}

type idemRecord struct {
	fingerprint string
	resp        *contract.StoredResponse
}

// NewIdempotencyStore constructs an empty IdempotencyStore.
func NewIdempotencyStore() *IdempotencyStore {
	return &IdempotencyStore{recs: map[string]*idemRecord{}}
}

var _ ports.IdempotencyStore = (*IdempotencyStore)(nil)

// Claim reserves key for fingerprint, or reports the existing record.
func (s *IdempotencyStore) Claim(_ context.Context, key, fingerprint string) (contract.IdempotencyClaim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.recs[key]
	if !ok {
		s.recs[key] = &idemRecord{fingerprint: fingerprint}
		return contract.IdempotencyClaim{Owned: true}, nil
	}
	if rec.fingerprint != fingerprint {
		return contract.IdempotencyClaim{}, contract.ErrIdempotencyKeyReused
	}
	if rec.resp == nil {
		return contract.IdempotencyClaim{Pending: true}, nil
	}
	cp := *rec.resp
	cp.Body = append([]byte(nil), rec.resp.Body...)
	return contract.IdempotencyClaim{Replay: &cp}, nil
}

// Complete stores the answer for an owned key.
func (s *IdempotencyStore) Complete(_ context.Context, key string, resp contract.StoredResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec, ok := s.recs[key]; ok {
		resp.Body = append([]byte(nil), resp.Body...)
		rec.resp = &resp
	}
	return nil
}

// Release forgets an owned key whose execution failed.
func (s *IdempotencyStore) Release(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec, ok := s.recs[key]; ok && rec.resp == nil {
		delete(s.recs, key)
	}
	return nil
}
