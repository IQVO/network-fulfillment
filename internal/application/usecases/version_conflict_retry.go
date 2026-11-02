package usecases

import (
	"errors"

	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
)

// versionConflictAttempts bounds how often a poller or sweep pass will
// reload an order and re-apply its transition after another writer
// (a sibling sweep, the poller, a REST confirmation) won the row.
// Three attempts absorbs a two-writer race; a persistently hot row is
// left for the next scheduled pass rather than looped on forever.
const versionConflictAttempts = 3

// withVersionRetry runs fn, retrying the whole load-mutate-save cycle
// while it fails with networkorder.ErrVersionConflict. fn MUST reload
// the aggregate itself on every attempt (not reuse the instance a
// previous attempt loaded): the conflict means that instance is stale.
// Side effects inside fn must be safe to repeat — in this package they
// are: Planner hold calls are idempotent on the network ref, and
// Save+Publish commit atomically, so an aborted attempt published
// nothing.
func withVersionRetry(fn func() error) error {
	var err error
	for range versionConflictAttempts {
		if err = fn(); !errors.Is(err, networkorder.ErrVersionConflict) {
			return err
		}
	}
	return err
}
