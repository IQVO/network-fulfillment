package networkorder

import "errors"

// ErrVersionConflict is the optimistic-locking signal: the row this
// aggregate was loaded from has since been written by someone else (a
// sweep racing the poller, a REST confirmation racing either). The
// writer must reload and re-apply its transition — see the
// usecases package's withVersionRetry — or, on the REST surface,
// answer 409 and let the caller decide.
var ErrVersionConflict = errors.New("network order was modified concurrently")

// Version is the aggregate's optimistic-locking version, bumped by one
// on every persisted write. A freshly Received order is version 0 until
// its first Save; a Rehydrated one carries whatever the row said.
func (o *NetworkOrder) Version() int { return o.version }

// RestoreVersion sets the version read from storage. Only a repository
// calls it: alongside Rehydrate when loading, and again after its own
// conditional write lands — the row's new version is then the fact to
// carry, and keeps the same instance saveable a second time. The
// version is a persistence fact the domain never derives (see
// RestoreHold for the same convention).
func (o *NetworkOrder) RestoreVersion(version int) {
	o.version = version
	o.loaded = true
}

// LoadedFromStorage reports whether this instance has seen a row:
// Rehydrate built it, or a repository read or wrote its version. Save
// guards exactly these writes with the conditional UPDATE — only a
// writer that has seen the row can be stale. A false return is a
// freshly received aggregate that has never read storage: an intake
// write, applied by the upsert exactly as it was before versions
// existed (the arrival-fixed columns are absent from the SET list, so
// a re-delivered demand still cannot move the clock the first arrival
// started).
func (o *NetworkOrder) LoadedFromStorage() bool { return o.loaded }

// AdvanceVersion moves to the next version after a successful
// conditional write. Repositories call it on Save success so a use case
// that saves the same instance twice (ReceiveNetworkDemand records the
// hold, then the answer) does not trip over its own version.
func (o *NetworkOrder) AdvanceVersion() { o.version++ }
