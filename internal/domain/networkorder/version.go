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

// RestoreVersion sets the version loaded from storage. Only a repository
// calls it, alongside Rehydrate: the version is a persistence fact the
// domain never derives (see RestoreHold for the same convention).
func (o *NetworkOrder) RestoreVersion(version int) { o.version = version }

// AdvanceVersion moves to the next version after a successful
// conditional write. Repositories call it on Save success so a use case
// that saves the same instance twice (ReceiveNetworkDemand records the
// hold, then the answer) does not trip over its own version.
func (o *NetworkOrder) AdvanceVersion() { o.version++ }
