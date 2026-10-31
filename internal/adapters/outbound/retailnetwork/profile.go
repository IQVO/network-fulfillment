package retailnetwork

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// The declaration of a node's operational profile (ADR 0027). The gateway owns
// the revision: it only goes up, is persisted before the first call that carries
// it, and is bumped only when the content (its fingerprint) changes.

var _ ports.ProfileHistory = (*LiveGateway)(nil)

// profileSubject is the ledger subject of one attempt at one revision.
func profileSubject(site string, revision, attempt int) string {
	return fmt.Sprintf("%s#%d#%d", site, revision, attempt)
}

// profileKey is deterministic per (node, revision, attempt): the first attempt
// is nf-profile-<site>-<revision>, a retry of the same revision adds -<attempt>
// because the network replays a failed transaction for the same key.
func profileKey(site string, revision, attempt int) string {
	if attempt <= 1 {
		return idempotencyKey("profile", fmt.Sprintf("%s-%d", site, revision))
	}
	return idempotencyKey("profile", fmt.Sprintf("%s-%d-%d", site, revision, attempt))
}

func profilePath(nodeID string) []string { return []string{"vendor", "nodes", nodeID, "profile"} }

// DeclareNodeProfile declares the profile of p.SiteId's node and settles what it
// can in one call: it takes the next revision when the content changed, writes
// it, and reads the transaction. The outcome says which of those happened.
//
// A profile the network could not take is ErrBadProfile before any I/O and
// before any revision is taken; a site with no node is
// contract.ErrSiteNotRegistered. A refusal by the network is a result
// (ProfileFailed, with its error list), a call that could not be made is an error.
func (g *LiveGateway) DeclareNodeProfile(ctx context.Context, p contract.NodeProfile) (contract.ProfileResult, error) {
	if p.Fingerprint == "" {
		return contract.ProfileResult{}, fmt.Errorf("%w: no fingerprint", ErrBadProfile)
	}
	if _, err := buildNodeProfileBody(g.client.vendorID, 1, p); err != nil {
		return contract.ProfileResult{}, err
	}
	site := string(p.SiteId)
	nodeID, mapped, err := g.store.NodeIDForSite(ctx, site)
	if err != nil {
		return contract.ProfileResult{}, err
	}
	if !mapped {
		return contract.ProfileResult{}, fmt.Errorf("%w: %s", contract.ErrSiteNotRegistered, site)
	}
	st, err := g.profileState(ctx, p)
	if err != nil {
		return contract.ProfileResult{}, err
	}
	if st.Fingerprint != p.Fingerprint {
		// Another declarer took this revision for other content between our read
		// and our write. Its content is being declared; ours waits for the next tick.
		return contract.ProfileResult{Outcome: contract.ProfilePending, Revision: st.Revision}, nil
	}
	return g.settleProfile(ctx, nodeID, p, st, true)
}

// profileState returns the persisted state for p's content: the one in force when
// the content is unchanged, else the next revision, persisted by a guarded write.
func (g *LiveGateway) profileState(ctx context.Context, p contract.NodeProfile) (NodeProfileState, error) {
	site := string(p.SiteId)
	st, found, err := g.store.NodeProfileState(ctx, site)
	if err != nil {
		return NodeProfileState{}, err
	}
	if found && st.Fingerprint == p.Fingerprint {
		return st, nil
	}
	next := 1
	if found {
		next = st.Revision + 1
	}
	return g.advanceProfile(ctx, p, next, 1)
}

func (g *LiveGateway) advanceProfile(ctx context.Context, p contract.NodeProfile, revision, attempt int) (NodeProfileState, error) {
	content, err := encodeProfileContent(p)
	if err != nil {
		return NodeProfileState{}, err
	}
	return g.store.AdvanceNodeProfile(ctx, NodeProfileState{
		SiteCode: string(p.SiteId), Revision: revision, Attempt: attempt, Fingerprint: p.Fingerprint, Content: content,
	})
}

// settleProfile takes the ledger row of (revision, attempt) as far as the network
// lets it go in this call. mayRecover bounds the stale-revision recovery to one
// per call.
func (g *LiveGateway) settleProfile(ctx context.Context, nodeID string, p contract.NodeProfile, st NodeProfileState, mayRecover bool) (contract.ProfileResult, error) {
	site := string(p.SiteId)
	body, err := buildNodeProfileBody(g.client.vendorID, st.Revision, p)
	if err != nil {
		return contract.ProfileResult{}, err
	}
	row, err := g.store.BeginSubmission(ctx, Submission{
		Kind: KindNodeProfile, Subject: profileSubject(site, st.Revision, st.Attempt),
		IdempotencyKey: profileKey(site, st.Revision, st.Attempt), Body: body,
	})
	if err != nil {
		return contract.ProfileResult{}, err
	}
	switch row.Status {
	case StatusSuccess:
		return contract.ProfileResult{Outcome: contract.ProfileUnchanged, Revision: st.Revision}, nil
	case StatusFailure:
		return g.afterFailure(ctx, nodeID, p, st, row, false, mayRecover)
	}
	sentNow := row.TransactionID == ""
	row, err = g.sendWith(ctx, row, http.MethodPut, "declareNodeProfile", profilePath(nodeID))
	if errors.Is(err, ErrWriteRefused) {
		g.logProfileFailure(site, st.Revision, row.Errors, err.Error(), true)
		return contract.ProfileResult{Outcome: contract.ProfileFailed, Revision: st.Revision, Errors: err.Error()}, nil
	}
	if err != nil {
		return contract.ProfileResult{}, err
	}
	return g.reconcileProfile(ctx, nodeID, p, st, row, sentNow, mayRecover)
}

// reconcileProfile reads the row's transaction by its persisted id.
func (g *LiveGateway) reconcileProfile(ctx context.Context, nodeID string, p contract.NodeProfile, st NodeProfileState, row Submission, sentNow, mayRecover bool) (contract.ProfileResult, error) {
	tx, err := g.client.getTransaction(ctx, row.TransactionID)
	if err != nil {
		if sentNow {
			// The write is accepted and its id is stored: the next tick reads it.
			g.logger.Warn("retail-network: could not read the outcome of a profile declaration yet; the next tick will",
				"site", p.SiteId, "revision", st.Revision, "err", err)
			return contract.ProfileResult{Outcome: contract.ProfileDeclared, Revision: st.Revision}, nil
		}
		return contract.ProfileResult{}, err
	}
	switch tx.Status {
	case txSuccess:
		if err := g.store.RecordOutcome(ctx, row.Kind, row.Subject, StatusSuccess, ""); err != nil {
			return contract.ProfileResult{}, err
		}
		return contract.ProfileResult{Outcome: contract.ProfileConfirmed, Revision: st.Revision}, nil
	case txFailure:
		errs := joinErrors(tx.Errors)
		if err := g.store.RecordOutcome(ctx, row.Kind, row.Subject, StatusFailure, errs); err != nil {
			return contract.ProfileResult{}, err
		}
		row.Status, row.Errors = StatusFailure, errs
		return g.afterFailure(ctx, nodeID, p, st, row, true, mayRecover)
	}
	if sentNow {
		return contract.ProfileResult{Outcome: contract.ProfileDeclared, Revision: st.Revision}, nil
	}
	return contract.ProfileResult{Outcome: contract.ProfilePending, Revision: st.Revision}, nil
}

type profileFailure int

const (
	failureRetryable profileFailure = iota // another attempt at the same revision may succeed
	failureStale                           // the network holds a revision at or above ours
	failureFinal                           // the same content can never succeed
)

// classifyProfileFailure reads what the network said about a failed row. A row
// the network refused outright (no transaction was ever created) is final: its
// body was judged and rejected.
func classifyProfileFailure(row Submission) profileFailure {
	switch {
	case row.TransactionID == "":
		return failureFinal
	case strings.Contains(row.Errors, errStaleProfileRevision):
		return failureStale
	case strings.Contains(row.Errors, errIncompleteOperationalProfile),
		strings.Contains(row.Errors, errCutoffOutsideOperatingHours),
		strings.Contains(row.Errors, "validation-failed"):
		return failureFinal
	default:
		return failureRetryable
	}
}

// afterFailure decides what a FAILURE row means. observedNow is true when this
// very call learned of the failure: it is logged at ERROR with the network's list
// and a retryable one waits for the next tick; on a later tick (observedNow
// false) a retryable failure becomes the next attempt at the same revision.
func (g *LiveGateway) afterFailure(ctx context.Context, nodeID string, p contract.NodeProfile, st NodeProfileState, row Submission, observedNow, mayRecover bool) (contract.ProfileResult, error) {
	failed := contract.ProfileResult{Outcome: contract.ProfileFailed, Revision: st.Revision, Errors: row.Errors}
	switch classifyProfileFailure(row) {
	case failureStale:
		g.logger.Warn("retail-network: the network already holds a profile revision at or above ours (stale); reading its revision to continue above it",
			"site", p.SiteId, "revision", st.Revision, "errors", row.Errors)
		if !mayRecover {
			return failed, nil
		}
		return g.recoverStale(ctx, nodeID, p, st)
	case failureFinal:
		g.logProfileFailure(string(p.SiteId), st.Revision, row.Errors, "", observedNow)
		return failed, nil
	}
	g.logProfileFailure(string(p.SiteId), st.Revision, row.Errors, "", observedNow)
	if observedNow {
		return failed, nil
	}
	next, err := g.advanceProfile(ctx, p, st.Revision, st.Attempt+1)
	if err != nil {
		return contract.ProfileResult{}, err
	}
	if next.Fingerprint != p.Fingerprint {
		return contract.ProfileResult{Outcome: contract.ProfilePending, Revision: next.Revision}, nil
	}
	return g.settleProfile(ctx, nodeID, p, next, mayRecover)
}

// logProfileFailure reports a refused or failed profile with the network's error
// list. The call that learns of it logs at ERROR; a later tick that finds the
// same standing refusal says so at WARN, so an operator sees it once loudly and
// then as long as it lasts without a flood of errors.
func (g *LiveGateway) logProfileFailure(site string, revision int, networkErrors, fallback string, observedNow bool) {
	if networkErrors == "" {
		networkErrors = fallback
	}
	if observedNow {
		g.logger.Error("retail-network: the network failed a node profile declaration",
			"site", site, "revision", revision, "errors", networkErrors)
		return
	}
	g.logger.Warn("retail-network: the node profile declaration stays failed until the profile changes or a retry succeeds",
		"site", site, "revision", revision, "errors", networkErrors)
}

// recoverStale re-reads the node's current revision from the network and declares
// the same content above it. The view that serves the revision can lag, or show
// none; then the revision climbs by one per tick until it clears the network's.
func (g *LiveGateway) recoverStale(ctx context.Context, nodeID string, p contract.NodeProfile, st NodeProfileState) (contract.ProfileResult, error) {
	node, err := g.client.getNode(ctx, nodeID)
	if err != nil {
		return contract.ProfileResult{}, err
	}
	next, err := g.advanceProfile(ctx, p, max(st.Revision, node.ProfileRevision)+1, 1)
	if err != nil {
		return contract.ProfileResult{}, err
	}
	if next.Fingerprint != p.Fingerprint {
		return contract.ProfileResult{Outcome: contract.ProfilePending, Revision: next.Revision}, nil
	}
	return g.settleProfile(ctx, nodeID, p, next, false)
}

// LastDeclared returns the profile last declared for the site, in our terms
// (ports.ProfileHistory): the persisted content of the revision in force.
func (g *LiveGateway) LastDeclared(ctx context.Context, siteId shared.SiteId) (contract.NodeProfile, bool, error) {
	st, found, err := g.store.NodeProfileState(ctx, string(siteId))
	if err != nil || !found {
		return contract.NodeProfile{}, false, err
	}
	p, err := decodeProfileContent(st.Content)
	if err != nil {
		return contract.NodeProfile{}, false, err
	}
	p.SiteId, p.Fingerprint = siteId, st.Fingerprint
	return p, true, nil
}

// ---- the declared content, persisted in the fleet's own terms ----

type profileContentDTO struct {
	Timezone         string             `json:"timezone"`
	Cutoffs          []profileCutoffDTO `json:"cutoffs"`
	HandlingNanos    int64              `json:"handlingNanos"`
	ThroughputPerDay int                `json:"throughputPerDay"`
}

type profileCutoffDTO struct {
	LocalTime string `json:"localTime"`
	Days      []int  `json:"days"`
	MaxOrders int    `json:"maxOrders"`
}

func encodeProfileContent(p contract.NodeProfile) ([]byte, error) {
	dto := profileContentDTO{
		Timezone: p.Timezone, HandlingNanos: int64(p.HandlingTime), ThroughputPerDay: p.ThroughputPerDay,
		Cutoffs: make([]profileCutoffDTO, len(p.Cutoffs)),
	}
	for i, c := range p.Cutoffs {
		days := make([]int, len(c.Days))
		for j, d := range c.Days {
			days[j] = int(d)
		}
		dto.Cutoffs[i] = profileCutoffDTO{LocalTime: c.LocalTime, Days: days, MaxOrders: c.MaxOrders}
	}
	return json.Marshal(dto)
}

func decodeProfileContent(raw []byte) (contract.NodeProfile, error) {
	var dto profileContentDTO
	if err := json.Unmarshal(raw, &dto); err != nil {
		return contract.NodeProfile{}, fmt.Errorf("retail-network: the persisted profile content is unreadable: %w", err)
	}
	p := contract.NodeProfile{
		Timezone: dto.Timezone, HandlingTime: time.Duration(dto.HandlingNanos), ThroughputPerDay: dto.ThroughputPerDay,
		Cutoffs: make([]contract.ProfileCutoff, len(dto.Cutoffs)),
	}
	for i, c := range dto.Cutoffs {
		days := make([]time.Weekday, len(c.Days))
		for j, d := range c.Days {
			days[j] = time.Weekday(d)
		}
		p.Cutoffs[i] = contract.ProfileCutoff{LocalTime: c.LocalTime, Days: days, MaxOrders: c.MaxOrders}
	}
	return p, nil
}
