package retailnetwork

import (
	"fmt"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// wireDecision is what an acknowledgement says on the wire.
type wireDecision struct {
	Decision   string
	ReasonCode string
}

// ackDecision maps OUR answer to the network's (ADR 0026 Decision 3). send is
// false when there is nothing to say: the window is closed, the network expires
// the order itself, and a write now would be refused (ack-window-closed) with a
// reason that is not true. An answer that cannot be sent at all is an error
// before any I/O.
//
// It is an if-chain on purpose: each rule is one branch the mutation gate can
// kill, where a tagless switch reports every case as not covered.
func ackDecision(a contract.Acknowledgement) (d wireDecision, send bool, err error) {
	if a.Accepted {
		if a.Reason != "" {
			return wireDecision{}, false, fmt.Errorf("%w: an acceptance carries no reason (got %q)", ErrBadAcknowledgement, a.Reason)
		}
		return wireDecision{Decision: decisionAccept}, true, nil
	}
	if a.Reason == shared.RejectionReasonInfeasibleDeadline {
		return wireDecision{Decision: decisionReject, ReasonCode: reasonCannotMeetShipBy}, true, nil
	}
	if a.Reason == shared.RejectionReasonUntranslatableSKU {
		return wireDecision{Decision: decisionReject, ReasonCode: reasonUnknownListing}, true, nil
	}
	if a.Reason == shared.RejectionReasonAcknowledgementDeadlineMissed {
		return wireDecision{}, false, nil
	}
	if a.Reason == "" {
		return wireDecision{}, false, fmt.Errorf("%w: a rejection needs a reason", ErrBadAcknowledgement)
	}
	return wireDecision{}, false, fmt.Errorf("%w: reason %q is not one the network is told", ErrBadAcknowledgement, a.Reason)
}

// submissionStatus maps the network's transaction status to the port's.
// Anything it does not recognise is PENDING: the reconciliation asks again
// rather than guessing a verdict.
func submissionStatus(status string) contract.SubmissionStatusValue {
	if status == txSuccess {
		return contract.SubmissionSuccess
	}
	if status == txFailure {
		return contract.SubmissionFailure
	}
	return contract.SubmissionPending
}

// ledgerStatus maps the network's transaction status to the ledger's.
func ledgerStatus(status string) SubmissionStatus {
	if status == txSuccess {
		return StatusSuccess
	}
	if status == txFailure {
		return StatusFailure
	}
	return StatusProcessing
}
