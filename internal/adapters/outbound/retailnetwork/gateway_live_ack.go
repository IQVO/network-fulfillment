package retailnetwork

import (
	"context"
	"errors"
	"fmt"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

var pathAcknowledgements = []string{"vendor", "acknowledgements"}

func (g *LiveGateway) orderFor(ctx context.Context, ref shared.NetworkRef) (PurchaseOrder, error) {
	po, ok, err := g.store.PurchaseOrder(ctx, ref)
	if err != nil {
		return PurchaseOrder{}, err
	}
	if !ok {
		return PurchaseOrder{}, fmt.Errorf("%w: %s", ErrUnknownRef, ref)
	}
	return po, nil
}

// SubmitAcknowledgement answers the order in full: accept, or reject with the
// reason mapped by ackDecision. The ledger row, with the exact body, is written
// before the call; a second call finds the transaction id and sends nothing. A
// ACKNOWLEDGEMENT_DEADLINE_MISSED answer is not written at all (the network
// expires the order itself).
func (g *LiveGateway) SubmitAcknowledgement(ctx context.Context, ref shared.NetworkRef, answer contract.Acknowledgement) error {
	decision, send, err := ackDecision(answer)
	if err != nil {
		return err
	}
	if !send {
		g.logger.Info("retail-network: no acknowledgement written, the window is closed and the network expires the order itself", "networkRef", ref)
		return nil
	}
	po, err := g.orderFor(ctx, ref)
	if err != nil {
		return err
	}
	body, err := buildAcknowledgementBody(g.client.vendorID, po, decision)
	if err != nil {
		return err
	}
	row, err := g.store.BeginSubmission(ctx, Submission{
		Kind: KindAcknowledgement, Subject: string(ref), IdempotencyKey: idempotencyKey("ack", string(ref)), Body: body,
	})
	if err != nil {
		return err
	}
	_, err = g.sendAcknowledgement(ctx, po, row)
	return err
}

// sendAcknowledgement sends the stored answer if it was never sent. A 409
// already-acknowledged (the key outlived its 24 hours) means the network holds
// the answer: the order's own acknowledgement gives the transaction to follow.
func (g *LiveGateway) sendAcknowledgement(ctx context.Context, po PurchaseOrder, row Submission) (Submission, error) {
	row, err := g.send(ctx, row, "submitAcknowledgement", pathAcknowledgements)
	if Problem(err) != problemAlreadyAcknowledged {
		return row, err
	}
	got, err := g.client.getPurchaseOrder(ctx, po.PoNumber)
	if err != nil || got.Acknowledgement == nil || got.Acknowledgement.TransactionID == "" {
		return row, err
	}
	txID := got.Acknowledgement.TransactionID
	if err := g.store.RecordTransaction(ctx, row.Kind, row.Subject, txID); err != nil {
		return row, err
	}
	row.TransactionID, row.Status = txID, StatusProcessing
	return row, nil
}

// SubmissionStatus reconciles the acknowledgement of ref against the network's
// transaction record. No ledger row is NOT_SENT; a row whose call never got
// through is sent again from its stored body and key first.
func (g *LiveGateway) SubmissionStatus(ctx context.Context, ref shared.NetworkRef) (contract.SubmissionStatusValue, error) {
	row, found, err := g.store.Submission(ctx, KindAcknowledgement, string(ref))
	if err != nil {
		return "", err
	}
	if !found {
		return contract.SubmissionNotSent, nil
	}
	if row.Status.Terminal() {
		return ledgerToPort(row.Status), nil
	}
	if row.TransactionID == "" {
		if row, err = g.resend(ctx, ref, row); err != nil {
			return refusedAsFailure(err)
		}
		if row.TransactionID == "" {
			return contract.SubmissionPending, nil
		}
	}
	return g.reconcile(ctx, row)
}

func (g *LiveGateway) resend(ctx context.Context, ref shared.NetworkRef, row Submission) (Submission, error) {
	po, err := g.orderFor(ctx, ref)
	if err != nil {
		return row, err
	}
	return g.sendAcknowledgement(ctx, po, row)
}

// refusedAsFailure turns a definitive refusal into the FAILURE verdict the
// reconciliation acts on; any other error stays an error and is retried.
func refusedAsFailure(err error) (contract.SubmissionStatusValue, error) {
	if errors.Is(err, ErrWriteRefused) {
		return contract.SubmissionFailure, nil
	}
	return "", err
}

// reconcile asks the network about the row's transaction and stores the verdict.
func (g *LiveGateway) reconcile(ctx context.Context, row Submission) (contract.SubmissionStatusValue, error) {
	tx, err := g.client.getTransaction(ctx, row.TransactionID)
	if err != nil {
		return "", err
	}
	if err := g.store.RecordOutcome(ctx, row.Kind, row.Subject, ledgerStatus(tx.Status), joinErrors(tx.Errors)); err != nil {
		return "", err
	}
	return submissionStatus(tx.Status), nil
}

// ledgerToPort maps a settled ledger status to the port's. Anything not
// terminal is PENDING.
func ledgerToPort(status SubmissionStatus) contract.SubmissionStatusValue {
	if status == StatusSuccess {
		return contract.SubmissionSuccess
	}
	if status == StatusFailure {
		return contract.SubmissionFailure
	}
	return contract.SubmissionPending
}
