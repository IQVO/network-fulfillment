package retailnetwork

import (
	"context"
	"fmt"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

var pathShipmentConfirmations = []string{"vendor", "shipment-confirmations"}

// RequestLabel asks for the labels of an order that leaves in packageCount
// packages and persists them. It is idempotent per ref: labels already
// persisted are returned as they are (the first count wins, since the same key
// with another body would be idempotency-key-reused), and a retry of an
// interrupted request carries the same key, so the network issues one set.
func (g *LiveGateway) RequestLabel(ctx context.Context, ref shared.NetworkRef, packageCount int) ([]contract.LabelResult, error) {
	if packageCount < 1 {
		return nil, fmt.Errorf("%w (got %d)", ErrBadPackageCount, packageCount)
	}
	have, err := g.store.Labels(ctx, ref)
	if err != nil {
		return nil, err
	}
	if len(have) > 0 {
		return labelResults(have), nil
	}
	po, err := g.orderFor(ctx, ref)
	if err != nil {
		return nil, err
	}
	body, err := buildLabelBody(g.client.vendorID, po.PoNumber, packageCount)
	if err != nil {
		return nil, err
	}
	resp, err := g.client.requestLabels(ctx, idempotencyKey("label", string(ref)), body)
	if err != nil {
		return nil, err
	}
	labels, err := labelsFromResponse(resp, packageCount)
	if err != nil {
		return nil, err
	}
	if err := g.store.SaveLabels(ctx, ref, labels); err != nil {
		return nil, err
	}
	return labelResults(labels), nil
}

// labelsFromResponse checks the network's answer against the request: one label
// per package, each with a tracking number (the shipment confirmation needs
// exactly these). A package number the network omits is its position.
func labelsFromResponse(resp labelResponseDTO, packages int) ([]Label, error) {
	if len(resp.Labels) != packages {
		return nil, fmt.Errorf("retail-network: requestLabel: asked for %d package labels, the network returned %d", packages, len(resp.Labels))
	}
	out := make([]Label, packages)
	for i, l := range resp.Labels {
		if l.TrackingNumber == "" {
			return nil, fmt.Errorf("retail-network: requestLabel: the label of package %d has no tracking number", i+1)
		}
		no := l.PackageNo
		if no == 0 {
			no = i + 1
		}
		out[i] = Label{PackageNo: no, LabelRef: l.LabelRef, TrackingNumber: l.TrackingNumber, Carrier: l.Carrier}
	}
	return out, nil
}

func labelResults(labels []Label) []contract.LabelResult {
	out := make([]contract.LabelResult, len(labels))
	for i, l := range labels {
		out[i] = contract.LabelResult{LabelRef: l.LabelRef, TrackingNumber: l.TrackingNumber, Carrier: l.Carrier}
	}
	return out
}

// SubmitShipmentConfirmation tells the network the order shipped at shippedAt,
// with the tracking numbers of the labels persisted by RequestLabel (no label is
// ErrNoLabel). The ledger row keeps the first body, so the first shippedAt wins
// and a second call sends nothing. The transaction's outcome is recorded for
// operators; a network FAILURE is logged, not returned, because no use case
// reconciles a confirmation (ADR 0026, Consequences).
func (g *LiveGateway) SubmitShipmentConfirmation(ctx context.Context, ref shared.NetworkRef, shippedAt time.Time) error {
	labels, err := g.store.Labels(ctx, ref)
	if err != nil {
		return err
	}
	if len(labels) == 0 {
		return fmt.Errorf("%w: %s", ErrNoLabel, ref)
	}
	po, err := g.orderFor(ctx, ref)
	if err != nil {
		return err
	}
	body, err := buildShipmentConfirmationBody(g.client.vendorID, po.PoNumber, shippedAt, trackingNumbers(labels))
	if err != nil {
		return err
	}
	row, err := g.store.BeginSubmission(ctx, Submission{
		Kind: KindShipmentConfirmation, Subject: string(ref), IdempotencyKey: idempotencyKey("ship", string(ref)), Body: body,
	})
	if err != nil {
		return err
	}
	if row, err = g.send(ctx, row, "submitShipmentConfirmation", pathShipmentConfirmations); err != nil {
		return err
	}
	g.recordConfirmationOutcome(ctx, ref, row)
	return nil
}

func trackingNumbers(labels []Label) []string {
	out := make([]string, len(labels))
	for i, l := range labels {
		out[i] = l.TrackingNumber
	}
	return out
}

// recordConfirmationOutcome stores what the network made of the confirmation,
// best effort: the write is already accepted, so a failing read only means the
// next call tries again.
func (g *LiveGateway) recordConfirmationOutcome(ctx context.Context, ref shared.NetworkRef, row Submission) {
	if row.TransactionID == "" || row.Status.Terminal() {
		return
	}
	status, err := g.reconcile(ctx, row)
	if err != nil {
		g.logger.Warn("retail-network: could not read the outcome of a shipment confirmation", "networkRef", sanitizeLog(string(ref)), "err", err)
		return
	}
	if status != contract.SubmissionFailure {
		return
	}
	failed, _, _ := g.store.Submission(ctx, row.Kind, row.Subject)
	g.logger.Error("retail-network: the network failed a shipment confirmation; the order stays accepted there until an operator acts",
		"networkRef", sanitizeLog(string(ref)), "errors", failed.Errors)
}
