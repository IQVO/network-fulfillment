package usecases

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
)

// ReturnInboundReceipts is the second half of the inbound loop (ADR 0028): when
// inbound-receiving closes a receipt for an ASN this service registered, it turns
// what the dock found into the vendor receipt and reports it to the network,
// exactly once per shipment. The facts come from the local projection of the
// receipt events (never a call to inbound-receiving); the report is retried with
// backoff until the network has it, and a refusal that can never succeed parks
// the receipt at ERROR.
type ReturnInboundReceipts struct {
	Receipts  ports.ReceiptProjection
	Gateway   ports.InboundGateway
	Clock     ports.Clock
	Logger    *slog.Logger
	BatchSize int
}

// ReturnResult says what one pass did.
type ReturnResult struct {
	Sent     int
	Retrying int
	Rejected int
}

// Execute reports every due receipt.
func (u *ReturnInboundReceipts) Execute(ctx context.Context) (ReturnResult, error) {
	batch := u.BatchSize
	if batch <= 0 {
		batch = defaultIntakeBatch
	}
	due, err := u.Receipts.DueReceipts(ctx, u.Clock.Now(), batch)
	if err != nil {
		return ReturnResult{}, err
	}
	var res ReturnResult
	var errs []error
	for _, facts := range due {
		if err := u.report(ctx, facts, &res); err != nil {
			errs = append(errs, err)
		}
	}
	return res, errors.Join(errs...)
}

func (u *ReturnInboundReceipts) report(ctx context.Context, facts contract.ReceiptFacts, res *ReturnResult) error {
	asn := AsnNumberFor(facts.Shipment.Ref)
	err := u.send(ctx, facts)
	switch {
	case err == nil:
		res.Sent++
		return u.Receipts.MarkReceiptSent(ctx, asn, u.Clock.Now())
	case errors.Is(err, contract.ErrReceiptRefused) || errors.Is(err, errNoCartons):
		res.Rejected++
		u.logger().Error("inbound receipt refused, it will not be retried", "shipment", facts.Shipment.Ref, "asn", asn, "err", err)
		return u.Receipts.MarkReceiptRejected(ctx, asn, err.Error())
	}
	res.Retrying++
	wait := IntakeBackoff(facts.Attempts + 1)
	u.logger().Warn("inbound receipt not reported yet, retrying", "shipment", facts.Shipment.Ref, "asn", asn,
		"attempt", facts.Attempts+1, "retryIn", wait, "err", err)
	return u.Receipts.MarkReceiptRetry(ctx, asn, err.Error(), u.Clock.Now().Add(wait))
}

func (u *ReturnInboundReceipts) send(ctx context.Context, facts contract.ReceiptFacts) error {
	receipt, err := BuildReceipt(facts, u.logger())
	if err != nil {
		return err
	}
	return u.Gateway.SubmitInboundReceipt(ctx, receipt)
}

func (u *ReturnInboundReceipts) logger() *slog.Logger {
	if u.Logger != nil {
		return u.Logger
	}
	return slog.Default()
}

var errNoCartons = errors.New("the receipt names no carton")

var complianceCodeOK = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}$`)

const maxComplianceCodes = 20

// ComplianceCode turns a carton discrepancy kind into the vendor API's UPPER_SNAKE
// code: CaseQtyMismatch becomes CASE_QTY_MISMATCH.
func ComplianceCode(kind string) string {
	var b strings.Builder
	for i, r := range kind {
		if i > 0 && r >= 'A' && r <= 'Z' {
			b.WriteByte('_')
		}
		b.WriteRune(r)
	}
	return strings.ToUpper(b.String())
}

// BuildReceipt maps what the dock found to the vendor receipt:
//
//   - a manifest carton that was received is RECEIVED, or DAMAGED when any of its
//     contents was; items are what was counted per GTIN, in eaches;
//   - a manifest carton that was never received is MISSING, with no items (the
//     close raises MissingCarton for it, but a closed receipt is the fact);
//   - a carton the dock accepted or flagged that is not on the manifest is
//     UNEXPECTED, with whatever was counted;
//   - the carton's other findings (CaseQtyMismatch, CartonOverLimit, DuplicateSscc,
//     ...) become its complianceCodes, sorted, without duplicates, at most 20.
//
// A finding with no SSCC (UnknownGtin, UnlabelledCarton) cannot be tied to a
// carton of the manifest: it is logged and not reported. A carton received
// without an SSCC has only an internal LPN the network does not know: likewise.
func BuildReceipt(f contract.ReceiptFacts, logger *slog.Logger) (contract.InboundReceipt, error) {
	received := map[string]contract.ReceivedCartonFact{}
	for _, c := range f.Cartons {
		if c.SSCC == "" {
			logger.Warn("a received carton without an SSCC is not reported to the network", "shipment", f.Shipment.Ref, "lpn", c.LPN)
			continue
		}
		received[c.SSCC] = c
	}
	codes := map[string]map[string]bool{}
	unexpected := map[string]bool{}
	for _, fd := range f.Findings {
		switch {
		case fd.Kind == contract.FindingMissingCarton:
			// A closed receipt reports every unreceived manifest carton as MISSING anyway.
		case fd.SSCC == "":
			logger.Warn("a carton finding without an SSCC is not reported to the network", "shipment", f.Shipment.Ref, "kind", fd.Kind)
		case fd.Kind == contract.FindingUnexpectedCarton:
			unexpected[fd.SSCC] = true
		default:
			if codes[fd.SSCC] == nil {
				codes[fd.SSCC] = map[string]bool{}
			}
			codes[fd.SSCC][ComplianceCode(fd.Kind)] = true
		}
	}

	out := contract.InboundReceipt{ShipmentRef: f.Shipment.Ref, SiteId: f.Shipment.SiteId, ReceivedAt: f.ClosedAt}
	onManifest := map[string]bool{}
	for _, m := range f.Shipment.Cartons {
		onManifest[m.SSCC] = true
		rc := contract.ReceiptCarton{SSCC: m.SSCC, Status: contract.ReceiptMissing, Items: []contract.ReceiptItem{}}
		if got, ok := received[m.SSCC]; ok {
			rc.Status, rc.Items = receivedStatus(got), itemsOf(got)
		}
		rc.ComplianceCodes = sortedCodes(codes[m.SSCC])
		out.Cartons = append(out.Cartons, rc)
	}
	for _, sscc := range unexpectedSSCCs(received, unexpected, onManifest) {
		rc := contract.ReceiptCarton{SSCC: sscc, Status: contract.ReceiptUnexpected, Items: []contract.ReceiptItem{}}
		if got, ok := received[sscc]; ok {
			rc.Items = itemsOf(got)
		}
		rc.ComplianceCodes = sortedCodes(codes[sscc])
		out.Cartons = append(out.Cartons, rc)
	}
	if len(out.Cartons) == 0 {
		return contract.InboundReceipt{}, fmt.Errorf("shipment %s: %w", f.Shipment.Ref, errNoCartons)
	}
	return out, nil
}

// unexpectedSSCCs lists the cartons that are not on the manifest and were seen at
// the dock (accepted, or flagged UnexpectedCarton), in SSCC order.
func unexpectedSSCCs(received map[string]contract.ReceivedCartonFact, flagged, onManifest map[string]bool) []string {
	set := map[string]bool{}
	for sscc := range received {
		if !onManifest[sscc] {
			set[sscc] = true
		}
	}
	for sscc := range flagged {
		if !onManifest[sscc] {
			set[sscc] = true
		}
	}
	out := make([]string, 0, len(set))
	for sscc := range set {
		out = append(out, sscc)
	}
	sort.Strings(out)
	return out
}

func receivedStatus(c contract.ReceivedCartonFact) contract.ReceiptStatus {
	for _, x := range c.Contents {
		if x.Condition == "Damaged" {
			return contract.ReceiptDamaged
		}
	}
	return contract.ReceiptReceived
}

// itemsOf sums the counted eaches per GTIN, every condition, in GTIN order.
func itemsOf(c contract.ReceivedCartonFact) []contract.ReceiptItem {
	sum := map[string]int64{}
	for _, x := range c.Contents {
		sum[x.GTIN] += x.Qty
	}
	items := make([]contract.ReceiptItem, 0, len(sum))
	for gtin, qty := range sum {
		items = append(items, contract.ReceiptItem{GTIN: gtin, QtyEaches: qty})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].GTIN < items[j].GTIN })
	return items
}

func sortedCodes(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for code := range set {
		if complianceCodeOK.MatchString(code) {
			out = append(out, code)
		}
	}
	sort.Strings(out)
	if len(out) > maxComplianceCodes {
		out = out[:maxComplianceCodes]
	}
	return out
}
