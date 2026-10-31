package usecases

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// IntakeInboundShipments is the first half of the inbound loop (ADR 0028): for
// each owned site it polls the shipments the network says are on their way,
// records them, and registers an ASN in inbound-receiving for each one.
//
// It is restart-safe and replay-safe. The poll cursor and every shipment live in
// Postgres; the ASN number is derived from the shipment reference, so the same
// shipment is always the same ASN and a replay is a no-op on both sides. A
// shipment that cannot be registered yet (a GTIN product-master does not know, an
// SKU inbound-receiving has not caught up with, a transport failure) stays
// PENDING with the error and is retried with backoff; one whose data
// inbound-receiving refuses outright is REJECTED and surfaced at ERROR. Nothing
// is guessed.
type IntakeInboundShipments struct {
	Sites     []shared.SiteId
	Gateway   ports.InboundGateway
	Shipments ports.InboundShipmentRepo
	Resolver  ports.GtinResolver
	Registrar ports.AsnRegistrar
	Clock     ports.Clock
	Logger    *slog.Logger
	// BatchSize bounds how many due shipments one pass attempts (default 100).
	BatchSize int
}

// IntakeResult says what one pass did.
type IntakeResult struct {
	Polled     int // shipments the network returned
	New        int // of those, not seen before
	Registered int
	Retrying   int // PENDING after a failure
	Rejected   int
}

const (
	defaultIntakeBatch = 100
	backoffBase        = time.Minute
	backoffCap         = time.Hour
)

// IntakeBackoff is the wait before attempt number `attempts` (the number of
// failures so far, at least 1): one minute doubling, capped at one hour.
func IntakeBackoff(attempts int) time.Duration {
	d := backoffBase
	for i := 1; i < attempts; i++ {
		d *= 2
		if d >= backoffCap {
			return backoffCap
		}
	}
	return d
}

var plainRef = regexp.MustCompile(`^[A-Za-z0-9._-]{1,60}$`)

// AsnNumberFor is the ASN number of a shipment: deterministic, so a replay of the
// poll is the same ASN. A reference made only of characters an ASN number allows
// is used as it is; any other is hashed (a sanitised copy could collide).
func AsnNumberFor(shipmentRef string) string {
	if plainRef.MatchString(shipmentRef) {
		return "NF-" + shipmentRef
	}
	sum := sha256.Sum256([]byte(shipmentRef))
	return "NF-" + hex.EncodeToString(sum[:])[:40]
}

// Execute runs one pass: poll every site, then attempt every due shipment.
func (u *IntakeInboundShipments) Execute(ctx context.Context) (IntakeResult, error) {
	var res IntakeResult
	var errs []error
	for _, site := range u.Sites {
		if err := u.poll(ctx, site, &res); err != nil {
			errs = append(errs, fmt.Errorf("site %s: %w", site, err))
		}
	}
	if err := u.register(ctx, &res); err != nil {
		errs = append(errs, err)
	}
	return res, errors.Join(errs...)
}

func (u *IntakeInboundShipments) poll(ctx context.Context, site shared.SiteId, res *IntakeResult) error {
	cursor, err := u.Shipments.Cursor(ctx, site)
	if err != nil {
		return err
	}
	shipments, err := u.Gateway.PollInboundShipments(ctx, site, cursor)
	if err != nil {
		return err
	}
	newest := cursor
	now := u.Clock.Now()
	for _, s := range shipments {
		s.SiteId = site
		res.Polled++
		fresh, err := u.Shipments.Record(ctx, s, AsnNumberFor(s.Ref), now)
		if err != nil {
			return err // the cursor stays: the next poll returns this one again
		}
		if fresh {
			res.New++
		}
		if s.ShippedAt.After(newest) {
			newest = s.ShippedAt
		}
	}
	if newest.After(cursor) {
		return u.Shipments.AdvanceCursor(ctx, site, newest)
	}
	return nil
}

func (u *IntakeInboundShipments) register(ctx context.Context, res *IntakeResult) error {
	batch := u.BatchSize
	if batch <= 0 {
		batch = defaultIntakeBatch
	}
	due, err := u.Shipments.Due(ctx, u.Clock.Now(), batch)
	if err != nil {
		return err
	}
	var errs []error
	for _, rec := range due {
		if err := u.attempt(ctx, rec, res); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// attempt tries one shipment. Only a failure to RECORD the outcome is returned;
// the shipment's own failures are recorded on its row.
func (u *IntakeInboundShipments) attempt(ctx context.Context, rec contract.InboundRecord, res *IntakeResult) error {
	ref := rec.Shipment.Ref
	asn, err := BuildAsn(ctx, rec.Shipment, rec.AsnNumber, u.Resolver)
	if err == nil {
		err = u.Registrar.RegisterAsn(ctx, asn)
	}
	if err == nil {
		res.Registered++
		return u.Shipments.MarkRegistered(ctx, ref, u.Clock.Now())
	}
	var refused *contract.AsnRefusedError
	var bad *dataError
	if (errors.As(err, &refused) && !refused.Retryable) || errors.As(err, &bad) {
		res.Rejected++
		u.logger().Error("inbound shipment rejected, it will not be retried", "shipment", ref, "asn", rec.AsnNumber, "err", err)
		return u.Shipments.MarkRejected(ctx, ref, err.Error())
	}
	res.Retrying++
	wait := IntakeBackoff(rec.Attempts + 1)
	u.logger().Warn("inbound shipment not registered yet, retrying", "shipment", ref, "asn", rec.AsnNumber,
		"attempt", rec.Attempts+1, "retryIn", wait, "err", err)
	return u.Shipments.MarkRetry(ctx, ref, err.Error(), u.Clock.Now().Add(wait))
}

func (u *IntakeInboundShipments) logger() *slog.Logger {
	if u.Logger != nil {
		return u.Logger
	}
	return slog.Default()
}

// dataError is a verdict about a shipment's own data (no cartons, a GTIN that is
// not an each GTIN): waiting cannot fix it.
type dataError struct{ msg string }

func (e *dataError) Error() string { return e.msg }

// BuildAsn maps a shipment tree to the ASN of inbound-receiving: cartons keep
// their SSCC, parent SSCC, kind and contents; the lines are the per-SKU roll-up
// of the carton contents in eaches, numbered 1..n in order of first appearance;
// every GTIN becomes a SKU through product-master (an unknown one is an error
// the caller retries, never a guess); ownerRef is the seller.
func BuildAsn(ctx context.Context, s contract.InboundShipment, asnNumber string, resolver ports.GtinResolver) (contract.AsnRegistration, error) {
	if len(s.Cartons) == 0 {
		return contract.AsnRegistration{}, &dataError{"the shipment has no cartons"}
	}
	supplier := s.OwnerRef
	if supplier == "" {
		supplier = "NETWORK"
	}
	asn := contract.AsnRegistration{AsnNumber: asnNumber, SupplierRef: supplier, OwnerRef: s.OwnerRef}
	lineOf := map[string]int{}
	resolved := map[string]contract.GtinResolution{}
	for _, c := range s.Cartons {
		out := contract.AsnCarton{SSCC: c.SSCC, ParentSSCC: c.ParentSSCC, Kind: c.Kind, GrossWeightG: c.GrossWeightG, Dims: c.Dims}
		for _, it := range c.Contents {
			r, ok := resolved[it.GTIN]
			if !ok {
				var err error
				if r, err = resolver.ResolveGtin(ctx, it.GTIN); err != nil {
					return contract.AsnRegistration{}, fmt.Errorf("GTIN %s: %w", it.GTIN, err)
				}
				resolved[it.GTIN] = r
			}
			if r.Level != "EACH" {
				return contract.AsnRegistration{}, &dataError{fmt.Sprintf("GTIN %s is a %s GTIN, the manifest counts eaches of an each GTIN", it.GTIN, r.Level)}
			}
			line, seen := lineOf[r.SKU]
			if !seen {
				line = len(asn.Lines) + 1
				lineOf[r.SKU] = line
				asn.Lines = append(asn.Lines, contract.AsnLine{LineNo: line, SKU: r.SKU})
			}
			asn.Lines[line-1].ExpectedQty += it.QtyEaches
			out.Contents = append(out.Contents, contract.AsnCartonContent{
				LineNo: line, SKU: r.SKU, GTIN: it.GTIN, Qty: it.QtyEaches, Lot: it.Lot, Expiry: it.Expiry,
			})
		}
		asn.Cartons = append(asn.Cartons, out)
	}
	return asn, nil
}
