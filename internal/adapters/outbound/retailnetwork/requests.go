package retailnetwork

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// NodeDefinition is what the network needs to register one of our sites as a
// vendor node. The fleet holds none of it: it comes from NETWORK_NODES_FILE
// (nodes.go), the way the product dictionary comes from a file.
type NodeDefinition struct {
	SiteCode    string
	NodeType    string
	Subtype     string
	RegionID    string
	Timezone    string
	PostalCode  string
	CountryCode string
	Lat         *float64
	Lon         *float64
}

// idempotencyKey is deterministic per (kind, subject) (ADR 0026 Decision 4): the
// same write asked twice, in this process or after a restart, carries the same
// key, so the network can never create it twice. A key over the network's
// 128-character limit is shortened by hashing the subject, still deterministically.
func idempotencyKey(prefix, subject string) string {
	key := "nf-" + prefix + "-" + subject
	if len(key) <= maxIdempotencyKeyLength {
		return key
	}
	sum := sha256.Sum256([]byte(subject))
	return "nf-" + prefix + "-" + hex.EncodeToString(sum[:])
}

// The builders below produce the exact bytes of a request body. They are called
// once per ledger row: the stored bytes, not a rebuilt body, are what a retry
// sends.

func buildAcknowledgementBody(vendorID string, po PurchaseOrder, d wireDecision) ([]byte, error) {
	req := acknowledgementRequestDTO{
		VendorID: vendorID, PoNumber: po.PoNumber, Decision: d.Decision, ReasonCode: d.ReasonCode,
		Lines: make([]ackLineDTO, len(po.Lines)),
	}
	for i, l := range po.Lines {
		req.Lines[i] = ackLineDTO{LineNo: l.LineNo, Quantity: l.Quantity}
	}
	return json.Marshal(req)
}

func buildLabelBody(vendorID, poNumber string, packageCount int) ([]byte, error) {
	if packageCount < 1 {
		return nil, fmt.Errorf("%w (got %d)", ErrBadPackageCount, packageCount)
	}
	req := labelRequestDTO{VendorID: vendorID, PoNumber: poNumber, Packages: make([]labelPackageDTO, packageCount)}
	for i := range req.Packages {
		req.Packages[i] = labelPackageDTO{PackageNo: i + 1}
	}
	return json.Marshal(req)
}

func buildShipmentConfirmationBody(vendorID, poNumber string, shippedAt time.Time, trackingNumbers []string) ([]byte, error) {
	return json.Marshal(shipmentConfirmationRequestDTO{
		VendorID: vendorID, PoNumber: poNumber, ShippedAt: shippedAt.UTC(), TrackingNumbers: trackingNumbers,
	})
}

func buildNodeRegistrationBody(vendorID string, n NodeDefinition) ([]byte, error) {
	return json.Marshal(nodeRegistrationRequestDTO{
		VendorID: vendorID, VendorNodeCode: n.SiteCode, NodeType: n.NodeType, Subtype: n.Subtype,
		RegionID: n.RegionID, Timezone: n.Timezone, PostalCode: n.PostalCode, CountryCode: n.CountryCode,
		Lat: n.Lat, Lon: n.Lon,
	})
}
