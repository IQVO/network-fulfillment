package retailnetwork

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
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

// buildNodeProfileBody turns a profile in our terms into the Vendor API's body at
// the given revision (ADR 0027 §3). It checks what the network would refuse
// (ErrBadProfile) before any I/O. The handling time is rounded UP to whole
// minutes: the figure is a p95, and a declaration must never promise less time
// than it needs. A cutoff with no known ceiling (MaxOrders 0) omits maxOrders:
// unknown is not zero. Operating hours are the days that have a cutoff, open all
// day, because the fleet holds none of its own.
func buildNodeProfileBody(vendorID string, revision int, p contract.NodeProfile) ([]byte, error) {
	cutoffs, err := profileCutoffsToWire(p.Cutoffs)
	if err != nil {
		return nil, err
	}
	switch {
	case revision < 1:
		return nil, fmt.Errorf("%w: revision %d", ErrBadProfile, revision)
	case p.Timezone == "":
		return nil, fmt.Errorf("%w: no timezone", ErrBadProfile)
	case p.HandlingTime <= 0:
		return nil, fmt.Errorf("%w: no handling time", ErrBadProfile)
	case p.ThroughputPerDay < 1:
		return nil, fmt.Errorf("%w: no throughput", ErrBadProfile)
	}
	return json.Marshal(nodeProfileRequestDTO{
		VendorID: vendorID, ProfileRevision: revision, Timezone: p.Timezone,
		OperatingHoursByDay: openDaysOf(cutoffs),
		HandlingTimeMinutes: int((p.HandlingTime + time.Minute - 1) / time.Minute),
		Cutoffs:             cutoffs,
		ThroughputCap:       throughputCapDTO{Value: p.ThroughputPerDay, TimeUnit: timeUnitDay, Unit: unitOrder},
	})
}

var localTimePattern = regexp.MustCompile(`^([01][0-9]|2[0-3]):[0-5][0-9]$`)

// profileCutoffsToWire validates the cutoffs and names their days the way the
// network does (lowercase English).
func profileCutoffsToWire(cutoffs []contract.ProfileCutoff) ([]cutoffSlotDTO, error) {
	if len(cutoffs) == 0 {
		return nil, fmt.Errorf("%w: no cutoff", ErrBadProfile)
	}
	out := make([]cutoffSlotDTO, len(cutoffs))
	taken := map[string]bool{} // "<time>|<weekday>"
	for i, c := range cutoffs {
		if !localTimePattern.MatchString(c.LocalTime) {
			return nil, fmt.Errorf("%w: cutoff time %q is not HH:mm", ErrBadProfile, c.LocalTime)
		}
		if c.MaxOrders < 0 {
			return nil, fmt.Errorf("%w: cutoff %s has a negative ceiling", ErrBadProfile, c.LocalTime)
		}
		if len(c.Days) == 0 {
			return nil, fmt.Errorf("%w: cutoff %s applies to no day", ErrBadProfile, c.LocalTime)
		}
		days := make([]string, len(c.Days))
		for j, d := range c.Days {
			if d < time.Sunday || d > time.Saturday {
				return nil, fmt.Errorf("%w: cutoff %s names weekday %d", ErrBadProfile, c.LocalTime, int(d))
			}
			name := strings.ToLower(d.String())
			if taken[c.LocalTime+"|"+name] {
				return nil, fmt.Errorf("%w: two cutoffs at %s on %s", ErrBadProfile, c.LocalTime, name)
			}
			taken[c.LocalTime+"|"+name] = true
			days[j] = name
		}
		out[i] = cutoffSlotDTO{LocalTime: c.LocalTime, MaxOrders: c.MaxOrders, Days: days}
	}
	return out, nil
}

// openDaysOf declares every weekday that has a cutoff open all day.
func openDaysOf(cutoffs []cutoffSlotDTO) map[string]dayHoursDTO {
	out := map[string]dayHoursDTO{}
	for _, c := range cutoffs {
		for _, d := range c.Days {
			out[d] = dayHoursDTO{Start: openAllDayStart, End: openAllDayEnd}
		}
	}
	return out
}
