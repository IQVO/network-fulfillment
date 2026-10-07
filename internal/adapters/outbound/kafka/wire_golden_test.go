package kafka_test

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/network-fulfillment/internal/adapters/outbound/kafka"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// updateWireGolden regenerates testdata/wire/**. It exists only so the
// goldens can be captured from a known-good build; CI never passes it.
var updateWireGolden = flag.Bool("update-wire-golden", false, "rewrite testdata/wire golden files")

// wireCase is one event occurrence whose exact CloudEvents bytes are pinned.
type wireCase struct {
	file  string
	event shared.DomainEvent
}

func wireCases() []wireCase {
	at := time.Date(2026, 9, 23, 8, 0, 0, 123456789, time.UTC)
	cases := []wireCase{
		{"NetworkOrderReceived.json", shared.NetworkOrderReceived{NetworkRef: "po-1", SiteId: "site-1",
			RequiredShipBy: at.Add(48 * time.Hour), AcknowledgeBy: at.Add(24 * time.Hour), LineCount: 2, At: at}},
		{"NetworkOrderReceived_untranslatable.json", shared.NetworkOrderReceived{NetworkRef: "po-1", SiteId: "site-1",
			RequiredShipBy: at.Add(48 * time.Hour), AcknowledgeBy: at.Add(24 * time.Hour), LineCount: 0, At: at}},
		{"NetworkOrderSubmitted.json", shared.NetworkOrderSubmitted{NetworkRef: "po-1", SiteId: "site-1",
			LocalOrderId: "ord-1", ReceivedAt: at.Add(-time.Minute), At: at}},
		// NetworkOrderAcknowledged is the v2 wire type (ADR 0016): the
		// settle. The historic v1 bytes (what was on the analytics topic
		// before) are kept as the replay fixture in
		// internal/adapters/inbound/kafka/testdata/historic-v1/.
		{"NetworkOrderAcknowledged.json", shared.NetworkOrderAcknowledged{NetworkRef: "po-1", SiteId: "site-1",
			LocalOrderId: "ord-1", ReceivedAt: at.Add(-time.Minute), At: at}},
		{"NetworkOrderShipmentConfirmed.json", shared.NetworkOrderShipmentConfirmed{NetworkRef: "po-1", SiteId: "site-1",
			LocalOrderId: "ord-1", At: at}},
		{"AcknowledgementDeadlineAtRisk.json", shared.AcknowledgementDeadlineAtRisk{NetworkRef: "po-1", SiteId: "site-1",
			AcknowledgeBy: at.Add(-time.Hour), At: at}},
		// Zero-valued optional members must keep serialising as they did
		// (empty strings, 0001-01-01T00:00:00Z) — no omitempty crept in.
		{"NetworkOrderReceived_zero.json", shared.NetworkOrderReceived{NetworkRef: "po-1"}},
		{"NetworkOrderSubmitted_zero.json", shared.NetworkOrderSubmitted{NetworkRef: "po-1"}},
		{"NetworkOrderAcknowledged_zero.json", shared.NetworkOrderAcknowledged{NetworkRef: "po-1"}},
		{"NetworkOrderRejected_zero.json", shared.NetworkOrderRejected{NetworkRef: "po-1"}},
		{"NetworkOrderShipmentConfirmed_zero.json", shared.NetworkOrderShipmentConfirmed{NetworkRef: "po-1"}},
		{"AcknowledgementDeadlineAtRisk_zero.json", shared.AcknowledgementDeadlineAtRisk{NetworkRef: "po-1"}},
	}
	for _, r := range []shared.RejectionReason{
		shared.RejectionReasonUntranslatableSKU,
		shared.RejectionReasonInfeasibleDeadline,
		shared.RejectionReasonAcknowledgementDeadlineMissed,
		shared.RejectionReasonSubmissionFailed,
	} {
		cases = append(cases, wireCase{
			"NetworkOrderRejected_" + string(r) + ".json",
			shared.NetworkOrderRejected{NetworkRef: "po-1", SiteId: "site-1", Reason: r, At: at},
		})
	}
	return cases
}

// TestWireFormat_ByteIdenticalGoldens pins the exact CloudEvents bytes
// (integration and analytics streams) of every event type. The goldens
// were captured BEFORE the JSON tags left the domain structs, so a pass
// proves the adapter-owned payload DTOs serialise byte-for-byte as the
// tagged domain events did.
func TestWireFormat_ByteIdenticalGoldens(t *testing.T) {
	encoders := map[string]outboundkafka.Encoder{
		"events":    outboundkafka.NewPublisher(nil, func() string { return fixedID }),
		"analytics": outboundkafka.NewAnalyticsPublisher(nil, func() string { return fixedID }),
	}
	for dir, enc := range encoders {
		for _, c := range wireCases() {
			t.Run(dir+"/"+c.file, func(t *testing.T) {
				out, err := enc.Encode(context.Background(), c.event)
				if err != nil {
					t.Fatalf("Encode: %v", err)
				}
				if len(out) != 1 {
					t.Fatalf("got %d messages, want 1", len(out))
				}
				assertWireGolden(t, filepath.Join("testdata", "wire", dir, c.file), out[0].Value)
			})
		}
	}
}

// assertWireGolden compares got with the golden at path, or rewrites the
// golden when -update-wire-golden is set.
func assertWireGolden(t *testing.T, path string, got []byte) {
	t.Helper()
	if *updateWireGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("wire bytes changed\n got: %s\nwant: %s", got, want)
	}
}
