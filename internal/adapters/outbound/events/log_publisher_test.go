package events_test

import (
	"bytes"
	"context"
	"flag"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/adapters/outbound/events"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// updateGolden rewrites testdata/log_publisher.golden. It exists only so
// the golden can be captured from a known-good build; CI never passes it.
var updateGolden = flag.Bool("update-golden", false, "rewrite testdata/log_publisher.golden")

// TestLogPublisher_GoldenOutput pins the exact JSON LogPublisher logs for
// every domain event type: the log line is a (diagnostic) wire form too,
// and must not change when the domain structs lose their json tags.
func TestLogPublisher_GoldenOutput(t *testing.T) {
	at := time.Date(2026, 9, 23, 8, 0, 0, 0, time.UTC)
	evs := []shared.DomainEvent{
		shared.NetworkOrderReceived{NetworkRef: "po-1", SiteId: "site-1", RequiredShipBy: at.Add(48 * time.Hour),
			AcknowledgeBy: at.Add(24 * time.Hour), LineCount: 2, At: at},
		shared.NetworkOrderAcknowledged{NetworkRef: "po-1", SiteId: "site-1", LocalOrderId: "ord-1",
			ReceivedAt: at.Add(-time.Minute), At: at},
		shared.NetworkOrderRejected{NetworkRef: "po-1", SiteId: "site-1", Reason: shared.RejectionReasonSubmissionFailed, At: at},
		shared.NetworkOrderShipmentConfirmed{NetworkRef: "po-1", SiteId: "site-1", LocalOrderId: "ord-1", At: at},
		shared.AcknowledgementDeadlineAtRisk{NetworkRef: "po-1", SiteId: "site-1", AcknowledgeBy: at.Add(-time.Hour), At: at},
	}

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) == 0 && a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	}))
	p := events.NewLogPublisher(logger)
	for _, e := range evs {
		if err := p.Publish(context.Background(), e); err != nil {
			t.Fatalf("Publish(%s): %v", e.EventName(), err)
		}
	}

	const path = "testdata/log_publisher.golden"
	if *updateGolden {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	if buf.String() != string(want) {
		t.Errorf("log output changed\n got: %s\nwant: %s", buf.String(), want)
	}
}

func TestLogPublisher_NonDomainEventStillLogged(t *testing.T) {
	var buf bytes.Buffer
	p := events.NewLogPublisher(slog.New(slog.NewJSONHandler(&buf, nil)))
	if err := p.Publish(context.Background(), map[string]string{"a": "b"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if !bytes.Contains(buf.Bytes(), []byte(`"event":{"a":"b"}`)) {
		t.Errorf("unexpected log line: %s", buf.String())
	}
}
