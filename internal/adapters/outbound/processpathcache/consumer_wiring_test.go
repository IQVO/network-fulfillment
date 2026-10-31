package processpathcache

import (
	"context"
	"strings"
	"testing"
	"time"
)

// Covers the constructor/wiring surface the message-handling tests do not
// reach: NewConsumer/NewConsumerForTopic's broker validation, the
// process-unique consumer group, and Close. The success path of
// NewConsumerForTopic (a reachable broker reporting partition offsets)
// needs Kafka and lives in the integration suite.

func TestNewConsumer_RequiresBrokers(t *testing.T) {
	_, err := NewConsumer(context.Background(), nil, nil)
	if err == nil || !strings.Contains(err.Error(), "no brokers configured") {
		t.Fatalf("err = %v, want 'no brokers configured'", err)
	}
}

func TestNewConsumerForTopic_UnreachableBrokerFailsFast(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := NewConsumerForTopic(ctx, []string{"127.0.0.1:1"}, "t", nil)
	if err == nil || !strings.Contains(err.Error(), "determine readiness target") {
		t.Fatalf("err = %v, want a wrapped readiness-target error", err)
	}
}

func TestNewTargetOffsets_NoBrokers(t *testing.T) {
	if _, err := newTargetOffsets(context.Background(), nil, "t"); err == nil || !strings.Contains(err.Error(), "no brokers configured") {
		t.Fatalf("err = %v, want 'no brokers configured'", err)
	}
}

func TestUniqueConsumerGroup_ProcessUniqueAndPrefixed(t *testing.T) {
	a, b := uniqueConsumerGroup(), uniqueConsumerGroup()
	if a == b {
		t.Fatalf("two calls returned the same group %q (a shared group id is the two-real-bugs design this exists to avoid)", a)
	}
	for _, g := range []string{a, b} {
		if !strings.HasPrefix(g, consumerGroupPrefix) {
			t.Errorf("group %q lacks the %q prefix", g, consumerGroupPrefix)
		}
	}
}

func TestClose_ReleasesTheReader(t *testing.T) {
	r := &fakeReader{}
	c := &Consumer{Reader: r}
	if err := c.Close(); err != nil {
		t.Fatalf("Close = %v, want nil", err)
	}
	if !r.closed {
		t.Error("Close did not close the underlying reader")
	}
}
