// Package pathcapacitycache is the outbound adapter that keeps a local,
// in-memory view of wes-work-planning's remaining per-path admission
// capacity up to date by consuming warehouse.work-planning.events from
// its earliest offset. It satisfies ports.PathCapacity.
//
// Mirrors processpathcache's (and the fleet's shared
// facilitycache/kafkacatalog) concurrency/readiness design: a fresh,
// PROCESS-UNIQUE consumer group per start, never a fixed shared name,
// and a Ready()/WaitReady() gate. See processpathcache's package doc
// comment for the two real historical bugs this design avoids by
// construction.
//
// This cache decodes ONLY PathCapacityChanged off a topic that also
// carries this service's other, unrelated event families (ChargeForecast,
// ShiftPlan, WorkUnit*, etc.) -- every other type is ignored, which is
// correct here, not a gap: nothing else on this topic is this cache's
// concern.
package pathcapacitycache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-fulfillment/internal/adapters/kafka/cloudevents"
)

// Topic is wes-work-planning's publish topic.
const Topic = "warehouse.work-planning.events"

const consumerGroupPrefix = "network-fulfillment-path-capacity-cache"

const replayCommitInterval = time.Second

// WaitReadyTimeout bounds how long the composition root waits for the
// initial replay before giving up and logging loudly.
const WaitReadyTimeout = 60 * time.Second

const typePathCapacityChanged = "com.warehouse.wes.work-planning.workpool.PathCapacityChanged"

// capacityData is PathCapacityChanged's payload shape.
type capacityData struct {
	PathId         string `json:"path_id"`
	CutoffAt       string `json:"cutoff_at"`
	RemainingUnits int    `json:"remaining_units"`
	Known          bool   `json:"known"`
}

// Reader is the subset of *kafkago.Reader this Consumer needs, so tests
// can substitute a fake without a live broker.
type Reader interface {
	ReadMessage(ctx context.Context) (kafkago.Message, error)
	Close() error
}

type capacityKey struct {
	pathID   string
	cutoffAt time.Time
}

type capacityValue struct {
	units int
	known bool
}

// Consumer maintains the local (pathId, cutoffAt) -> remaining-capacity
// read model by replaying Topic from its earliest offset.
type Consumer struct {
	Reader Reader
	Logger *slog.Logger

	mu    sync.RWMutex
	cache map[capacityKey]capacityValue

	ready   bool
	readyCh chan struct{}
	target  targetOffsets
}

type targetOffsets map[int]int64

// NewConsumer constructs a Consumer reading Topic from brokers under a
// fresh, process-unique consumer group, starting at the earliest offset.
func NewConsumer(ctx context.Context, brokers []string, logger *slog.Logger) (*Consumer, error) {
	return NewConsumerForTopic(ctx, brokers, Topic, logger)
}

// NewConsumerForTopic is NewConsumer with an explicit topic, so
// integration tests can drive the identical replay/readiness logic
// against a throwaway topic instead of the real one.
func NewConsumerForTopic(ctx context.Context, brokers []string, topic string, logger *slog.Logger) (*Consumer, error) {
	if logger == nil {
		logger = slog.Default()
	}
	target, err := newTargetOffsets(ctx, brokers, topic)
	if err != nil {
		return nil, fmt.Errorf("pathcapacitycache: determine readiness target: %w", err)
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        uniqueConsumerGroup(),
		StartOffset:    kafkago.FirstOffset,
		CommitInterval: replayCommitInterval,
	})

	c := &Consumer{
		Reader:  reader,
		Logger:  logger,
		cache:   make(map[capacityKey]capacityValue),
		readyCh: make(chan struct{}),
		target:  target,
	}
	if len(target) == 0 {
		c.markReady()
	}
	return c, nil
}

func uniqueConsumerGroup() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-host"
	}
	return fmt.Sprintf("%s-%s-%d-%d", consumerGroupPrefix, host, os.Getpid(), time.Now().UnixNano())
}

func newTargetOffsets(ctx context.Context, brokers []string, topic string) (targetOffsets, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("pathcapacitycache: no brokers configured")
	}
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return nil, fmt.Errorf("pathcapacitycache: dial %s: %w", brokers[0], err)
	}
	defer func() { _ = conn.Close() }()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		if errors.Is(err, kafkago.UnknownTopicOrPartition) {
			return targetOffsets{}, nil
		}
		return nil, fmt.Errorf("pathcapacitycache: read partitions for %s: %w", topic, err)
	}

	out := make(targetOffsets, len(partitions))
	for _, p := range partitions {
		pconn, err := kafkago.DialLeader(ctx, "tcp", brokers[0], topic, p.ID)
		if err != nil {
			return nil, fmt.Errorf("pathcapacitycache: dial leader for partition %d: %w", p.ID, err)
		}
		first, last, err := pconn.ReadOffsets()
		closeErr := pconn.Close()
		if err != nil {
			return nil, fmt.Errorf("pathcapacitycache: read offsets for partition %d: %w", p.ID, err)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("pathcapacitycache: close leader conn for partition %d: %w", p.ID, closeErr)
		}
		if last > first {
			out[p.ID] = last
		}
	}
	return out, nil
}

// Close releases the underlying Kafka reader.
func (c *Consumer) Close() error {
	return c.Reader.Close()
}

// Ready reports whether this consumer has processed every message that
// existed in Topic at the moment it started.
func (c *Consumer) Ready() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ready
}

// WaitReady blocks until Ready() would return true or ctx is done.
func (c *Consumer) WaitReady(ctx context.Context) error {
	if c.Ready() {
		return nil
	}
	select {
	case <-c.readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Consumer) markReady() {
	c.mu.Lock()
	alreadyReady := c.ready
	c.ready = true
	c.mu.Unlock()
	if !alreadyReady {
		close(c.readyCh)
	}
}

// RemainingCapacity satisfies ports.PathCapacity. An exact (pathId,
// cutoffAt) match only -- PathCapacityChanged's own doc comment names
// nearest-window correlation as a FUTURE caller's responsibility, not
// something this cache invents a tolerance for.
func (c *Consumer) RemainingCapacity(pathId string, cutoffAt time.Time) (int, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.cache[capacityKey{pathID: pathId, cutoffAt: cutoffAt.UTC()}]
	if !ok || !v.known {
		return 0, false
	}
	return v.units, true
}

// Run consumes Topic until ctx is cancelled or the reader returns a
// fatal error.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.Reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if err := c.handle(msg); err != nil {
			if errors.Is(err, cloudevents.ErrNotCloudEvent) {
				c.Logger.WarnContext(ctx, "skipping non-CloudEvents message",
					"topic", msg.Topic, "offset", msg.Offset, "error", err)
			} else {
				c.Logger.ErrorContext(ctx, "path capacity cache message handling failed",
					"topic", msg.Topic, "offset", msg.Offset, "error", err)
			}
		}
		c.checkReady(msg)
	}
}

func (c *Consumer) checkReady(msg kafkago.Message) {
	c.mu.RLock()
	target, tracked := c.target[msg.Partition]
	alreadyReady := c.ready
	c.mu.RUnlock()
	if alreadyReady || !tracked {
		return
	}
	if msg.Offset >= target-1 {
		c.markReady()
	}
}

func (c *Consumer) handle(msg kafkago.Message) error {
	e, err := cloudevents.Decode(msg.Value)
	if err != nil {
		return err
	}
	if e.Type() != typePathCapacityChanged {
		// Every other event family on this shared topic: not this
		// cache's concern.
		return nil
	}
	var d capacityData
	if err := e.DataAs(&d); err != nil {
		return fmt.Errorf("pathcapacitycache: decode %s: %w", e.Type(), err)
	}
	cutoffAt, err := time.Parse(time.RFC3339, d.CutoffAt)
	if err != nil {
		return fmt.Errorf("pathcapacitycache: malformed cutoff_at %q: %w", d.CutoffAt, err)
	}
	c.mu.Lock()
	c.cache[capacityKey{pathID: d.PathId, cutoffAt: cutoffAt.UTC()}] = capacityValue{units: d.RemainingUnits, known: d.Known}
	c.mu.Unlock()
	return nil
}
