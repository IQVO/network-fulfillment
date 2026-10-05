// Package processpathcache is the outbound adapter that keeps a local,
// in-memory view of process-path-management's fulfillment-capability
// contract (its ADR 0010) up to date by consuming
// warehouse.process-path-management.events from its earliest offset. It
// satisfies ports.ProcessPathCapability.
//
// Mirrors order-management's internal/adapters/outbound/kafkacatalog and
// inventory-storage's internal/adapters/outbound/facilitycache
// byte-for-byte in concurrency/readiness design: a fresh,
// PROCESS-UNIQUE consumer group per start (NEVER a fixed shared name —
// see those packages' own doc comments for the two real bugs, a
// per-message synchronous commit and a shared group id resuming from a
// prior instance's offset, that this design avoids by construction),
// and a Ready()/WaitReady() gate so the composition root can block
// starting real recompute work until the initial replay completes.
//
// One difference from kafkacatalog: order-management's copy never
// needed CPTScheduleChanged (it has its own, differently-sourced
// CPTWindow model per its ADR 0014). This cache DOES decode it, because
// ADR 0001 §8's throughput formula needs the site-level cutoff schedule
// as well as each path's own cycle-time-p95 — the two event families
// share this one topic and this is their one combined read model.
package processpathcache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-fulfillment/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// Topic is process-path-management's publish topic. This service has no
// business knowing anything else about that service beyond this topic
// name and the CloudEvents type/payload shapes below.
const Topic = "warehouse.process-path-management.events"

// consumerGroupPrefix names this service's dedicated, PER-PROCESS
// consumer group on Topic. Deliberately NOT a fixed, shared name.
const consumerGroupPrefix = "network-fulfillment-process-path-capability-cache"

// replayCommitInterval: see kafkacatalog/facilitycache's identical
// constant for the measured O(history) x RTT regression a zero
// CommitInterval causes on a GroupID'd reader. Safe here for the same
// reason: the group is unique per process and its offsets are never
// resumed.
const replayCommitInterval = time.Second

// WaitReadyTimeout bounds how long the composition root waits for the
// initial replay before giving up and logging loudly, rather than
// hanging forever on a broker that will never deliver.
const WaitReadyTimeout = 60 * time.Second

// CloudEvents `type` strings this consumer dispatches on, byte-for-byte
// as process-path-management publishes them. Dispatch is on the FULL
// string, never a short name.
const (
	typeCreated            = "com.warehouse.wes.process-path-management.processpath.ProcessPathCreated"
	typeUpdated            = "com.warehouse.wes.process-path-management.processpath.ProcessPathUpdated"
	typeDeactivated        = "com.warehouse.wes.process-path-management.processpath.ProcessPathDeactivated"
	typeCPTScheduleChanged = "com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged"
)

// pathData is ProcessPathCreated/Updated's payload shape (the fields
// this cache needs). cycle_time_p95 is the wire's time.Duration.String()
// form (e.g. "45m0s"), parsed with time.ParseDuration.
type pathData struct {
	PathId       string `json:"path_id"`
	CycleTimeP95 string `json:"cycle_time_p95"`
}

// deactivatedData is ProcessPathDeactivated's payload shape.
type deactivatedData struct {
	PathId string `json:"path_id"`
}

// cutoffData is one entry of CPTScheduleChanged's cutoffs array.
type cutoffData struct {
	CptId           string   `json:"cpt_id"`
	LocalTime       string   `json:"local_time"`
	DaysOfWeek      []string `json:"days_of_week"`
	ShipMethod      string   `json:"ship_method"`
	EligiblePathIds []string `json:"eligible_path_ids"`
}

// scheduleData is CPTScheduleChanged's full payload — a snapshot, not a
// diff, so a fresh consumer replaying from FirstOffset needs no prior
// state to build this read model.
type scheduleData struct {
	SiteId   string       `json:"site_id"`
	Timezone string       `json:"timezone"`
	Cutoffs  []cutoffData `json:"cutoffs"`
}

// Reader is the subset of *kafkago.Reader this Consumer needs, so tests
// can substitute a fake without a live broker.
type Reader interface {
	ReadMessage(ctx context.Context) (kafkago.Message, error)
	Close() error
}

// pathState is this cache's per-path knowledge: whether it's currently
// active (present at all; a ProcessPathDeactivated deletes it) and its
// cycle-time-p95.
type pathState struct {
	cycleTimeP95   time.Duration
	cycleTimeKnown bool
}

// Consumer maintains the local read model by replaying Topic from its
// earliest offset. Schedules and path cycle times are kept in SEPARATE
// maps and joined at lookup time, deliberately: they are different
// aggregates published under different keys with no cross-aggregate
// ordering guarantee, so a CPTScheduleChanged naming a path whose own
// ProcessPathCreated has not yet been seen simply resolves with
// CycleTimeKnown=false until it arrives, rather than caching a
// permanently wrong join.
type Consumer struct {
	Reader Reader
	Logger *slog.Logger

	mu        sync.RWMutex
	schedules map[shared.SiteId]scheduleData
	paths     map[string]pathState

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
		return nil, fmt.Errorf("processpathcache: determine readiness target: %w", err)
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        uniqueConsumerGroup(),
		StartOffset:    kafkago.FirstOffset,
		CommitInterval: replayCommitInterval,
	})

	c := &Consumer{
		Reader:    reader,
		Logger:    logger,
		schedules: make(map[shared.SiteId]scheduleData),
		paths:     make(map[string]pathState),
		readyCh:   make(chan struct{}),
		target:    target,
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
		return nil, fmt.Errorf("processpathcache: no brokers configured")
	}
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return nil, fmt.Errorf("processpathcache: dial %s: %w", brokers[0], err)
	}
	defer func() { _ = conn.Close() }()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		if isUnknownTopic(err) {
			// The topic does not exist yet -- a legitimate startup
			// state (this consumer starting before process-path-
			// management has ever published, or a fresh cluster).
			return targetOffsets{}, nil
		}
		return nil, fmt.Errorf("processpathcache: read partitions for %s: %w", topic, err)
	}

	out := make(targetOffsets, len(partitions))
	for _, p := range partitions {
		pconn, err := kafkago.DialLeader(ctx, "tcp", brokers[0], topic, p.ID)
		if err != nil {
			return nil, fmt.Errorf("processpathcache: dial leader for partition %d: %w", p.ID, err)
		}
		first, last, err := pconn.ReadOffsets()
		closeErr := pconn.Close()
		if err != nil {
			return nil, fmt.Errorf("processpathcache: read offsets for partition %d: %w", p.ID, err)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("processpathcache: close leader conn for partition %d: %w", p.ID, closeErr)
		}
		if last > first {
			out[p.ID] = last
		}
	}
	return out, nil
}

func isUnknownTopic(err error) bool {
	return errors.Is(err, kafkago.UnknownTopicOrPartition)
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

// NextCutoff satisfies ports.ProcessPathCapability.
func (c *Consumer) NextCutoff(siteId shared.SiteId, after time.Time) (contract.NextCutoff, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	sched, ok := c.schedules[siteId]
	if !ok || len(sched.Cutoffs) == 0 {
		return contract.NextCutoff{}, false
	}

	best, bestPaths, found := earliestCutoff(sched.Cutoffs, after)
	if !found {
		return contract.NextCutoff{}, false
	}
	return contract.NextCutoff{CutoffAt: best, Paths: c.eligiblePaths(bestPaths)}, true
}

// earliestCutoff resolves each cutoff's NEXT occurrence on or after
// `after` within the next 7 days (every schedule repeats weekly at
// most), then returns the earliest one(s) -- more than one cutoff can
// tie on the same instant. The cutoff's own timezone is deliberately not
// interpreted beyond a direct string compare against `after` treated as
// UTC-equivalent local time for THIS v1 -- a real timezone-aware
// resolution is tracked as a known simplification, not silently assumed
// correct; see the package's own tests for the exact boundary this
// produces.
func earliestCutoff(cutoffs []cutoffData, after time.Time) (time.Time, []cutoffData, bool) {
	var best time.Time
	var bestPaths []cutoffData
	found := false
	for day := 0; day < 8; day++ {
		candidateDate := after.AddDate(0, 0, day)
		for _, cutoff := range cutoffs {
			at, ok := cutoffOnDate(cutoff, candidateDate, after)
			if !ok {
				continue
			}
			if !found || at.Before(best) {
				best = at
				found = true
				bestPaths = []cutoffData{cutoff}
			} else if at.Equal(best) {
				bestPaths = append(bestPaths, cutoff)
			}
		}
		if found {
			break
		}
	}
	return best, bestPaths, found
}

// cutoffOnDate resolves one cutoff's instant on candidateDate, reporting
// ok=false when the cutoff does not apply that day, is malformed, or
// falls before `after`.
func cutoffOnDate(cutoff cutoffData, candidateDate, after time.Time) (time.Time, bool) {
	if !dayMatches(cutoff.DaysOfWeek, candidateDate) {
		return time.Time{}, false
	}
	at, err := cutoffInstant(candidateDate, cutoff.LocalTime)
	if err != nil || at.Before(after) {
		return time.Time{}, false
	}
	return at, true
}

// eligiblePaths resolves the deduplicated, joined path list for a set of
// tied-earliest cutoffs. Caller holds c.mu (read or write).
func (c *Consumer) eligiblePaths(cutoffs []cutoffData) []contract.EligiblePath {
	seen := make(map[string]bool)
	var paths []contract.EligiblePath
	for _, cutoff := range cutoffs {
		for _, pathID := range cutoff.EligiblePathIds {
			if seen[pathID] {
				continue
			}
			seen[pathID] = true
			state := c.paths[pathID]
			paths = append(paths, contract.EligiblePath{
				PathId:         pathID,
				CycleTimeP95:   state.cycleTimeP95,
				CycleTimeKnown: state.cycleTimeKnown,
			})
		}
	}
	return paths
}

func dayMatches(days []string, at time.Time) bool {
	want := at.Weekday().String()[:3]
	for _, d := range days {
		if strings.EqualFold(d, want) {
			return true
		}
	}
	return false
}

func cutoffInstant(date time.Time, localTime string) (time.Time, error) {
	parts := strings.Split(localTime, ":")
	if len(parts) != 2 {
		return time.Time{}, fmt.Errorf("processpathcache: malformed local_time %q", localTime)
	}
	var h, m int
	if _, err := fmt.Sscanf(parts[0], "%d", &h); err != nil {
		return time.Time{}, err
	}
	if _, err := fmt.Sscanf(parts[1], "%d", &m); err != nil {
		return time.Time{}, err
	}
	return time.Date(date.Year(), date.Month(), date.Day(), h, m, 0, 0, date.Location()), nil
}

// Run consumes Topic until ctx is cancelled or the reader returns a
// fatal error. A handling error is logged and the loop continues, so
// one malformed message cannot wedge this consumer.
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
				c.Logger.ErrorContext(ctx, "process-path capability cache message handling failed",
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
	switch e.Type() {
	case typeCreated, typeUpdated:
		var d pathData
		if err := e.DataAs(&d); err != nil {
			return fmt.Errorf("processpathcache: decode %s: %w", e.Type(), err)
		}
		c.applyPath(d)
	case typeDeactivated:
		var d deactivatedData
		if err := e.DataAs(&d); err != nil {
			return fmt.Errorf("processpathcache: decode %s: %w", e.Type(), err)
		}
		c.applyDeactivated(d)
	case typeCPTScheduleChanged:
		var d scheduleData
		if err := e.DataAs(&d); err != nil {
			return fmt.Errorf("processpathcache: decode %s: %w", e.Type(), err)
		}
		c.applySchedule(d)
	default:
		// An event type this cache has no use for yet (ProcessPath
		// carries more fields; other consumers of this topic read
		// them). Ignoring an unknown/unneeded type is correct, not
		// an error.
	}
	return nil
}

func (c *Consumer) applyPath(d pathData) {
	state := pathState{}
	if dur, err := time.ParseDuration(d.CycleTimeP95); err == nil {
		state.cycleTimeP95 = dur
		state.cycleTimeKnown = true
	}
	c.mu.Lock()
	c.paths[d.PathId] = state
	c.mu.Unlock()
}

func (c *Consumer) applyDeactivated(d deactivatedData) {
	c.mu.Lock()
	delete(c.paths, d.PathId)
	c.mu.Unlock()
}

func (c *Consumer) applySchedule(d scheduleData) {
	c.mu.Lock()
	c.schedules[shared.SiteId(d.SiteId)] = d
	c.mu.Unlock()
}
