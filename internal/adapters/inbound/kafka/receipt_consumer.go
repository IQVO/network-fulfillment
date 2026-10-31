package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v4"
	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/network-fulfillment/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/ports"
)

// ReceiptTopic is inbound-receiving's integration topic (its asyncapi channel
// warehouse.inbound-receiving.events). Messages are keyed by ASN number.
const ReceiptTopic = "warehouse.inbound-receiving.events"

// The full CloudEvents types of inbound-receiving this consumer projects
// (ADR 0028). Everything else on the topic is ignored (forward compatibility).
const (
	TypeReceiptCartonReceived   = "com.warehouse.wms.inbound-receiving.receipt.ReceiptCartonReceived"
	TypeCartonDiscrepancyRaised = "com.warehouse.wms.inbound-receiving.receipt.CartonDiscrepancyRaised"
	TypeReceiptClosed           = "com.warehouse.wms.inbound-receiving.receipt.ReceiptClosed"
)

// ReceiptConsumer reads inbound-receiving's receipt events under a consumer
// group of this service's own (INBOUND_RECEIPT_CONSUMER_GROUP) and applies them
// to the local ReceiptProjection, which keeps only the ASNs this service
// registered. Delivery is at-least-once; the projection claims each CloudEvents
// id with its write, so a redelivery changes nothing. Like the analytics
// consumer, a message that is not a CloudEvent, or whose handling keeps failing,
// goes to topic+".dlq" so one poison message never blocks the partition.
type ReceiptConsumer struct {
	Reader     *kafkago.Reader
	Projection ports.ReceiptProjection
	Logger     *slog.Logger
	Now        func() time.Time
	dlqWriter  *kafkago.Writer
}

// receiptReadMaxWait keeps a quiet topic from blocking Reader.Close for the
// kafka-go default of 10s (the fleet rule for every reader).
const receiptReadMaxWait = 500 * time.Millisecond

// NewReceiptConsumer builds the consumer for topic under groupID. A new group
// starts at the earliest offset: the projection ignores ASNs that are not ours,
// and a receipt closed while the service was down must not be lost.
func NewReceiptConsumer(brokers []string, topic, groupID string, projection ports.ReceiptProjection, logger *slog.Logger) *ReceiptConsumer {
	if logger == nil {
		logger = slog.Default()
	}
	return &ReceiptConsumer{
		Reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers, Topic: topic, GroupID: groupID,
			StartOffset: kafkago.FirstOffset, MaxWait: receiptReadMaxWait,
		}),
		Projection: projection,
		Logger:     logger,
		Now:        time.Now,
		dlqWriter:  newDLQWriter(brokers, topic+dlqTopicSuffix),
	}
}

// Run consumes until ctx ends (nil) or the reader fails for good (the error).
func (c *ReceiptConsumer) Run(ctx context.Context) error {
	for {
		msg, err := c.Reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if err := c.handleMessage(ctx, msg); err != nil {
			if ctx.Err() != nil && errors.Is(err, context.Canceled) {
				return nil // shutdown mid-handle: uncommitted, redelivered, deduplicated
			}
			return err
		}
	}
}

// Close releases the reader and the dead-letter writer.
func (c *ReceiptConsumer) Close() error {
	return errors.Join(c.Reader.Close(), c.dlqWriter.Close())
}

func (c *ReceiptConsumer) handleMessage(ctx context.Context, msg kafkago.Message) error {
	policy := backoff.NewExponentialBackOff(backoff.WithInitialInterval(retryInitialInterval), backoff.WithMaxInterval(retryMaxInterval))
	bounded := backoff.WithContext(backoff.WithMaxRetries(policy, maxHandlerAttempts-1), ctx)
	err := backoff.Retry(func() error {
		err := c.Handle(ctx, string(msg.Key), msg.Value)
		if errors.Is(err, cloudevents.ErrNotCloudEvent) || errors.Is(err, errBadReceiptEvent) {
			return backoff.Permanent(err)
		}
		return err
	}, bounded)
	if err == nil {
		return c.Reader.CommitMessages(ctx, msg)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	c.Logger.Warn("receipt consumer: sending the message to the dead-letter topic", "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", err)
	if dlqErr := c.dlq(ctx, msg, err); dlqErr != nil {
		return fmt.Errorf("receipt consumer: publish to dead-letter topic: %w", dlqErr)
	}
	return c.Reader.CommitMessages(ctx, msg)
}

func (c *ReceiptConsumer) dlq(ctx context.Context, msg kafkago.Message, cause error) error {
	headers := append([]kafkago.Header{}, msg.Headers...)
	headers = append(headers,
		kafkago.Header{Key: "x-dlq-source-topic", Value: []byte(c.Reader.Config().Topic)},
		kafkago.Header{Key: "x-dlq-error", Value: []byte(cause.Error())},
		kafkago.Header{Key: "x-dlq-failed-at", Value: []byte(c.Now().UTC().Format(time.RFC3339))},
	)
	return c.dlqWriter.WriteMessages(ctx, kafkago.Message{Key: msg.Key, Value: msg.Value, Headers: headers})
}

var errBadReceiptEvent = errors.New("receipt consumer: the event data is not what inbound-receiving's contract says")

type cartonReceivedData struct {
	ReceiptID string `json:"receipt_id"`
	AsnNumber string `json:"asn_number"`
	SSCC      string `json:"sscc"`
	LPN       string `json:"lpn"`
	Contents  []struct {
		GTIN      string `json:"gtin"`
		Qty       int64  `json:"qty"`
		Condition string `json:"condition"`
	} `json:"contents"`
	ReceivedAt time.Time `json:"received_at"`
}

type discrepancyData struct {
	ReceiptID string    `json:"receipt_id"`
	SSCC      string    `json:"sscc"`
	Kind      string    `json:"kind"`
	Expected  string    `json:"expected"`
	Actual    string    `json:"actual"`
	At        time.Time `json:"at"`
}

type receiptClosedData struct {
	ReceiptID string    `json:"receipt_id"`
	AsnNumber string    `json:"asn_number"`
	ClosedAt  time.Time `json:"closed_at"`
}

// Handle decodes one message value and applies it, without retry. key is the
// Kafka key: the ASN number. An unknown type is a skip; a value that is not a
// CloudEvent wraps cloudevents.ErrNotCloudEvent.
func (c *ReceiptConsumer) Handle(ctx context.Context, key string, raw []byte) error {
	env, err := cloudevents.Decode(raw)
	if err != nil {
		return fmt.Errorf("receipt consumer: decode event: %w", err)
	}
	switch env.Type() {
	case TypeReceiptCartonReceived:
		return c.carton(ctx, key, env)
	case TypeCartonDiscrepancyRaised:
		return c.finding(ctx, key, env)
	case TypeReceiptClosed:
		return c.closed(ctx, key, env)
	}
	return nil
}

func asnOf(key, fromData string) string {
	if fromData != "" {
		return fromData
	}
	return key
}

func (c *ReceiptConsumer) carton(ctx context.Context, key string, env ce.Event) error {
	var d cartonReceivedData
	if err := env.DataAs(&d); err != nil {
		return fmt.Errorf("%w: ReceiptCartonReceived: %v", errBadReceiptEvent, err)
	}
	asn := asnOf(key, d.AsnNumber)
	if asn == "" || (d.SSCC == "" && d.LPN == "") {
		return fmt.Errorf("%w: ReceiptCartonReceived names no ASN or no carton", errBadReceiptEvent)
	}
	f := contract.ReceivedCartonFact{ReceiptID: d.ReceiptID, AsnNumber: asn, SSCC: d.SSCC, LPN: d.LPN, ReceivedAt: d.ReceivedAt}
	for _, x := range d.Contents {
		f.Contents = append(f.Contents, contract.ReceivedContent{GTIN: x.GTIN, Qty: x.Qty, Condition: x.Condition})
	}
	return c.Projection.RecordCartonReceived(ctx, env.ID(), f)
}

func (c *ReceiptConsumer) finding(ctx context.Context, key string, env ce.Event) error {
	var d discrepancyData
	if err := env.DataAs(&d); err != nil {
		return fmt.Errorf("%w: CartonDiscrepancyRaised: %v", errBadReceiptEvent, err)
	}
	if key == "" || d.Kind == "" {
		return fmt.Errorf("%w: CartonDiscrepancyRaised names no ASN (the Kafka key) or no kind", errBadReceiptEvent)
	}
	return c.Projection.RecordFinding(ctx, env.ID(), contract.CartonFinding{
		ReceiptID: d.ReceiptID, AsnNumber: key, SSCC: d.SSCC, Kind: d.Kind, Expected: d.Expected, Actual: d.Actual, At: d.At,
	})
}

func (c *ReceiptConsumer) closed(ctx context.Context, key string, env ce.Event) error {
	var d receiptClosedData
	if err := env.DataAs(&d); err != nil {
		return fmt.Errorf("%w: ReceiptClosed: %v", errBadReceiptEvent, err)
	}
	asn := asnOf(key, d.AsnNumber)
	if asn == "" {
		return fmt.Errorf("%w: ReceiptClosed names no ASN", errBadReceiptEvent)
	}
	return c.Projection.RecordClosed(ctx, env.ID(), contract.ReceiptClosedFact{ReceiptID: d.ReceiptID, AsnNumber: asn, ClosedAt: d.ClosedAt}, c.Now().UTC())
}
