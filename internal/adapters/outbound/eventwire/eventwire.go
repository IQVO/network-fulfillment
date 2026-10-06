// Package eventwire owns the JSON wire shape of network-fulfillment's
// domain events. Serialisation is an adapter concern: the domain structs in
// internal/domain/shared carry no struct tags, and every adapter that puts
// an event on a wire (the CloudEvents `data` member on both Kafka streams,
// the log publisher) maps it through Payload first.
//
// The field names, order and encodings here are the Published Language's
// `data` schema (dataschema ...:v1) and MUST NOT change without a new
// dataschema version; byte-identical goldens in the outbound kafka and
// events packages pin them.
package eventwire

import (
	"fmt"
	"time"

	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

type networkOrderReceived struct {
	NetworkRef     shared.NetworkRef `json:"networkRef"`
	SiteId         shared.SiteId     `json:"siteId"`
	RequiredShipBy time.Time         `json:"requiredShipBy"`
	AcknowledgeBy  time.Time         `json:"acknowledgeBy"`
	LineCount      int               `json:"lineCount"`
	At             time.Time         `json:"at"`
}

type networkOrderAcknowledged struct {
	NetworkRef   shared.NetworkRef   `json:"networkRef"`
	SiteId       shared.SiteId       `json:"siteId"`
	LocalOrderId shared.LocalOrderId `json:"localOrderId"`
	ReceivedAt   time.Time           `json:"receivedAt"`
	At           time.Time           `json:"at"`
}

type networkOrderRejected struct {
	NetworkRef shared.NetworkRef      `json:"networkRef"`
	SiteId     shared.SiteId          `json:"siteId"`
	Reason     shared.RejectionReason `json:"reason"`
	At         time.Time              `json:"at"`
}

type networkOrderShipmentConfirmed struct {
	NetworkRef   shared.NetworkRef   `json:"networkRef"`
	SiteId       shared.SiteId       `json:"siteId"`
	LocalOrderId shared.LocalOrderId `json:"localOrderId"`
	At           time.Time           `json:"at"`
}

type acknowledgementDeadlineAtRisk struct {
	NetworkRef    shared.NetworkRef `json:"networkRef"`
	SiteId        shared.SiteId     `json:"siteId"`
	AcknowledgeBy time.Time         `json:"acknowledgeBy"`
	At            time.Time         `json:"at"`
}

// Payload maps a domain event to its wire DTO. Every event type the domain
// raises must be listed here; an unmapped type is an error rather than a
// silent fallback to reflection over the (untagged) domain struct, which
// would emit Go field names and break consumers.
func Payload(event shared.DomainEvent) (any, error) {
	switch e := event.(type) {
	case shared.NetworkOrderReceived:
		return networkOrderReceived{
			NetworkRef: e.NetworkRef, SiteId: e.SiteId,
			RequiredShipBy: e.RequiredShipBy, AcknowledgeBy: e.AcknowledgeBy,
			LineCount: e.LineCount, At: e.At,
		}, nil
	case shared.NetworkOrderAcknowledged:
		return networkOrderAcknowledged{
			NetworkRef: e.NetworkRef, SiteId: e.SiteId, LocalOrderId: e.LocalOrderId,
			ReceivedAt: e.ReceivedAt, At: e.At,
		}, nil
	case shared.NetworkOrderRejected:
		return networkOrderRejected{
			NetworkRef: e.NetworkRef, SiteId: e.SiteId, Reason: e.Reason, At: e.At,
		}, nil
	case shared.NetworkOrderShipmentConfirmed:
		return networkOrderShipmentConfirmed{
			NetworkRef: e.NetworkRef, SiteId: e.SiteId, LocalOrderId: e.LocalOrderId, At: e.At,
		}, nil
	case shared.AcknowledgementDeadlineAtRisk:
		return acknowledgementDeadlineAtRisk{
			NetworkRef: e.NetworkRef, SiteId: e.SiteId, AcknowledgeBy: e.AcknowledgeBy, At: e.At,
		}, nil
	default:
		return nil, fmt.Errorf("eventwire: no wire payload for event %T (%s)", event, event.EventName())
	}
}
