package retailnetwork

import "time"

// The wire shapes of the Vendor API (apis/openapi.yaml of vendor-connectivity,
// copied under testdata/ and checked by contract_test.go). They are the network's
// Published Language and stay in this file, in this package: nothing here is
// handed to a use case.

// ---- responses ----

type poLineDTO struct {
	LineNo    int    `json:"lineNo"`
	ListingID string `json:"listingId"`
	Quantity  int    `json:"quantity"`
}

type acknowledgementDTO struct {
	Decision      string `json:"decision"`
	ReasonCode    string `json:"reasonCode,omitempty"`
	TransactionID string `json:"transactionId"`
}

type purchaseOrderDTO struct {
	PoNumber        string              `json:"poNumber"`
	VendorID        string              `json:"vendorId"`
	NodeID          string              `json:"nodeId"`
	State           string              `json:"state"`
	IssuedAt        time.Time           `json:"issuedAt"`
	RequiredShipBy  time.Time           `json:"requiredShipBy"`
	AckBy           time.Time           `json:"ackBy"`
	Lines           []poLineDTO         `json:"lines"`
	Acknowledgement *acknowledgementDTO `json:"acknowledgement,omitempty"`
}

type purchaseOrderPageDTO struct {
	PurchaseOrders []purchaseOrderDTO `json:"purchaseOrders"`
	NextToken      string             `json:"nextToken,omitempty"`
}

type submissionErrorDTO struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Path    string `json:"path,omitempty"`
}

type transactionDTO struct {
	TransactionID string               `json:"transactionId"`
	VendorID      string               `json:"vendorId"`
	Kind          string               `json:"kind"`
	Status        string               `json:"status"`
	ReceivedAt    time.Time            `json:"receivedAt"`
	ProcessedAt   *time.Time           `json:"processedAt,omitempty"`
	Errors        []submissionErrorDTO `json:"errors,omitempty"`
	NodeID        string               `json:"nodeId,omitempty"`
	PoNumber      string               `json:"poNumber,omitempty"`
}

type transactionAcceptedDTO struct {
	TransactionID string `json:"transactionId"`
}

type vendorNodeDTO struct {
	NodeID         string `json:"nodeId"`
	VendorID       string `json:"vendorId"`
	VendorNodeCode string `json:"vendorNodeCode"`
	NodeType       string `json:"nodeType"`
	Status         string `json:"status"`
	// ProfileRevision is the revision of the latest profile the network knows;
	// absent (0) before the first one (ADR 0027 §4, stale recovery).
	ProfileRevision int `json:"profileRevision,omitempty"`
}

type vendorNodeListDTO struct {
	Nodes []vendorNodeDTO `json:"nodes"`
}

type labelDTO struct {
	PackageNo      int    `json:"packageNo,omitempty"`
	LabelRef       string `json:"labelRef"`
	TrackingNumber string `json:"trackingNumber"`
	Carrier        string `json:"carrier"`
}

type labelResponseDTO struct {
	PoNumber string     `json:"poNumber"`
	Labels   []labelDTO `json:"labels"`
}

// ---- requests: exactly the schema's properties, the server refuses unknown ones ----

type ackLineDTO struct {
	LineNo   int `json:"lineNo"`
	Quantity int `json:"quantity"`
}

type acknowledgementRequestDTO struct {
	VendorID   string       `json:"vendorId"`
	PoNumber   string       `json:"poNumber"`
	Decision   string       `json:"decision"`
	ReasonCode string       `json:"reasonCode,omitempty"`
	Lines      []ackLineDTO `json:"lines"`
}

type labelPackageDTO struct {
	PackageNo int `json:"packageNo"`
}

type labelRequestDTO struct {
	VendorID string            `json:"vendorId"`
	PoNumber string            `json:"poNumber"`
	Packages []labelPackageDTO `json:"packages"`
}

type shipmentConfirmationRequestDTO struct {
	VendorID        string    `json:"vendorId"`
	PoNumber        string    `json:"poNumber"`
	ShippedAt       time.Time `json:"shippedAt"`
	TrackingNumbers []string  `json:"trackingNumbers"`
}

type nodeRegistrationRequestDTO struct {
	VendorID       string   `json:"vendorId"`
	VendorNodeCode string   `json:"vendorNodeCode"`
	NodeType       string   `json:"nodeType"`
	Subtype        string   `json:"subtype,omitempty"`
	RegionID       string   `json:"regionId"`
	Timezone       string   `json:"timezone"`
	PostalCode     string   `json:"postalCode"`
	CountryCode    string   `json:"countryCode"`
	Lat            *float64 `json:"lat,omitempty"`
	Lon            *float64 `json:"lon,omitempty"`
}

// ---- the vocabulary the adapter sends and understands ----

// dayHoursDTO is the local opening interval of one weekday.
type dayHoursDTO struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

type cutoffSlotDTO struct {
	LocalTime string   `json:"localTime"`
	MaxOrders int      `json:"maxOrders,omitempty"`
	Days      []string `json:"days,omitempty"`
}

type throughputCapDTO struct {
	Value    int    `json:"value"`
	TimeUnit string `json:"timeUnit"`
	Unit     string `json:"unit"`
}

// nodeProfileRequestDTO is the profile body (ADR 0027 §3): holidays and
// sortDwellMinutes are optional and left out, because the fleet holds neither.
// OperatingHoursByDay is a map so that its encoding is sorted by key and a body
// is byte-for-byte deterministic.
type nodeProfileRequestDTO struct {
	VendorID            string                 `json:"vendorId"`
	ProfileRevision     int                    `json:"profileRevision"`
	Timezone            string                 `json:"timezone"`
	OperatingHoursByDay map[string]dayHoursDTO `json:"operatingHoursByDay"`
	HandlingTimeMinutes int                    `json:"handlingTimeMinutes"`
	Cutoffs             []cutoffSlotDTO        `json:"cutoffs"`
	ThroughputCap       throughputCapDTO       `json:"throughputCap"`
}

const (
	stateIssued = "ISSUED"

	timeUnitDay = "DAY"
	unitOrder   = "ORDER"

	// The adapter declares a day it hands over on as open all day: the fleet
	// holds no hours of its own, so the cutoffs and the handling time carry the
	// constraint (ADR 0027 §3).
	openAllDayStart = "00:00"
	openAllDayEnd   = "23:59"

	// errStaleProfileRevision is the code of a profile transaction that failed
	// because the network already holds a revision at or above ours.
	errStaleProfileRevision = "ErrStaleProfileRevision"
	// The codes after which the same content can never succeed.
	errIncompleteOperationalProfile = "ErrIncompleteOperationalProfile"
	errCutoffOutsideOperatingHours  = "ErrCutoffOutsideOperatingHours"

	decisionAccept = "ACCEPT"
	decisionReject = "REJECT"

	reasonCannotMeetShipBy = "CANNOT_MEET_SHIP_BY"
	reasonUnknownListing   = "UNKNOWN_LISTING"

	txProcessing = "PROCESSING"
	txSuccess    = "SUCCESS"
	txFailure    = "FAILURE"
)
