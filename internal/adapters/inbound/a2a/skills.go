package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/claudioed/network-fulfillment/internal/application/usecases"
	"github.com/claudioed/network-fulfillment/internal/domain/networkorder"
	"github.com/claudioed/network-fulfillment/internal/domain/shared"
)

// tracerName is the OTel instrumentation scope for A2A skill spans.
const tracerName = "github.com/claudioed/network-fulfillment/internal/adapters/inbound/a2a"

// Skill ids advertised on the Agent Card (see Card).
const (
	skillNetworkDemandStatus    = "network_demand_status"
	skillAdvertisedAvailability = "advertised_availability"
)

// skillRequest is the wire contract of every skill call: the first meaningful
// message part carries (as a Data part, or a Text part holding the same JSON)
// the skill id and its arguments. Strict on purpose - an agent client is
// machine-to-machine, so unknown fields are rejected, never ignored: a
// misspelled name like "stat" must not silently drop the filter and answer
// every order. (Go's encoding/json matches field names case-insensitively, so
// "State" still reaches state; only genuinely unknown names are refused.)
type skillRequest struct {
	Skill     string          `json:"skill"`
	Arguments json.RawMessage `json:"arguments"`
}

// networkDemandArgs and availabilityArgs are the per-skill argument shapes.
// Pointers tell "absent" from "present but empty".
type networkDemandArgs struct {
	NetworkRef *string `json:"networkRef"`
	State      *string `json:"state"`
}

type availabilityArgs struct {
	SKU    *string `json:"sku"`
	SiteId *string `json:"siteId"`
}

// answer maps one incoming message to a skill and returns the agent's reply
// Message. It is the whole executor body: this is a message-only agent, so a
// skill run is exactly one request mapped to one answer (ADR 0030).
func answer(ctx context.Context, deps Deps, ec *a2asrv.ExecutorContext) a2a.Event {
	req, problem := parseSkillRequest(ec)
	if problem != nil {
		return reply(ec, envelope{Skill: "", Problem: problem}, problem.summary())
	}

	ctx, span := otel.Tracer(tracerName).Start(ctx, "a2a.skill "+req.Skill)
	defer span.End()
	span.SetAttributes(attribute.String("a2a.skill", req.Skill))

	dto, problem := run(ctx, deps, req)
	if problem != nil {
		span.SetStatus(codes.Error, problem.Detail)
		return reply(ec, envelope{Skill: req.Skill, Problem: problem}, problem.summary())
	}
	return reply(ec, envelope{Skill: req.Skill, Data: dto}, dto.Summary())
}

// run dispatches one validated request to its skill.
func run(ctx context.Context, deps Deps, req skillRequest) (skillDTO, *problem) {
	switch req.Skill {
	case skillNetworkDemandStatus:
		data, summary, problem := deps.networkDemandStatus(ctx, req.Arguments)
		if problem != nil {
			return skillDTO{}, problem
		}
		return skillDTO{NetworkDemand: &data, summary: summary}, nil
	case skillAdvertisedAvailability:
		data, summary, problem := deps.advertisedAvailability(ctx, req.Arguments)
		if problem != nil {
			return skillDTO{}, problem
		}
		return skillDTO{AdvertisedAvailability: &data, summary: summary}, nil
	default:
		return skillDTO{}, problemUnknownSkill(req.Skill)
	}
}

// reply builds the agent Message for one skill run: a Data part carrying the
// structured envelope and a Text part carrying the one-line summary, bound to
// the caller's context (and task, when the client referenced one).
func reply(ec *a2asrv.ExecutorContext, env envelope, summary string) a2a.Event {
	return a2a.NewMessageForTask(a2a.MessageRoleAgent, ec, a2a.NewDataPart(env), a2a.NewTextPart(summary))
}

// parseSkillRequest extracts the skillRequest from the message's first
// meaningful part. A Data part (or a Text part holding the same JSON object) is
// accepted; anything else is a request-shape problem, answered - not raised -
// because a malformed delegation is a business answer for the calling agent,
// not a transport failure.
func parseSkillRequest(ec *a2asrv.ExecutorContext) (skillRequest, *problem) {
	msg := ec.Message
	if msg == nil || len(msg.Parts) == 0 {
		return skillRequest{}, problemMalformedRequest("the message carries no parts")
	}
	raw, err := partJSON(msg.Parts[0])
	if err != nil {
		return skillRequest{}, problemMalformedRequest(fmt.Sprintf("the first message part is not a skill request: %v", err))
	}
	var req skillRequest
	if err := decodeStrict(raw, &req); err != nil {
		return skillRequest{}, problemMalformedRequest(fmt.Sprintf("the skill request is not valid: %v", err))
	}
	if req.Skill == "" {
		return skillRequest{}, problemMalformedRequest(`the skill request carries no "skill" field`)
	}
	return req, nil
}

// partJSON renders one part's content as the JSON bytes of a skill request.
func partJSON(part *a2a.Part) ([]byte, error) {
	switch content := part.Content.(type) {
	case a2a.Data:
		return json.Marshal(content.Value)
	case a2a.Text:
		return []byte(string(content)), nil
	default:
		return nil, errors.New("expected a data part (or a text part holding the skill JSON)")
	}
}

// decodeStrict decodes one JSON document into dst, rejecting unknown fields and
// trailing data. Absent arguments decode as the empty object so each skill's
// own checks produce the specific problem.
func decodeStrict(raw []byte, dst any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = []byte("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("unexpected data after the JSON document")
	}
	return nil
}

// networkDemandStatus is the orders read skill. networkRef selects one order,
// state filters the list, neither lists every order: the same three questions
// the MCP get_network_order and list_network_orders tools answer, as one goal
// ("where does the network's demand stand").
func (d Deps) networkDemandStatus(ctx context.Context, rawArgs json.RawMessage) (networkDemand, string, *problem) {
	var args networkDemandArgs
	if err := decodeStrict(rawArgs, &args); err != nil {
		return networkDemand{}, "", problemInvalidArgument(fmt.Sprintf("arguments: %v", err))
	}
	if d.Orders == nil || d.Clock == nil {
		return networkDemand{}, "", problemDependencyUnavailable("the network order store is not wired into this agent")
	}
	if args.NetworkRef != nil && args.State != nil {
		return networkDemand{}, "", problemInvalidArgument("networkRef and state are mutually exclusive: give networkRef for one order or state to filter the list")
	}
	if args.NetworkRef != nil {
		return d.oneOrder(ctx, *args.NetworkRef)
	}
	state, p := parseState(args.State)
	if p != nil {
		return networkDemand{}, "", p
	}
	return d.listOrders(ctx, state)
}

func (d Deps) oneOrder(ctx context.Context, rawRef string) (networkDemand, string, *problem) {
	ref := strings.TrimSpace(rawRef)
	if ref == "" {
		return networkDemand{}, "", problemInvalidArgument("networkRef must not be empty")
	}
	o, err := d.Orders.FindByRef(ctx, shared.NetworkRef(ref))
	if err != nil {
		return networkDemand{}, "", problemFromError(err)
	}
	if o == nil {
		return networkDemand{}, "", problemFromError(usecases.ErrOrderNotFound, ref)
	}
	dto := toNetworkOrderDTO(o, d.Clock.Now())
	summary := fmt.Sprintf("%s: %s, %d line(s)%s", dto.NetworkRef, dto.State, len(dto.Lines), overdueSuffix(dto))
	return networkDemand{Count: 1, Orders: []networkOrderDTO{dto}}, summary, nil
}

// parseState validates the optional state filter against the domain's states;
// an unknown value is refused rather than silently matching nothing.
func parseState(raw *string) (networkorder.State, *problem) {
	if raw == nil {
		return "", nil
	}
	state := networkorder.State(strings.TrimSpace(*raw))
	switch state {
	case networkorder.StateNew, networkorder.StateSubmitted, networkorder.StateAcknowledged, networkorder.StateRejected, networkorder.StateConfirmed:
		return state, nil
	default:
		return "", problemInvalidArgument(fmt.Sprintf("unknown state %q: want one of NEW, SUBMITTED, ACKNOWLEDGED, REJECTED, CONFIRMED", *raw))
	}
}

func (d Deps) listOrders(ctx context.Context, state networkorder.State) (networkDemand, string, *problem) {
	orders, err := d.Orders.ListAll(ctx)
	if err != nil {
		return networkDemand{}, "", problemFromError(err)
	}
	now := d.Clock.Now()
	out := networkDemand{Orders: make([]networkOrderDTO, 0, len(orders))}
	overdue := 0
	for _, o := range orders {
		if state != "" && o.State() != state {
			continue
		}
		dto := toNetworkOrderDTO(o, now)
		if dto.AcknowledgementOverdue {
			overdue++
		}
		out.Orders = append(out.Orders, dto)
	}
	sort.Slice(out.Orders, func(i, j int) bool { return out.Orders[i].NetworkRef < out.Orders[j].NetworkRef })
	out.Count = len(out.Orders)
	which := "network orders"
	if state != "" {
		which = string(state) + " network orders"
	}
	return out, fmt.Sprintf("%d %s, %d past the acknowledgement deadline", out.Count, which, overdue), nil
}

func overdueSuffix(dto networkOrderDTO) string {
	if dto.AcknowledgementOverdue {
		return ", acknowledgement overdue"
	}
	return ""
}

// advertisedAvailability is the capability read skill: the stored
// CapabilityOffers, optionally narrowed by sku and/or siteId. It reads the
// latest persisted snapshot and computes nothing.
func (d Deps) advertisedAvailability(ctx context.Context, rawArgs json.RawMessage) (advertisedAvailability, string, *problem) {
	var args availabilityArgs
	if err := decodeStrict(rawArgs, &args); err != nil {
		return advertisedAvailability{}, "", problemInvalidArgument(fmt.Sprintf("arguments: %v", err))
	}
	if d.Offers == nil {
		return advertisedAvailability{}, "", problemDependencyUnavailable("the capability offer store is not wired into this agent")
	}
	for name, v := range map[string]*string{"sku": args.SKU, "siteId": args.SiteId} {
		if v != nil && strings.TrimSpace(*v) == "" {
			return advertisedAvailability{}, "", problemInvalidArgument(name + " must not be empty when given")
		}
	}
	offers, err := d.Offers.ListAll(ctx)
	if err != nil {
		return advertisedAvailability{}, "", problemFromError(err)
	}
	out := advertisedAvailability{Offers: make([]capabilityOfferDTO, 0, len(offers))}
	for _, o := range offers {
		dto := toCapabilityOfferDTO(o)
		if (args.SKU != nil && dto.SKU != strings.TrimSpace(*args.SKU)) || (args.SiteId != nil && dto.SiteId != strings.TrimSpace(*args.SiteId)) {
			continue
		}
		out.Offers = append(out.Offers, dto)
	}
	sort.Slice(out.Offers, func(i, j int) bool {
		if out.Offers[i].SKU != out.Offers[j].SKU {
			return out.Offers[i].SKU < out.Offers[j].SKU
		}
		return out.Offers[i].SiteId < out.Offers[j].SiteId
	})
	out.Count = len(out.Offers)
	return out, fmt.Sprintf("%d advertised offer(s)", out.Count), nil
}

// problemFromError maps a use case or store error to a problem answer, using
// the SAME classification as the REST adapter (internal/adapters/inbound/http
// statusFor) so a delegating agent sees the outcome a REST caller would:
// ErrOrderNotFound is 404 and anything unrecognised is 500. The REST 409
// (confirm-before-acknowledge) and 422 (domain validation) classes belong to
// the shipment-confirmation write and to demand intake, neither of which this
// read-only agent offers, so they cannot occur here. A store failure is a 500
// exactly as in REST, answered as a problem and never raised as a JSON-RPC
// error. The only 503 is adapter-owned: a Deps port left unwired.
func problemFromError(err error, ref ...string) *problem {
	switch {
	case errors.Is(err, usecases.ErrOrderNotFound):
		r := ""
		if len(ref) > 0 {
			r = ref[0]
		}
		return problemOrderNotFound(r)
	default:
		slog.Error("a2a skill failed", "error", err)
		return problemInternal()
	}
}
