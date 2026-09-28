/** Wire types mirroring apis/openapi.yaml's NetworkOrder / NetworkOrderLine /
 *  InboundStatus schemas exactly -- kept hand-in-sync with the OpenAPI spec
 *  rather than code-generated for v1, same convention as every sibling
 *  remote's types.ts. */

/** NetworkOrder.state -- see internal/domain/networkorder/network_order.go's
 *  State enum. NEW is received/unanswered; ACKNOWLEDGED is a full-fulfil
 *  commitment (no partial acknowledgement in this protocol); REJECTED is a
 *  refusal (a valid answer, not a failure); CONFIRMED means shipment has
 *  been confirmed to the network. */
export type NetworkOrderState = "NEW" | "ACKNOWLEDGED" | "REJECTED" | "CONFIRMED";

/** Mirrors NetworkOrderLine in apis/openapi.yaml. */
export interface NetworkOrderLine {
  networkLineRef: string;
  networkProductId: string;
  sku: string;
  quantity: number;
}

/** Mirrors NetworkOrder in apis/openapi.yaml. localOrderId is absent until
 *  acknowledged -- local work never exists for demand not yet committed to. */
export interface NetworkOrder {
  networkRef: string;
  siteId: string;
  state: NetworkOrderState;
  requiredShipBy: string;
  acknowledgeBy: string;
  receivedAt: string;
  localOrderId?: string;
  acknowledgementOverdue: boolean;
  lines: NetworkOrderLine[];
}

/** Mirrors the GET /network-orders response envelope. */
export interface ListNetworkOrdersResponse {
  networkOrders: NetworkOrder[];
}

/** Mirrors InboundStatus in apis/openapi.yaml -- the poller-health surface.
 *  `since` is ABSENT before any poll has completed cleanly, which after
 *  real uptime is itself the signal that no poll has ever succeeded. */
export interface InboundStatus {
  networkMode: "stub" | "sandbox" | "live";
  polls: number;
  received: number;
  failed: number;
  since?: string;
  unanswered: number;
  overdue: number;
}
