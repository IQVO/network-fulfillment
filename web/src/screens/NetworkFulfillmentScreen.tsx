import { NETWORK_FULFILLMENT_API_BASE } from "../config";
import type { InboundStatus, ListNetworkOrdersResponse, NetworkOrder } from "../types";
import { Card, StatusPill, DataTable, useFetch } from "@warehouse/ui-kit";

/**
 * network-fulfillment observation screen. This context's REST surface is
 * deliberately READ-ONLY (apis/openapi.yaml's own description block):
 * demand enters only by polling the external network, so there is no
 * intake form here, only the two questions an operator actually has
 * during an incident:
 *
 *   - GET /network-orders      -> the unanswered working set (state=NEW),
 *     soonest deadline first -- the only listing exposed, because it is
 *     the only one whose size is an operational signal.
 *   - GET /inbound-status      -> is the poller alive? A stalled poller is
 *     SILENT (no orders, no errors, a healthy /healthz), so this counter
 *     set is the only way to see it. `overdue` above zero and `polls`
 *     climbing while `since` stays put are both worth alerting on.
 *
 * Verified against the real response shapes documented in
 * apis/openapi.yaml (NetworkOrder, NetworkOrderLine, InboundStatus).
 */
export function NetworkFulfillmentScreen() {
  const {
    data: ordersData,
    loading: ordersLoading,
    error: ordersError,
  } = useFetch<ListNetworkOrdersResponse>(`${NETWORK_FULFILLMENT_API_BASE}/network-orders`, {
    pollMs: 30_000,
  });

  const {
    data: inboundStatus,
    loading: statusLoading,
    error: statusError,
  } = useFetch<InboundStatus>(`${NETWORK_FULFILLMENT_API_BASE}/inbound-status`, {
    pollMs: 30_000,
  });

  const orders = ordersData?.networkOrders ?? [];

  // A poller with no successful pass ever has no `since` -- distinct from
  // "working but nothing to fetch". overdue > 0 is the fact worth alerting
  // on: demand whose 24h acknowledgement window has already closed.
  const pollerNeverSucceeded = !statusLoading && !statusError && inboundStatus && !inboundStatus.since;
  const pollerOverdue = (inboundStatus?.overdue ?? 0) > 0;
  const pollerUnhealthy = Boolean(pollerNeverSucceeded || pollerOverdue);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-5)" }}>
      <div>
        <h1 style={{ fontSize: "var(--wh-font-size-2xl)", margin: 0 }}>Network Fulfillment</h1>
        <p style={{ color: "var(--wh-color-text-muted)", marginTop: 4 }}>
          network-fulfillment · demand received from the external retail network, and the
          answer we gave it
        </p>
      </div>

      <Card title="Poller health">
        {statusError && (
          <div style={{ color: "var(--wh-color-status-danger)" }}>{statusError.message}</div>
        )}

        {statusLoading && (
          <div
            style={{
              height: 40,
              width: 220,
              borderRadius: 6,
              background: "var(--wh-color-border-subtle)",
              animation: "wh-shimmer 1.4s ease-in-out infinite",
            }}
          />
        )}

        {inboundStatus && !statusLoading && (
          <div style={{ display: "flex", flexDirection: "column", gap: "var(--wh-space-3)" }}>
            <div style={{ display: "flex", alignItems: "center", gap: "var(--wh-space-3)" }}>
              <StatusPill
                status={pollerUnhealthy ? "Poller unhealthy" : "Poller healthy"}
                tone={pollerUnhealthy ? "danger" : "success"}
              />
              <span style={{ color: "var(--wh-color-text-muted)", fontSize: "var(--wh-font-size-sm)" }}>
                mode: {inboundStatus.networkMode}
              </span>
            </div>
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fit, minmax(120px, 1fr))",
                gap: "var(--wh-space-3)",
              }}
            >
              <Stat label="Polls" value={inboundStatus.polls} />
              <Stat label="Received" value={inboundStatus.received} />
              <Stat label="Failed" value={inboundStatus.failed} />
              <Stat label="Unanswered" value={inboundStatus.unanswered} />
              <Stat
                label="Overdue"
                value={inboundStatus.overdue}
                danger={inboundStatus.overdue > 0}
              />
            </div>
            <div style={{ color: "var(--wh-color-text-muted)", fontSize: "var(--wh-font-size-sm)" }}>
              {inboundStatus.since
                ? `Last successful poll watermark: ${inboundStatus.since}`
                : "No poll pass has ever completed cleanly -- the inbound leg has never succeeded."}
            </div>
          </div>
        )}
      </Card>

      <Card title="Unanswered network orders">
        {ordersError && (
          <div style={{ color: "var(--wh-color-status-danger)", marginBottom: "var(--wh-space-3)" }}>
            {ordersError.message}
          </div>
        )}

        <DataTable<NetworkOrder>
          rowKey={(o) => o.networkRef}
          rows={orders}
          loading={ordersLoading}
          emptyState={
            <div style={{ color: "var(--wh-color-text-muted)", fontSize: "var(--wh-font-size-sm)" }}>
              No unanswered network orders -- everything received so far has been acknowledged,
              rejected, or confirmed.
            </div>
          }
          columns={[
            { key: "networkRef", header: "Network Ref", render: (o) => o.networkRef },
            { key: "siteId", header: "Site", render: (o) => o.siteId },
            {
              key: "state",
              header: "State",
              // network-fulfillment's NEW/ACKNOWLEDGED/REJECTED/CONFIRMED
              // are not yet in ui-kit's StatusPill DomainStatus union --
              // falls back to StatusPill's neutral tone until a follow-up
              // PR in warehouse-ui-kit adds them (see PR description).
              render: (o) => <StatusPill status={o.state} tone="neutral" size="sm" />,
            },
            {
              key: "requiredShipBy",
              header: "Required Ship By",
              render: (o) => o.requiredShipBy,
            },
            {
              key: "acknowledgeBy",
              header: "Acknowledge By",
              render: (o) => (
                <span style={{ color: o.acknowledgementOverdue ? "var(--wh-color-status-danger)" : undefined }}>
                  {o.acknowledgeBy}
                  {o.acknowledgementOverdue ? " (overdue)" : ""}
                </span>
              ),
            },
            {
              key: "localOrderId",
              header: "Local Order",
              render: (o) => o.localOrderId ?? "—",
            },
          ]}
        />
      </Card>
    </div>
  );
}

function Stat({ label, value, danger }: { label: string; value: number; danger?: boolean }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
      <span
        style={{
          fontSize: "var(--wh-font-size-xl)",
          fontWeight: 700,
          color: danger ? "var(--wh-color-status-danger)" : undefined,
        }}
      >
        {value}
      </span>
      <span style={{ color: "var(--wh-color-text-muted)", fontSize: "var(--wh-font-size-xs)" }}>
        {label}
      </span>
    </div>
  );
}
