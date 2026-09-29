import { describe, expect, it } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse, server } from "../test/mocks/server";
import { NETWORK_FULFILLMENT_API_BASE } from "../config";
import { NetworkFulfillmentScreen } from "./NetworkFulfillmentScreen";
import type { InboundStatus, ListNetworkOrdersResponse } from "../types";

const HEALTHY_STATUS: InboundStatus = {
  networkMode: "stub",
  polls: 42,
  received: 10,
  failed: 0,
  since: "2026-09-26T10:00:00Z",
  unanswered: 1,
  overdue: 0,
};

const UNHEALTHY_STATUS: InboundStatus = {
  networkMode: "stub",
  polls: 3,
  received: 0,
  failed: 3,
  unanswered: 2,
  overdue: 2,
};

const ORDERS: ListNetworkOrdersResponse = {
  networkOrders: [
    {
      networkRef: "po-4711",
      siteId: "site-1",
      state: "NEW",
      requiredShipBy: "2026-09-28T00:00:00Z",
      acknowledgeBy: "2026-09-27T10:00:00Z",
      receivedAt: "2026-09-26T10:00:00Z",
      acknowledgementOverdue: false,
      lines: [
        {
          networkLineRef: "1",
          networkProductId: "ASIN-XYZ123",
          sku: "sku-1",
          quantity: 3,
        },
      ],
    },
  ],
};

function mockEndpoints(orders: ListNetworkOrdersResponse, status: InboundStatus) {
  server.use(
    http.get(`${NETWORK_FULFILLMENT_API_BASE}/network-orders`, () => HttpResponse.json(orders)),
    http.get(`${NETWORK_FULFILLMENT_API_BASE}/inbound-status`, () => HttpResponse.json(status)),
  );
}

describe("NetworkFulfillmentScreen", () => {
  it("shows loading skeletons before either endpoint resolves", () => {
    server.use(
      http.get(`${NETWORK_FULFILLMENT_API_BASE}/network-orders`, () => new Promise(() => {})),
      http.get(`${NETWORK_FULFILLMENT_API_BASE}/inbound-status`, () => new Promise(() => {})),
    );
    render(<NetworkFulfillmentScreen />);
    expect(screen.getByText("Network Fulfillment")).toBeInTheDocument();
    expect(screen.getByRole("table")).toHaveAttribute("aria-busy", "true");
  });

  it("renders the unanswered orders table and healthy poller status", async () => {
    mockEndpoints(ORDERS, HEALTHY_STATUS);
    render(<NetworkFulfillmentScreen />);

    expect(await screen.findByText("po-4711")).toBeInTheDocument();
    expect(screen.getByText("site-1")).toBeInTheDocument();
    expect(screen.getByText("Poller healthy")).toBeInTheDocument();
    expect(screen.getByText("mode: stub", { exact: false })).toBeInTheDocument();
  });

  it("renders the empty state when there are no unanswered orders", async () => {
    mockEndpoints({ networkOrders: [] }, HEALTHY_STATUS);
    render(<NetworkFulfillmentScreen />);

    expect(
      await screen.findByText(/No unanswered network orders/i),
    ).toBeInTheDocument();
  });

  it("flags the poller as unhealthy when overdue > 0", async () => {
    mockEndpoints(ORDERS, UNHEALTHY_STATUS);
    render(<NetworkFulfillmentScreen />);

    expect(await screen.findByText("Poller unhealthy")).toBeInTheDocument();
  });

  it("flags the poller as unhealthy when no poll has ever succeeded (since absent)", async () => {
    mockEndpoints(ORDERS, {
      networkMode: "stub",
      polls: 5,
      received: 0,
      failed: 0,
      unanswered: 1,
      overdue: 0,
    });
    render(<NetworkFulfillmentScreen />);

    await waitFor(() => {
      expect(screen.getByText("Poller unhealthy")).toBeInTheDocument();
    });
    expect(screen.getByText(/never completed cleanly|ever completed cleanly/i)).toBeInTheDocument();
  });

  it("surfaces the acknowledge-by overdue marker on a stale order", async () => {
    const overdueOrder: ListNetworkOrdersResponse = {
      networkOrders: [{ ...ORDERS.networkOrders[0], acknowledgementOverdue: true }],
    };
    mockEndpoints(overdueOrder, HEALTHY_STATUS);
    render(<NetworkFulfillmentScreen />);

    expect(await screen.findByText(/\(overdue\)/)).toBeInTheDocument();
  });
});
