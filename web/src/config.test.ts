import { describe, expect, it } from "vitest";
import { resolveNetworkFulfillmentApiBase } from "./config";

describe("resolveNetworkFulfillmentApiBase", () => {
  it("builds the production API base from the runtime API origin", () => {
    expect(
      resolveNetworkFulfillmentApiBase({ apiOrigin: "http://localhost:8000" }, true),
    ).toBe("http://localhost:8000/api/network-fulfillment");
  });

  it("normalizes a trailing slash on the runtime API origin", () => {
    expect(
      resolveNetworkFulfillmentApiBase({ apiOrigin: "https://warehouse.example/" }, true),
    ).toBe("https://warehouse.example/api/network-fulfillment");
  });

  it("fails loudly when production runtime configuration has no API origin", () => {
    expect(() => resolveNetworkFulfillmentApiBase({}, true)).toThrow(
      "window.__WAREHOUSE_CONFIG__.apiOrigin is required in production",
    );
  });

  it("retains the existing standalone API origin in Vite development", () => {
    expect(resolveNetworkFulfillmentApiBase({}, false)).toBe("http://localhost:8088");
  });
});
