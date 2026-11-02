import type { SidebarsConfig } from "@docusaurus/plugin-content-docs";

const sidebar: SidebarsConfig = {
  apisidebar: [
    {
      type: "doc",
      id: "api-reference/rest/network-fulfillment-api",
    },
    {
      type: "category",
      label: "network-orders",
      link: {
        type: "doc",
        id: "api-reference/rest/network-orders",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/list-unanswered-network-orders",
          label: "List orders still awaiting an answer",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-network-order",
          label: "Get one network order",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/confirm-network-order-shipment",
          label: "Confirm shipment of one network order",
          className: "api-method post",
        },
      ],
    },
    {
      type: "category",
      label: "capability-offers",
      link: {
        type: "doc",
        id: "api-reference/rest/capability-offers",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/list-capability-offers",
          label: "List every currently-advertised capability offer",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "operations",
      link: {
        type: "doc",
        id: "api-reference/rest/operations",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/healthz",
          label: "Liveness probe",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/readyz",
          label: "Readiness probe",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/metrics",
          label: "Prometheus metrics",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-inbound-status",
          label: "Is the inbound polling leg alive?",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "reports",
      link: {
        type: "doc",
        id: "api-reference/rest/reports",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/get-acknowledgement-report",
          label: "Network Order Acknowledgement & Translation report",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-acknowledgement-report-freshness",
          label: "How far the report lags real time",
          className: "api-method get",
        },
      ],
    },
  ],
};

export default sidebar.apisidebar;
