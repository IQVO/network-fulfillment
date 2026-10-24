# Derived from: apis/openapi.yaml components.responses.Problem ("A NUL
# character (U+0000) is never valid input (PostgreSQL text cannot hold it):
# in the request path it is a 404 `resource-not-found`, in a query parameter
# a 400 `invalid-request`, and in a request body a 400
# `malformed-request-body`") and the getNetworkOrder 404 description
# ("or a path that carries a NUL character (`resource-not-found`)").
# Implemented by the rejectNUL middleware (docs/adr/0025): without it a fuzzed
# request becomes a 500 on the real Postgres runtime, and the in-memory
# adapters never noticed.
Feature: NUL-byte request hardening
  A NUL byte is never valid input on any endpoint of either deployable. It
  is answered with the same RFC 7807 problem documents as any other client
  error, before the request reaches a handler or a database.

  @bdd
  Scenario: A NUL byte in the request path can never name a resource
    When I GET "/network-orders/po-4%0004"
    Then the response status is 404
    And the problem detail type is "resource-not-found"

  @bdd
  Scenario: A NUL byte in a query parameter is a malformed request
    When I GET "/reports/acknowledgement?from=2026-10-08T00%3A00%3A00Z%00&to=2026-10-09T00%3A00%3A00Z" on the reports service
    Then the response status is 400
    And the problem detail type is "invalid-request"

  @bdd
  Scenario: A NUL byte escape in a request body is a malformed request
    When I send "POST" to "/network-orders/po-1/shipment-confirmation" with body "po\u00001"
    Then the response status is 400
    And the problem detail type is "malformed-request-body"
