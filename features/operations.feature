# Derived from: apis/openapi.yaml (healthz, readyz, metrics, and the paths that
# define this surface as READ-ONLY apart from one write endpoint) and
# .claude/rules/domain-model.md; docs/adr/0004 (graceful shutdown readiness,
# circuit_breaker_state), 0014. Fleet rule: NO authentication anywhere.
Feature: Operations surface
  Liveness is separate from readiness, readiness flips first on shutdown, the
  breaker state is observable, the surface is read-only apart from the one
  ADR 0014 write, and nothing requires credentials.

  @bdd
  Scenario: Liveness reports ok
    When I GET "/healthz"
    Then the response status is 200
    And the response body contains "status":"ok"

  @bdd
  Scenario: Readiness reports ready, then not_ready once shutdown begins, while liveness is unaffected
    When I GET "/readyz"
    Then the response status is 200
    And the response body contains "status":"ready"
    When the service begins graceful shutdown
    And I GET "/readyz"
    Then the response status is 503
    And the response body contains "status":"not_ready"
    When I GET "/healthz"
    Then the response status is 200

  @bdd
  Scenario: Metrics expose the circuit breaker state per dependency
    Given the "order-management" circuit breaker is closed
    When I GET "/metrics"
    Then the response status is 200
    And the response body contains circuit_breaker_state{dependency="order-management"} 0

  @bdd
  Scenario Outline: There is no HTTP intake: demand is polled, never pushed
    When I send "<method>" to "<path>"
    Then the request is refused because the endpoint is read-only
    And the response status is not a success

    Examples:
      | method | path                  |
      | POST   | /network-orders       |
      | PUT    | /network-orders/po-1  |
      | PATCH  | /network-orders/po-1  |
      | DELETE | /network-orders/po-1  |
      | POST   | /inbound-status       |
      | POST   | /capability-offers    |

  @bdd
  Scenario Outline: Every endpoint answers without credentials
    When I GET "<path>" without credentials
    Then the response status is 200
    And the response is not an authentication challenge

    Examples:
      | path                |
      | /healthz            |
      | /readyz             |
      | /metrics            |
      | /network-orders     |
      | /inbound-status     |
      | /capability-offers  |

  @bdd
  Scenario: The reports service is unauthenticated and alive
    When I GET "/healthz" on the reports service
    Then the response status is 200
    And the reports service status is "ok"
    And the response is not an authentication challenge
