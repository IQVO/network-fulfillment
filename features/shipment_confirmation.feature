# Derived from: apis/openapi.yaml (confirmNetworkOrderShipment:
# POST /network-orders/{networkRef}/shipment-confirmation -> 204/404/409/422/500)
# and .claude/rules/domain-model.md (NetworkOrder.ConfirmShipment only from
# ACKNOWLEDGED, ErrConfirmBeforeAcknowledge; ConfirmNetworkOrderShipment use
# case); docs/adr/0014, 0015.
Feature: Shipment confirmation
  The one write endpoint of this context. It tells the network an order has
  shipped. A shipment may not be confirmed for an order we never committed
  to, and a retried call must never re-submit to the network or re-publish
  the event.

  Background:
    Given the product dictionary maps "ASIN-1" to "sku-1"

  @bdd
  Scenario: Confirming an acknowledged order closes it and tells the network
    Given a network order "po-1" in state "ACKNOWLEDGED"
    When I confirm shipment of network order "po-1"
    Then the response status is 204
    And the response body is empty
    And the network accepted 1 shipment confirmation for "po-1"
    And the domain event "NetworkOrderShipmentConfirmed" was published 1 time for "po-1"
    When I request network order "po-1"
    Then the network order state is "CONFIRMED"

  @bdd
  Scenario: Confirming twice is idempotent: 204 both times, one submission, one event
    Given a network order "po-1" in state "ACKNOWLEDGED"
    When I confirm shipment of network order "po-1"
    And I confirm shipment of network order "po-1"
    Then the response status is 204
    And the network accepted 1 shipment confirmation for "po-1"
    And the domain event "NetworkOrderShipmentConfirmed" was published 1 time for "po-1"

  @bdd
  Scenario: Confirming shipment for an unknown order is a 404
    When I confirm shipment of network order "po-404"
    Then the response status is 404
    And the problem detail type is "network-order-not-found"

  @bdd
  Scenario Outline: Shipment cannot be confirmed for an order we have not committed to
    Given a network order "po-1" in state "<state>"
    When I confirm shipment of network order "po-1"
    Then the response status is 409
    And the problem detail type is "confirm-before-acknowledge"
    And the network was asked to confirm shipment of 0 times for "po-1"
    And the domain event "NetworkOrderShipmentConfirmed" was not published for "po-1"
    When I request network order "po-1"
    Then the network order state is "<state>"

    Examples:
      | state     | case                                          |
      | NEW       | never answered                                |
      | SUBMITTED | answered, but the network has not settled it  |
      | REJECTED  | refused, so there is nothing to ship          |

  # The spec documents this as designed behaviour (apis/openapi.yaml, 500):
  # the order is saved CONFIRMED BEFORE the network is told, so after a failed
  # submission a retry answers 204 without re-submitting. See docs/adr/0018,
  # finding 3: the network is therefore never told about that shipment.
  @bdd
  Scenario: A failed submission to the network surfaces as 500 and a retry answers 204 without re-submitting
    Given a network order "po-1" in state "ACKNOWLEDGED"
    And the network will fail shipment confirmations
    When I confirm shipment of network order "po-1"
    Then the response status is 500
    And the problem detail type is "internal-error"
    When the network accepts shipment confirmations again
    And I confirm shipment of network order "po-1"
    Then the response status is 204
    And the network was asked to confirm shipment of 1 time for "po-1"
    And the domain event "NetworkOrderShipmentConfirmed" was published 1 time for "po-1"

  # KNOWN SPEC/ROUTER MISMATCH (docs/adr/0018, finding 2): the spec documents
  # 422 `empty-network-ref` for an empty networkRef, but net/http's ServeMux
  # cleans "//" and answers a redirect (307) before the handler runs, so the
  # branch is unreachable over HTTP. The scenario states the spec'd behaviour.
  @bdd @known-bug
  Scenario: An empty networkRef is a 422 problem document
    When I confirm shipment of network order ""
    Then the response status is 422
    And the problem detail type is "empty-network-ref"
