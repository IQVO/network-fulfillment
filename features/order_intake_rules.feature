# Derived from the context documentation (warehouse-docs network-fulfillment:
# use-cases.md ReceiveNetworkDemand, aggregate-design-canvas.md invariants and
# corrective policies, eventstorming.md diagram 1, domain-events.md,
# troubleshooting.md "Network orders") and docs/adr/0024. The older
# network_order_intake.feature pins the happy path and the verdicts; these
# scenarios pin the idempotent-receipt / resume rule, the recorded hold and the
# lineless order the documentation describes.
Feature: Network demand intake, documented rules
  A demand the network lists again is never answered twice, an unanswered one
  inside its window RESUMES, one past its window is left to the sweeps, and the
  hold is recorded on the order the moment order-management raises it.

  Background:
    Given the product dictionary maps "ASIN-1" to "sku-1"
    And the product dictionary maps "ASIN-2" to "sku-2"

  @bdd
  Scenario: A refusal whose cancel failed is retried inside the window and frees the same hold
    Given order-management cannot meet the required ship-by
    And order-management fails to cancel held orders
    And the network has demand "po-1" for 2 units of "ASIN-1"
    When the poller runs a pass
    And I request network order "po-1"
    Then the network order state is "NEW"
    And the network order has no local order
    And the hold of network order "po-1" is open
    And the network was told about "po-1" exactly 0 times
    When I request the inbound status
    Then the inbound status reports failed 1
    When order-management cancels held orders again
    And the poller runs a pass
    And I request network order "po-1"
    Then the network order state is "REJECTED"
    And the network order has no local order
    And the hold of network order "po-1" is settled as "CANCELLED"
    And order-management holds 2 raised orders
    And order-management knows 1 distinct held order
    And the network was told about "po-1" exactly 1 time
    And the network was told "po-1" is rejected
    And the domain event "NetworkOrderReceived" was published 1 time for "po-1"
    And the domain event "NetworkOrderRejected" was published 1 time for "po-1"
    And the NetworkOrderRejected event for "po-1" carries reason "INFEASIBLE_DEADLINE"

  @bdd
  Scenario Outline: An unanswered order is resumed only while its acknowledgement window is open
    Given order-management is unavailable
    And the network has demand "po-1" for 2 units of "ASIN-1"
    When the poller runs a pass
    And <elapsed> seconds pass
    And order-management is available again
    And the poller runs a pass
    And I request network order "po-1"
    Then the network order state is "<state>"
    And the network was told about "po-1" exactly <told> times
    And order-management holds <raised> raised orders

    Examples:
      | elapsed | state     | told | raised | case                                                    |
      | 86399   | SUBMITTED | 1    | 1      | a second before the window closes it is still answered  |
      | 86400   | SUBMITTED | 1    | 1      | the window closes strictly after 24h, at 24h it is open |
      | 86401   | NEW       | 0    | 0      | one second late nothing is answered, the sweeps own it  |

  @bdd
  Scenario: An order past its window is left unanswered even though the network lists it again
    Given order-management is unavailable
    And the network has demand "po-1" for 2 units of "ASIN-1"
    When the poller runs a pass
    And 25 hours pass
    And order-management is available again
    And the poller runs a pass
    And I request network order "po-1"
    Then the network order state is "NEW"
    And the network order is overdue
    And the network was told about "po-1" exactly 0 times
    When overdue orders are rejected
    And I request network order "po-1"
    Then the network order state is "REJECTED"
    And the NetworkOrderRejected event for "po-1" carries reason "ACKNOWLEDGEMENT_DEADLINE_MISSED"

  @bdd
  Scenario: An untranslatable order recorded but never answered is rejected when the network lists it again
    Given an untranslatable network order "po-3" was recorded but never answered
    When the poller runs a pass
    And I request network order "po-3"
    Then the network order state is "REJECTED"
    And the network order has no lines
    And the NetworkOrderRejected event for "po-3" carries reason "UNTRANSLATABLE_SKU"
    And the network was told "po-3" is rejected
    And order-management holds 0 raised orders

  @bdd
  Scenario: Untranslatable demand is recorded without lines and received with a line count of zero
    Given the network has demand "po-3" for 2 units of "ASIN-GHOST"
    When the poller runs a pass
    And I request network order "po-3"
    Then the network order state is "REJECTED"
    And the network order has no lines
    And the network order has no local order
    And the "NetworkOrderReceived" event for "po-3" carries lineCount 0
    And the "NetworkOrderReceived" event for "po-3" carries siteId "site-1"
    And order-management holds 0 raised orders
    And order-management was asked to cancel 0 times in total

  @bdd
  Scenario: Receipt is announced with the line count and the deadline the network must be answered by
    Given the network has demand "po-5" for site "site-1" due in 72 hours with lines:
      | line | product | quantity |
      | 1    | ASIN-1  | 2        |
      | 2    | ASIN-2  | 1        |
    When the poller runs a pass
    Then the "NetworkOrderReceived" event for "po-5" carries lineCount 2
    And the "NetworkOrderReceived" event for "po-5" carries acknowledgeBy "2026-10-09T12:00:00Z"

  @bdd
  Scenario: One malformed unit never stops the rest of the poll, and is counted again on every pass
    Given the network has demand "po-bad" for site "site-1" due in 48 hours with lines:
      | line | product | quantity |
      | 1    | ASIN-1  | 0        |
    And the network has demand "po-good" for 2 units of "ASIN-1"
    When the poller runs a pass
    And I request network order "po-good"
    Then the network order state is "SUBMITTED"
    When I request network order "po-bad"
    Then the response status is 404
    When I request the inbound status
    Then the inbound status reports received 1
    And the inbound status reports failed 1
    And the inbound status has no watermark
    When the poller runs a pass
    And I request the inbound status
    Then the inbound status reports received 1
    And the inbound status reports failed 2
    And the network was told about "po-good" exactly 1 time
