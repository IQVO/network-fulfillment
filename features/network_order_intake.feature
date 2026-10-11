# Derived from: apis/openapi.yaml (getNetworkOrder, listUnansweredNetworkOrders,
# getInboundStatus) and .claude/rules/domain-model.md (NetworkOrder aggregate,
# ReceiveNetworkDemand use case); docs/adr/0001, 0009, 0016.
#
# Demand never arrives over HTTP: it is POLLED from the network (ADR 0001 §5).
# "Creating" a network order therefore means seeding the stub network and
# running the poller, then observing the result through the REST API.
Feature: Network order intake
  Demand that arrives from the external retail network carries a deadline we
  did not choose. It is translated into our product vocabulary, answered
  exactly once, in full or not at all, and recorded with both vocabularies so
  a disputed order can be reconciled.

  Background:
    Given the product dictionary maps "ASIN-1" to "sku-1"
    And the product dictionary maps "ASIN-2" to "sku-2"

  @bdd
  Scenario: Feasible demand is accepted, held, and recorded as SUBMITTED
    Given the network has demand "po-1" for 3 units of "ASIN-1"
    When the poller runs a pass
    And I request network order "po-1"
    Then the response status is 200
    And the network order state is "SUBMITTED"
    And the network order is "po-1" for site "site-1"
    And the network order has line "1" for product "ASIN-1" translated to SKU "sku-1" with quantity 3
    And the network order has a local order
    And the network was told "po-1" is accepted
    And order-management holds 1 raised order
    And order-management had 0 held orders released
    And the domain event "NetworkOrderReceived" was published for "po-1"
    And the domain event "NetworkOrderSubmitted" was published for "po-1"
    And the domain event "NetworkOrderAcknowledged" was not published for "po-1"

  @bdd
  Scenario: The acknowledgement clock is anchored to arrival and never recomputed
    Given the network has demand "po-1" for 1 unit of "ASIN-1"
    When the poller runs a pass
    And 5 hours pass
    And I request network order "po-1"
    Then the network order was received at "2026-10-08T12:00:00Z"
    And the acknowledgement window closes 24 hours after receipt
    And the network order keeps the required ship-by the network set

  @bdd
  Scenario: Demand whose deadline cannot be met is rejected and its hold freed
    Given order-management cannot meet the required ship-by
    And the network has demand "po-2" for 5 units of "ASIN-1"
    When the poller runs a pass
    And I request network order "po-2"
    Then the network order state is "REJECTED"
    And the network order has no local order
    And the network was told "po-2" is rejected
    And order-management holds 1 raised order
    And order-management had 1 held order cancelled
    And order-management had 0 held orders released
    And the NetworkOrderRejected event for "po-2" carries reason "INFEASIBLE_DEADLINE"

  @bdd
  Scenario Outline: Demand naming a product we cannot translate is rejected in full
    Given the network has demand "po-3" for site "site-1" due in 48 hours with lines:
      | line | product  | quantity |
      | 1    | <first>  | 2        |
      | 2    | <second> | 1        |
    When the poller runs a pass
    And I request network order "po-3"
    Then the network order state is "REJECTED"
    And the network was told "po-3" is rejected
    And order-management holds 0 raised orders
    And the domain event "NetworkOrderReceived" was published for "po-3"
    And the NetworkOrderRejected event for "po-3" carries reason "UNTRANSLATABLE_SKU"

    Examples:
      | first        | second        | case                         |
      | ASIN-GHOST   | ASIN-PHANTOM  | every line is unknown        |
      | ASIN-1       | ASIN-PHANTOM  | one of two lines is unknown  |

  @bdd
  Scenario Outline: A purchase order the network lists again is answered only once
    Given <verdict>
    And the network has demand "po-4" for 2 units of "ASIN-1"
    When the poller runs a pass
    And the network delivers purchase order "po-4" again
    And the poller runs a pass
    And I request network order "po-4"
    Then the network order state is "<state>"
    And the network was told about "po-4" exactly 1 time
    And order-management holds 1 raised order
    And the domain event "NetworkOrderReceived" was published 1 time for "po-4"

    Examples:
      | verdict                                           | state     |
      | order-management can meet the required ship-by    | SUBMITTED |
      | order-management cannot meet the required ship-by | REJECTED  |

  @bdd
  Scenario: order-management is asked in our vocabulary, with the network's deadline untouched
    Given the product dictionary maps "ASIN-1B" to "sku-1"
    And the network has demand "po-5" for site "site-2" due in 72 hours with lines:
      | line | product | quantity |
      | 1    | ASIN-1  | 2        |
      | 2    | ASIN-1B | 3        |
      | 3    | ASIN-2  | 1        |
    When the poller runs a pass
    Then the held order is for site "site-2"
    And the held order asks order-management for 5 units of "sku-1"
    And the held order asks order-management for 1 unit of "sku-2"
    And the held order names 2 SKUs
    And the held order carries the required ship-by of "po-5" unchanged

  @bdd
  Scenario Outline: Malformed demand is never recorded and is counted as failed
    Given the network has demand "<ref>" for site "site-1" due in 48 hours with lines:
      | line   | product | quantity   |
      | <line> | ASIN-1  | <quantity> |
    When the poller runs a pass
    And I list the unanswered network orders
    Then the unanswered list is empty
    And the network was told about "<ref>" exactly 0 times
    When I request the inbound status
    Then the inbound status reports failed 1
    And the inbound status reports received 0

    Examples:
      | case                 | ref  | line | quantity |
      | empty network ref    |      | 1    | 2        |
      | empty line ref       | po-9 |      | 2        |
      | zero quantity        | po-9 | 1    | 0        |
      | negative quantity    | po-9 | 1    | -3       |

  @bdd
  Scenario: Demand with no lines at all is never recorded
    Given the network has demand "po-9" for site "site-1" due in 48 hours with lines:
      | line | product | quantity |
    When the poller runs a pass
    And I list the unanswered network orders
    Then the unanswered list is empty
    When I request the inbound status
    Then the inbound status reports failed 1

  @bdd
  Scenario: A demand that cannot be answered yet stays NEW and visible as unanswered
    Given order-management is unavailable
    And the network has demand "po-6" for 2 units of "ASIN-1"
    When the poller runs a pass
    And I request network order "po-6"
    Then the network order state is "NEW"
    And the network order has no local order
    And the network was told about "po-6" exactly 0 times
    When I list the unanswered network orders
    Then the unanswered orders, soonest deadline first, are "po-6"

  # KNOWN BUG (docs/adr/0018, finding 1): ReceiveNetworkDemand saves the order
  # as NEW BEFORE asking order-management for a verdict, and its idempotency
  # guard (FindByRef != nil -> return existing) then short-circuits every
  # retry, so the poller counts the re-fetched demand as "received" while the
  # order is never answered and sits NEW until the sweep rejects it as
  # ACKNOWLEDGEMENT_DEADLINE_MISSED. apis/openapi.yaml says a failed unit
  # "will be re-fetched ... processing is idempotent on networkRef, which is
  # what makes the retry safe" - i.e. the retry must answer it.
  @bdd @known-bug
  Scenario: Demand that failed during an order-management outage is answered on a later pass
    Given order-management is unavailable
    And the network has demand "po-7" for 2 units of "ASIN-1"
    When the poller runs a pass
    And order-management is available again
    And the poller runs a pass
    And I request network order "po-7"
    Then the network order state is "SUBMITTED"
    And the network was told "po-7" is accepted
