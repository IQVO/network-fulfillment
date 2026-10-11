# Derived from: apis/openapi.yaml (getNetworkOrder, listUnansweredNetworkOrders,
# schemas NetworkOrder / NetworkOrderLine / Problem) and
# .claude/rules/domain-model.md (NetworkOrder: AcknowledgementOverdue,
# acknowledgeBy = receivedAt + 24h); ADR 0001 hard rule 2 (customer PII stops here).
Feature: Observing network orders
  What did we tell the network about purchase order X? The read surface
  answers it without ever exposing the customer behind the order, and lists
  the one working set whose size is an operational signal: orders awaiting
  an answer.

  Background:
    Given the product dictionary maps "ASIN-1" to "sku-1"

  @bdd
  Scenario: An unknown purchase order is a 404 problem document
    When I request network order "po-404"
    Then the response status is 404
    And the problem detail type is "network-order-not-found"

  @bdd
  Scenario: A network order exposes the documented properties and no customer PII
    Given a network order "po-1" in state "ACKNOWLEDGED"
    When I request network order "po-1"
    Then the response status is 200
    And the network order carries only the documented properties
    And the network order has line "1" for product "ASIN-1" translated to SKU "sku-1" with quantity 2

  @bdd
  Scenario: The unanswered list is an empty array, not null, when nothing is waiting
    When I list the unanswered network orders
    Then the response status is 200
    And the unanswered list is empty

  @bdd
  Scenario: Only orders still in NEW are listed as unanswered
    Given a network order "po-new" in state "NEW"
    And a network order "po-submitted" in state "SUBMITTED"
    And a network order "po-rejected" in state "REJECTED"
    And a network order "po-acknowledged" in state "ACKNOWLEDGED"
    And a network order "po-confirmed" in state "CONFIRMED"
    When I list the unanswered network orders
    Then the unanswered orders, soonest deadline first, are "po-new"

  @bdd
  Scenario: Unanswered orders are listed soonest deadline first
    Given a network order "po-c" in state "NEW"
    And 1 hour passes
    And a network order "po-a" in state "NEW"
    And 1 hour passes
    And a network order "po-b" in state "NEW"
    When I list the unanswered network orders
    Then the unanswered orders, soonest deadline first, are "po-c", "po-a", "po-b"

  @bdd
  Scenario Outline: An unanswered order is overdue only once its 24-hour window has strictly closed
    Given a network order "po-1" in state "NEW"
    And <minutes> minutes pass
    When I request network order "po-1"
    Then the network order is <verdict>

    Examples:
      | minutes | verdict     | case                              |
      | 1439    | not overdue | one minute inside the window      |
      | 1440    | not overdue | exactly at the acknowledgeBy time |
      | 1441    | overdue     | one minute past the window        |

  @bdd
  Scenario Outline: An answered order is never overdue, however old it gets
    Given a network order "po-1" in state "<state>"
    And 72 hours pass
    When I request network order "po-1"
    Then the network order is not overdue

    Examples:
      | state        |
      | SUBMITTED    |
      | ACKNOWLEDGED |
      | REJECTED     |
