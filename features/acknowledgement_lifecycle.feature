# Derived from: apis/openapi.yaml (schema NetworkOrder.state: NEW, SUBMITTED,
# ACKNOWLEDGED, REJECTED, CONFIRMED) and .claude/rules/domain-model.md
# (ReconcileSubmittedOrders, SweepAcknowledgementDeadlines, RejectOverdueOrders);
# docs/adr/0001 §5-§6, 0016. The network's reply to a submission is only
# accepted-for-processing until its transaction-status record says otherwise.
Feature: Acknowledgement lifecycle
  An order is acknowledged only when the network's own transaction-status
  record confirms our submission; a refusal frees the hold; and an order whose
  24-hour window closes unanswered is reported, then rejected - never
  acknowledged late.

  Background:
    Given the product dictionary maps "ASIN-1" to "sku-1"

  @bdd
  Scenario: Reconciliation settles a SUBMITTED order and releases the held order
    Given a network order "po-1" in state "SUBMITTED"
    When the acknowledgement reconciliation runs
    And I request network order "po-1"
    Then the network order state is "ACKNOWLEDGED"
    And the network order has a local order
    And the held order was released and not cancelled
    And the domain event "NetworkOrderAcknowledged" was published 1 time for "po-1"

  @bdd
  Scenario: A submission the network has not settled leaves the order SUBMITTED and the hold in place
    Given a network order "po-1" in state "SUBMITTED"
    And the network has not yet settled the submission for "po-1"
    When the acknowledgement reconciliation runs
    And I request network order "po-1"
    Then the network order state is "SUBMITTED"
    And order-management had 0 held orders released
    And order-management had 0 held orders cancelled
    And the domain event "NetworkOrderAcknowledged" was not published for "po-1"

  @bdd
  Scenario: A submission the network refuses rejects the order and frees the hold
    Given a network order "po-1" in state "SUBMITTED"
    And the network will refuse the submission for "po-1"
    When the acknowledgement reconciliation runs
    And I request network order "po-1"
    Then the network order state is "REJECTED"
    And order-management had 1 held order cancelled
    And order-management had 0 held orders released
    And the NetworkOrderRejected event for "po-1" carries reason "SUBMISSION_FAILED"
    And the domain event "NetworkOrderAcknowledged" was not published for "po-1"

  @bdd
  Scenario: Reconciling twice acknowledges an order only once
    Given a network order "po-1" in state "SUBMITTED"
    When the acknowledgement reconciliation runs
    And the acknowledgement reconciliation runs
    Then the domain event "NetworkOrderAcknowledged" was published 1 time for "po-1"
    And order-management had 1 held order released

  @bdd
  Scenario: The deadline sweep reports an overdue order but does not change it
    Given a network order "po-1" in state "NEW"
    And 25 hours pass
    When the acknowledgement deadline sweep runs
    And I request network order "po-1"
    Then the network order state is "NEW"
    And the network order is overdue
    And the domain event "AcknowledgementDeadlineAtRisk" was published 1 time for "po-1"

  @bdd
  Scenario: The deadline sweep re-reports a still-overdue order on every pass
    Given a network order "po-1" in state "NEW"
    And 25 hours pass
    When the acknowledgement deadline sweep runs
    And the acknowledgement deadline sweep runs
    Then the domain event "AcknowledgementDeadlineAtRisk" was published 2 times for "po-1"

  @bdd
  Scenario: The deadline sweep ignores an order still inside its window
    Given a network order "po-1" in state "NEW"
    And 23 hours pass
    When the acknowledgement deadline sweep runs
    Then the domain event "AcknowledgementDeadlineAtRisk" was not published for "po-1"

  @bdd
  Scenario: An order whose window closed unanswered is rejected, not acknowledged late
    Given a network order "po-1" in state "NEW"
    And 25 hours pass
    When overdue orders are rejected
    And I request network order "po-1"
    Then the network order state is "REJECTED"
    And the NetworkOrderRejected event for "po-1" carries reason "ACKNOWLEDGEMENT_DEADLINE_MISSED"
    And the network was told about "po-1" exactly 0 times
    And the domain event "NetworkOrderAcknowledged" was not published for "po-1"

  @bdd
  Scenario Outline: Rejecting overdue orders leaves everything not overdue alone
    Given a network order "po-1" in state "<state>"
    And <hours> hours pass
    When overdue orders are rejected
    And I request network order "po-1"
    Then the network order state is "<state>"
    And the domain event "NetworkOrderRejected" was published <rejections> times for "po-1"

    Examples:
      | state        | hours | rejections | case                                     |
      | NEW          | 23    | 0          | unanswered but still inside its window   |
      | SUBMITTED    | 72    | 0          | answered orders are never overdue        |
      | ACKNOWLEDGED | 72    | 0          | answered orders are never overdue        |
