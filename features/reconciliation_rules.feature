# Derived from the context documentation (warehouse-docs network-fulfillment:
# use-cases.md ReconcileSubmittedOrders, aggregate-design-canvas.md
# "Corrective Policies / Reconciliation", domain-events.md, troubleshooting.md
# "Network orders" rows on SUBMITTED and ACKNOWLEDGED orders) and docs/adr/0016
# and 0024. acknowledgement_lifecycle.feature pins SUCCESS, FAILURE and PENDING
# once each; these scenarios pin what the docs say happens around them.
Feature: Reconciliation, documented rules
  The network's transaction-status record settles a SUBMITTED order: SUCCESS
  acknowledges and releases the hold, FAILURE frees the hold and rejects, and
  PENDING or a failed lookup leaves the order for the next pass. One order never
  stops the pass.

  Background:
    Given the product dictionary maps "ASIN-1" to "sku-1"

  @bdd
  Scenario: A refusal whose hold cannot be cancelled leaves the order SUBMITTED and is retried
    Given a network order "po-1" in state "SUBMITTED"
    And the network will refuse the submission for "po-1"
    And order-management fails to cancel held orders
    When the acknowledgement reconciliation runs
    And I request network order "po-1"
    Then the network order state is "SUBMITTED"
    And the hold of network order "po-1" is open
    And the domain event "NetworkOrderRejected" was not published for "po-1"
    When order-management cancels held orders again
    And the acknowledgement reconciliation runs
    And I request network order "po-1"
    Then the network order state is "REJECTED"
    And the hold of network order "po-1" is settled as "CANCELLED"
    And the NetworkOrderRejected event for "po-1" carries reason "SUBMISSION_FAILED"
    And the domain event "NetworkOrderRejected" was published 1 time for "po-1"

  @bdd
  Scenario: A submission still pending is settled on a later pass and released exactly once
    Given a network order "po-1" in state "SUBMITTED"
    And the network has not yet settled the submission for "po-1"
    When the acknowledgement reconciliation runs
    And the acknowledgement reconciliation runs
    Then order-management had 0 held orders released
    And the domain event "NetworkOrderAcknowledged" was not published for "po-1"
    When the network settles the submission for "po-1"
    And the acknowledgement reconciliation runs
    And the acknowledgement reconciliation runs
    And I request network order "po-1"
    Then the network order state is "ACKNOWLEDGED"
    And order-management had 1 held order released
    And the domain event "NetworkOrderAcknowledged" was published 1 time for "po-1"

  # troubleshooting.md: "Order is ACKNOWLEDGED but the work never reached the
  # floor": the release is the last step, after the commit, and is not retried.
  @bdd
  Scenario: A release that fails after the acknowledgement commit is not retried and the hold is never cancelled
    Given a network order "po-1" in state "SUBMITTED"
    And order-management fails to release held orders
    When the acknowledgement reconciliation runs
    And the acknowledgement reconciliation runs
    And 72 hours pass
    And the orphaned-hold sweep runs
    And overdue orders are rejected
    And I request network order "po-1"
    Then the network order state is "ACKNOWLEDGED"
    And the domain event "NetworkOrderAcknowledged" was published 1 time for "po-1"
    And order-management was asked to release 1 time in total
    And order-management had 0 held orders released
    And order-management was asked to cancel 0 times in total

  @bdd
  Scenario: A transaction-status lookup that fails for one order does not stop the pass and is retried
    Given a network order "po-1" in state "SUBMITTED"
    And a network order "po-2" in state "SUBMITTED"
    And the network cannot be asked about the submission of "po-1"
    When the acknowledgement reconciliation runs
    And I request network order "po-2"
    Then the network order state is "ACKNOWLEDGED"
    When I request network order "po-1"
    Then the network order state is "SUBMITTED"
    When the network can be asked about the submission of "po-1" again
    And the acknowledgement reconciliation runs
    And I request network order "po-1"
    Then the network order state is "ACKNOWLEDGED"
    And the domain event "NetworkOrderAcknowledged" was published 1 time for "po-1"
    And the domain event "NetworkOrderAcknowledged" was published 1 time for "po-2"
    And order-management had 2 held orders released

  @bdd
  Scenario Outline: Reconciliation looks only at SUBMITTED orders
    Given a network order "po-1" in state "<state>"
    When the acknowledgement reconciliation runs
    And I request network order "po-1"
    Then the network order state is "<state>"
    And the domain event "NetworkOrderAcknowledged" was published <acknowledgements> times for "po-1"
    And order-management had <released> held orders released

    Examples:
      | state        | acknowledgements | released | case                                               |
      | NEW          | 0                | 0        | never answered: the sweeps own it                  |
      | REJECTED     | 0                | 0        | refused: nothing to settle                         |
      | ACKNOWLEDGED | 1                | 1        | already settled by the fixture, not settled twice  |
      | CONFIRMED    | 1                | 1        | already settled by the fixture, not settled twice  |

  @bdd
  Scenario: Submission and settlement are announced with the held order they concern
    Given the network has demand "po-1" for 2 units of "ASIN-1"
    When the poller runs a pass
    And the acknowledgement reconciliation runs
    Then the "NetworkOrderSubmitted" event for "po-1" names the local order of "po-1"
    And the "NetworkOrderAcknowledged" event for "po-1" names the local order of "po-1"
    And the "NetworkOrderAcknowledged" event for "po-1" carries receivedAt "2026-10-08T12:00:00Z"
    And the "NetworkOrderAcknowledged" event for "po-1" carries siteId "site-1"
    And the "NetworkOrderSubmitted" event for "po-1" carries receivedAt "2026-10-08T12:00:00Z"

  @bdd
  Scenario: A rejection announced after a refused submission carries the site it concerned
    Given a network order "po-1" in state "SUBMITTED"
    And the network will refuse the submission for "po-1"
    When the acknowledgement reconciliation runs
    Then the "NetworkOrderRejected" event for "po-1" carries siteId "site-1"
    And the "NetworkOrderRejected" event for "po-1" carries reason "SUBMISSION_FAILED"

  @bdd
  Scenario: Shipment confirmation is announced with the held order that shipped
    Given a network order "po-1" in state "ACKNOWLEDGED"
    When I confirm shipment of network order "po-1"
    Then the "NetworkOrderShipmentConfirmed" event for "po-1" names the local order of "po-1"
    And the "NetworkOrderShipmentConfirmed" event for "po-1" carries siteId "site-1"
