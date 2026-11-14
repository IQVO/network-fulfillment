# Derived from the context documentation (warehouse-docs network-fulfillment:
# use-cases.md SweepAcknowledgementDeadlines / RejectOverdueOrders /
# SweepOrphanedHolds, aggregate-design-canvas.md "Corrective Policies" and the
# hold invariants, domain-events.md OrphanedHoldCancelled, troubleshooting.md
# "Network orders") and docs/adr/0024. The older acknowledgement_lifecycle.feature
# and orphaned_holds.feature pin the happy paths; these scenarios pin the
# boundaries, the answers of order-management the docs classify, the per-order
# isolation of every pass, and the event payload.
Feature: Deadline and hold policies, documented rules
  The 24-hour window closes strictly after 24 hours; an overdue order is
  reported then refused, never answered late; and a hold nobody decided is
  freed only strictly after acknowledgeBy plus the grace, whatever
  order-management answers to the cancel.

  Background:
    Given the product dictionary maps "ASIN-1" to "sku-1"

  # ---- the window boundary ---------------------------------------------------

  @bdd
  Scenario Outline: The deadline sweep reports an order only once its window has strictly closed
    Given a network order "po-1" in state "NEW"
    And <elapsed> seconds pass
    When the acknowledgement deadline sweep runs
    Then the domain event "AcknowledgementDeadlineAtRisk" was published <reports> times for "po-1"

    Examples:
      | elapsed | reports | case                                         |
      | 86399   | 0       | a second inside the window                   |
      | 86400   | 0       | at exactly 24h the window is still open      |
      | 86401   | 1       | one second past the window                   |

  @bdd
  Scenario Outline: An unanswered order is rejected only once its window has strictly closed
    Given a network order "po-1" in state "NEW"
    And <elapsed> seconds pass
    When overdue orders are rejected
    And I request network order "po-1"
    Then the network order state is "<state>"

    Examples:
      | elapsed | state    | case                                    |
      | 86400   | NEW      | at exactly 24h the window is still open |
      | 86401   | REJECTED | one second past the window              |

  @bdd
  Scenario: The deadline-at-risk event names the deadline that was missed
    Given a network order "po-1" in state "NEW"
    And 25 hours pass
    When the acknowledgement deadline sweep runs
    Then the "AcknowledgementDeadlineAtRisk" event for "po-1" carries acknowledgeBy "2026-10-09T12:00:00Z"
    And the "AcknowledgementDeadlineAtRisk" event for "po-1" carries siteId "site-1"

  # ---- RejectOverdueOrders and the hold --------------------------------------

  @bdd
  Scenario Outline: An overdue order whose hold cannot be cancelled stays NEW and is retried
    Given a network order "po-1" is stuck NEW with its hold open
    And 25 hours pass
    And order-management answers the cancel with status <status>
    When overdue orders are rejected
    And I request network order "po-1"
    Then the network order state is "NEW"
    And the hold of network order "po-1" is open
    And the domain event "NetworkOrderRejected" was not published for "po-1"
    When order-management cancels held orders again
    And overdue orders are rejected
    And I request network order "po-1"
    Then the network order state is "REJECTED"
    And the hold of network order "po-1" is settled as "CANCELLED"
    And the NetworkOrderRejected event for "po-1" carries reason "ACKNOWLEDGEMENT_DEADLINE_MISSED"
    And order-management was asked to cancel 2 times in total

    Examples:
      | status | case                                                          |
      | 404    | here a 404 is a failure: only the orphaned-hold sweep reads it |
      | 409    | here a 409 is a failure, not "already released"               |
      | 422    | here a 422 is a failure, not "already released"               |
      | 503    | order-management is down                                      |

  @bdd
  Scenario Outline: Rejecting an overdue order skips the cancel of a hold the orphaned-hold sweep already settled
    Given a network order "po-1" is stuck NEW with its hold open
    And 26 hours pass
    And order-management answers the cancel with status <status>
    When the orphaned-hold sweep runs
    And overdue orders are rejected
    And I request network order "po-1"
    Then the network order state is "REJECTED"
    And the hold of network order "po-1" is settled as "<outcome>"
    And the NetworkOrderRejected event for "po-1" carries reason "ACKNOWLEDGEMENT_DEADLINE_MISSED"
    And order-management was asked to cancel 1 time in total
    And the domain event "OrphanedHoldCancelled" was published <events> times for "po-1"

    Examples:
      | status | outcome          | events | case                                        |
      | 404    | CANCELLED        | 1      | the hold was gone: reported, then rejected  |
      | 409    | ALREADY_RELEASED | 0      | released elsewhere: recorded, no event      |
      | 422    | ALREADY_RELEASED | 0      | 422 reads the same as 409                   |

  @bdd
  Scenario: An order with no hold is rejected overdue without asking order-management to cancel
    Given an untranslatable network order "po-1" was recorded but never answered
    And 25 hours pass
    When overdue orders are rejected
    And I request network order "po-1"
    Then the network order state is "REJECTED"
    And the NetworkOrderRejected event for "po-1" carries reason "ACKNOWLEDGEMENT_DEADLINE_MISSED"
    And order-management was asked to cancel 0 times in total
    And the network was told about "po-1" exactly 0 times

  @bdd
  Scenario: One overdue order that cannot be freed does not stop the others
    Given a network order "po-1" is stuck NEW with its hold open
    And a network order "po-2" is stuck NEW with its hold open
    And 25 hours pass
    And order-management answers the cancel of the hold of "po-1" with status 503
    When overdue orders are rejected
    And I request network order "po-2"
    Then the network order state is "REJECTED"
    When I request network order "po-1"
    Then the network order state is "NEW"
    When order-management cancels the hold of "po-1" again
    And overdue orders are rejected
    And I request network order "po-1"
    Then the network order state is "REJECTED"
    And the domain event "NetworkOrderRejected" was published 1 time for "po-1"
    And the domain event "NetworkOrderRejected" was published 1 time for "po-2"

  # ---- SweepOrphanedHolds ----------------------------------------------------

  @bdd
  Scenario Outline: The sweep acts only strictly after acknowledgeBy plus the grace
    Given <fixture>
    And <elapsed> seconds pass
    When the orphaned-hold sweep runs
    Then order-management was asked to cancel <cancels> times in total
    And the domain event "OrphanedHoldCancelled" was published <cancels> times for "po-1"

    Examples:
      | fixture                                                      | elapsed | cancels | case                                       |
      | a network order "po-1" is stuck NEW with its hold open       | 90000   | 0       | NEW at exactly acknowledgeBy + 1h          |
      | a network order "po-1" is stuck NEW with its hold open       | 90001   | 1       | NEW one second past it                     |
      | a network order "po-1" was rejected before its hold was settled | 90000   | 0       | REJECTED at exactly acknowledgeBy + 1h     |
      | a network order "po-1" was rejected before its hold was settled | 90001   | 1       | REJECTED one second past it                |

  @bdd
  Scenario: A rejected order's open hold is freed without touching the order's state
    Given a network order "po-1" was rejected before its hold was settled
    And 26 hours pass
    When the orphaned-hold sweep runs
    And I request network order "po-1"
    Then the network order state is "REJECTED"
    And the hold of network order "po-1" is settled as "CANCELLED"
    And the domain event "OrphanedHoldCancelled" was published 1 time for "po-1"
    And the domain event "NetworkOrderRejected" was not published for "po-1"

  @bdd
  Scenario Outline: An order we committed to is never swept however long it waits
    Given a network order "po-1" in state "<state>"
    And 720 hours pass
    When the orphaned-hold sweep runs and its tally is kept
    Then the sweep tally is 0 examined, 0 cancelled, 0 already released and 0 failed
    And order-management was asked to cancel 0 times in total
    And the domain event "OrphanedHoldCancelled" was not published for "po-1"

    Examples:
      | state        |
      | ACKNOWLEDGED |
      | CONFIRMED    |

  @bdd
  Scenario: An order with no recorded hold is never examined by the sweep
    Given an untranslatable network order "po-1" was recorded but never answered
    And 72 hours pass
    When the orphaned-hold sweep runs and its tally is kept
    Then the sweep tally is 0 examined, 0 cancelled, 0 already released and 0 failed
    And order-management was asked to cancel 0 times in total

  @bdd
  Scenario Outline: The orphaned-hold event says what order-management answered
    Given a network order "po-1" is stuck NEW with its hold open
    And 26 hours pass
    And <answer>
    When the orphaned-hold sweep runs
    Then the "OrphanedHoldCancelled" event for "po-1" carries answer "<reported>"
    And the "OrphanedHoldCancelled" event for "po-1" names the local order of "po-1"
    And the "OrphanedHoldCancelled" event for "po-1" carries acknowledgeBy "2026-10-09T12:00:00Z"
    And the "OrphanedHoldCancelled" event for "po-1" carries siteId "site-1"

    Examples:
      | answer                                           | reported  | case                       |
      | order-management cancels held orders again       | CANCELLED | a 2xx: the hold was cancelled |
      | order-management answers the cancel with status 404 | NOT_FOUND | a 404: the hold no longer existed |

  @bdd
  Scenario: A hold answered 422 is already released, recorded without an event and never retried
    Given a network order "po-1" is stuck NEW with its hold open
    And 26 hours pass
    And order-management answers the cancel with status 422
    When the orphaned-hold sweep runs
    And the orphaned-hold sweep runs
    Then the hold of network order "po-1" is settled as "ALREADY_RELEASED"
    And order-management was asked to cancel 1 time in total
    And the domain event "OrphanedHoldCancelled" was not published for "po-1"

  @bdd
  Scenario Outline: An unclassified answer leaves the hold open to be retried on the next pass
    Given a network order "po-1" is stuck NEW with its hold open
    And 26 hours pass
    And order-management answers the cancel with status <status>
    When the orphaned-hold sweep runs and its tally is kept
    Then the sweep tally is 1 examined, 0 cancelled, 0 already released and 1 failed
    And the hold of network order "po-1" is open
    And the domain event "OrphanedHoldCancelled" was not published for "po-1"
    When order-management cancels held orders again
    And the orphaned-hold sweep runs
    Then the hold of network order "po-1" is settled as "CANCELLED"

    Examples:
      | status | case                      |
      | 500    | order-management failed   |
      | 503    | order-management is down  |

  @bdd
  Scenario: One hold that cannot be freed never aborts the pass, and every outcome is counted
    Given a network order "po-1" is stuck NEW with its hold open
    And a network order "po-2" is stuck NEW with its hold open
    And a network order "po-3" is stuck NEW with its hold open
    And a network order "po-4" is stuck NEW with its hold open
    And 26 hours pass
    And a network order "po-5" is stuck NEW with its hold open
    And order-management answers the cancel of the hold of "po-1" with status 503
    And order-management answers the cancel of the hold of "po-2" with status 404
    And order-management answers the cancel of the hold of "po-3" with status 409
    When the orphaned-hold sweep runs and its tally is kept
    Then the sweep tally is 5 examined, 2 cancelled, 1 already released and 1 failed
    And the sweep metric counted 2 "cancelled"
    And the sweep metric counted 1 "already_released"
    And the sweep metric counted 1 "failed"
    And the hold of network order "po-1" is open
    And the hold of network order "po-2" is settled as "CANCELLED"
    And the hold of network order "po-3" is settled as "ALREADY_RELEASED"
    And the hold of network order "po-4" is settled as "CANCELLED"
    And the hold of network order "po-5" is open

  # ---- what the read surface shows -------------------------------------------

  @bdd
  Scenario: An order stuck NEW with its hold open does not expose the hold on the read surface
    Given a network order "po-1" is stuck NEW with its hold open
    When I request network order "po-1"
    Then the network order state is "NEW"
    And the network order has no local order
    When I list the unanswered network orders
    Then the unanswered orders, soonest deadline first, are "po-1"

  @bdd
  Scenario: A rejected order never exposes its hold even though one was recorded
    Given a network order "po-1" in state "SUBMITTED"
    And the network will refuse the submission for "po-1"
    When the acknowledgement reconciliation runs
    And I request network order "po-1"
    Then the network order state is "REJECTED"
    And the hold of network order "po-1" is settled as "CANCELLED"
    And the network order has no local order

  @bdd
  Scenario: A confirmed order keeps showing the held order it shipped
    Given a network order "po-1" in state "CONFIRMED"
    When I request network order "po-1"
    Then the network order state is "CONFIRMED"
    And the network order has a local order
