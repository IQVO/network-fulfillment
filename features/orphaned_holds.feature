# Derived from: docs/adr/0024-orphaned-order-management-hold-sweep.md (gap
# OM-0020, order-management ADR 0020 "Accepted deferral"). A demand that cannot
# be met makes order-management hold real inventory; if NF never frees the hold
# (its cancel kept failing) the sweep does, once the window and the grace have
# passed, classifying order-management's answer. The suite only drives the
# in-process order-management double.
Feature: Orphaned order-management holds
  A hold order-management made for demand NF never decided is cancelled by
  the orphaned-hold sweep after acknowledgeBy plus the grace (one hour here),
  reported once, and never retried once it is settled. An order NF is still
  deciding, or has committed to, is never touched.

  Background:
    Given the product dictionary maps "ASIN-1" to "sku-1"

  @bdd
  Scenario: A hold left by a failed cancel is cancelled once the window and the grace have passed
    Given order-management cannot meet the required ship-by
    And order-management fails to cancel held orders
    And the network has demand "po-1" for 2 units of "ASIN-1"
    When the poller runs a pass
    And 26 hours pass
    And order-management cancels held orders again
    And the orphaned-hold sweep runs
    And I request network order "po-1"
    Then the network order state is "NEW"
    And the hold of network order "po-1" is settled as "CANCELLED"
    And the domain event "OrphanedHoldCancelled" was published 1 time for "po-1"
    And every domain event was published on both the events and the analytics stream

  @bdd
  Scenario: The sweep leaves a hold alone inside the window and inside the grace
    Given order-management cannot meet the required ship-by
    And order-management fails to cancel held orders
    And the network has demand "po-1" for 2 units of "ASIN-1"
    When the poller runs a pass
    And 23 hours pass
    And the orphaned-hold sweep runs
    And 2 hours pass
    And the orphaned-hold sweep runs
    Then the hold of network order "po-1" is open
    And order-management was asked to cancel 1 time in total
    And the domain event "OrphanedHoldCancelled" was not published for "po-1"

  @bdd
  Scenario: A hold order-management says does not exist is done
    Given order-management cannot meet the required ship-by
    And order-management fails to cancel held orders
    And the network has demand "po-1" for 2 units of "ASIN-1"
    When the poller runs a pass
    And 26 hours pass
    And order-management answers the cancel with status 404
    And the orphaned-hold sweep runs
    Then the hold of network order "po-1" is settled as "CANCELLED"
    And the domain event "OrphanedHoldCancelled" was published 1 time for "po-1"

  @bdd
  Scenario: A hold found already released is not an orphan and is never retried
    Given order-management cannot meet the required ship-by
    And order-management fails to cancel held orders
    And the network has demand "po-1" for 2 units of "ASIN-1"
    When the poller runs a pass
    And 26 hours pass
    And order-management answers the cancel with status 409
    And the orphaned-hold sweep runs
    And the orphaned-hold sweep runs
    Then the hold of network order "po-1" is settled as "ALREADY_RELEASED"
    And order-management was asked to cancel 2 times in total
    And the domain event "OrphanedHoldCancelled" was not published for "po-1"

  @bdd
  Scenario: A cancel that fails is retried on the next pass and never marked done
    Given order-management cannot meet the required ship-by
    And order-management fails to cancel held orders
    And the network has demand "po-1" for 2 units of "ASIN-1"
    When the poller runs a pass
    And 26 hours pass
    And the orphaned-hold sweep runs
    Then the hold of network order "po-1" is open
    And the domain event "OrphanedHoldCancelled" was not published for "po-1"
    When order-management cancels held orders again
    And the orphaned-hold sweep runs
    And the orphaned-hold sweep runs
    Then the hold of network order "po-1" is settled as "CANCELLED"
    And the domain event "OrphanedHoldCancelled" was published 1 time for "po-1"
    And order-management was asked to cancel 3 times in total

  @bdd
  Scenario: An order NF has committed to is never swept
    Given a network order "po-1" in state "SUBMITTED"
    And 72 hours pass
    When the orphaned-hold sweep runs
    Then the hold of network order "po-1" is open
    And order-management was asked to cancel 0 times in total

  @bdd
  Scenario: Rejecting an overdue order frees its recorded hold and leaves the sweep nothing to do
    Given order-management cannot meet the required ship-by
    And order-management fails to cancel held orders
    And the network has demand "po-1" for 2 units of "ASIN-1"
    When the poller runs a pass
    And 25 hours pass
    And order-management cancels held orders again
    And overdue orders are rejected
    And 2 hours pass
    And the orphaned-hold sweep runs
    And I request network order "po-1"
    Then the network order state is "REJECTED"
    And the hold of network order "po-1" is settled as "CANCELLED"
    And order-management had 1 held order cancelled
    And order-management was asked to cancel 2 times in total
    And the domain event "OrphanedHoldCancelled" was not published for "po-1"
