# Derived from the context documentation (warehouse-docs network-fulfillment:
# use-cases.md RecomputeCapabilityOffers, aggregate-design-canvas.md
# CapabilityOffer invariants and corrective policies, troubleshooting.md
# "Capability offers", business-context.md) and docs/adr/0017, 0031, 0033.
# capability_offers.feature pins the min() formula and the cycle-time fit; these
# scenarios pin the rest of what the docs say the recompute pass does: the
# walk to the next feasible cutoff (its week-long bound included), a full
# cutoff that misses only its own truck, capacity matched on the exact cutoff
# instant, and a negative figure never advertised.
Feature: Capability offers, documented rules
  The offer advertises against the next cutoff the site can still make and has
  room for, never against capacity observed for another instant, and never
  advertises less than zero or more than physically exists.

  Background:
    Given the product dictionary maps "ASIN-1" to "sku-1"

  # ---- the seven-day horizon ---------------------------------------------------

  @bdd
  Scenario Outline: The walk to the next feasible cutoff stops a week from now
    Given inventory-storage reports 40 usable units of "sku-1"
    And process-path-management reports the next cutoff in 44 minutes
    And process-path-management reports a later cutoff in <later> minutes
    And path "p1" is eligible with a cycle time of 120 minutes
    And path "p1" has 25 units of remaining capacity
    When the capability offers are recomputed
    And I list the capability offers
    Then the capability offer for "sku-1" at "site-1" advertises <advertised> with basis "THROUGHPUT_CONSTRAINED"

    Examples:
      | later | advertised | case                                                      |
      | 10080 | 25         | a cutoff exactly seven days ahead is still inside the bound |
      | 10081 | 0          | one minute past the week: a proven zero                   |

  # ---- ADR 0033: a full cutoff misses only its own truck ------------------------

  @bdd
  Scenario: A cutoff whose capacity is used up is skipped for the next one with room
    Given inventory-storage reports 40 usable units of "sku-1"
    And process-path-management reports the next cutoff in 60 minutes
    And process-path-management reports a later cutoff in 1440 minutes
    And path "p1" is eligible with a cycle time of 30 minutes
    And path "p1" has 0 units of remaining capacity for the cutoff in 60 minutes
    And path "p1" has 25 units of remaining capacity for the cutoff in 1440 minutes
    When the capability offers are recomputed
    And I list the capability offers
    Then the capability offer for "sku-1" at "site-1" advertises 25 with basis "THROUGHPUT_CONSTRAINED"

  @bdd
  Scenario: When every cutoff of the week is full the offer is a proven zero
    Given inventory-storage reports 40 usable units of "sku-1"
    And process-path-management reports the next cutoff in 60 minutes
    And process-path-management reports a later cutoff in 1440 minutes
    And path "p1" is eligible with a cycle time of 30 minutes
    And path "p1" has 0 units of remaining capacity for the cutoff in 60 minutes
    And path "p1" has 0 units of remaining capacity for the cutoff in 1440 minutes
    When the capability offers are recomputed
    And I list the capability offers
    Then the capability offer for "sku-1" at "site-1" advertises 0 with basis "THROUGHPUT_CONSTRAINED"

  @bdd
  Scenario: A cutoff with room is not skipped even when a later one has more
    Given inventory-storage reports 40 usable units of "sku-1"
    And process-path-management reports the next cutoff in 60 minutes
    And process-path-management reports a later cutoff in 1440 minutes
    And path "p1" is eligible with a cycle time of 30 minutes
    And path "p1" has 3 units of remaining capacity for the cutoff in 60 minutes
    And path "p1" has 90 units of remaining capacity for the cutoff in 1440 minutes
    When the capability offers are recomputed
    And I list the capability offers
    Then the capability offer for "sku-1" at "site-1" advertises 3 with basis "THROUGHPUT_CONSTRAINED"

  # ---- capacity is matched on the exact cutoff instant ---------------------------

  @bdd
  Scenario: A capacity figure observed for another cutoff says nothing about this one
    Given inventory-storage reports 40 usable units of "sku-1"
    And process-path-management reports the next cutoff in 120 minutes
    And path "p1" is eligible with a cycle time of 30 minutes
    And path "p1" has 25 units of remaining capacity for the cutoff in 600 minutes
    When the capability offers are recomputed
    And I list the capability offers
    Then the capability offer for "sku-1" at "site-1" advertises 40 with basis "PHYSICAL"

  @bdd
  Scenario: A figure observed for exactly this cutoff is the one that counts
    Given inventory-storage reports 40 usable units of "sku-1"
    And process-path-management reports the next cutoff in 120 minutes
    And path "p1" is eligible with a cycle time of 30 minutes
    And path "p1" has 25 units of remaining capacity for the cutoff in 120 minutes
    When the capability offers are recomputed
    And I list the capability offers
    Then the capability offer for "sku-1" at "site-1" advertises 25 with basis "THROUGHPUT_CONSTRAINED"

  # ---- value rules -----------------------------------------------------------------

  @bdd
  Scenario: A path with a zero cycle time stays eligible, zero is no evidence
    Given inventory-storage reports 40 usable units of "sku-1"
    And process-path-management reports the next cutoff in 120 minutes
    And path "p1" is eligible with a cycle time of 0 minutes
    And path "p1" has 25 units of remaining capacity
    When the capability offers are recomputed
    And I list the capability offers
    Then the capability offer for "sku-1" at "site-1" advertises 25 with basis "THROUGHPUT_CONSTRAINED"

  @bdd
  Scenario: A path over-committed beyond its capacity never makes the offer negative
    Given inventory-storage reports 40 usable units of "sku-1"
    And process-path-management reports the next cutoff in 120 minutes
    And path "p1" is eligible with a cycle time of 30 minutes
    And path "p1" is over-committed by 15 units
    When the capability offers are recomputed
    And I list the capability offers
    Then the capability offer for "sku-1" at "site-1" advertises 0 with basis "THROUGHPUT_CONSTRAINED"
    And no capability offer advertises more than the physical stock

  @bdd
  Scenario: Two network products that map to one SKU are advertised once
    Given the product dictionary maps "ASIN-1B" to "sku-1"
    And inventory-storage reports 40 usable units of "sku-1"
    When the capability offers are recomputed
    And I list the capability offers
    Then the capability offers list has 1 offer
    And the capability offer for "sku-1" at "site-1" advertises 40 with basis "PHYSICAL"

  @bdd
  Scenario: Every SKU of the pass is advertised against the same cutoff
    Given the product dictionary maps "ASIN-2" to "sku-2"
    And inventory-storage reports 40 usable units of "sku-1"
    And inventory-storage reports 40 usable units of "sku-2"
    And process-path-management reports the next cutoff in 60 minutes
    And process-path-management reports a later cutoff in 1440 minutes
    And path "p1" is eligible with a cycle time of 30 minutes
    And path "p1" has 0 units of remaining capacity for the cutoff in 60 minutes
    And path "p1" has 18 units of remaining capacity for the cutoff in 1440 minutes
    When the capability offers are recomputed
    And I list the capability offers
    Then the capability offer for "sku-1" at "site-1" advertises 18 with basis "THROUGHPUT_CONSTRAINED"
    And the capability offer for "sku-2" at "site-1" advertises 18 with basis "THROUGHPUT_CONSTRAINED"
