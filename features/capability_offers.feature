# Derived from: apis/openapi.yaml (listCapabilityOffers, schema CapabilityOffer:
# advertisedQuantity = min(physicalAvailable, throughputFeasibleBefore(nextCutoff)),
# basis PHYSICAL | THROUGHPUT_CONSTRAINED; route registered only when
# CAPABILITY_OFFER_ENABLED) and .claude/rules/domain-model.md (CapabilityOffer,
# RecomputeCapabilityOffers); docs/adr/0001 §8, 0017.
Feature: Capability offers
  What this context would advertise to the network it can ship for each
  (SKU, site): physical availability capped by what the eligible paths can
  actually move before the next cutoff. We never advertise more than
  physically exists.

  Background:
    Given the product dictionary maps "ASIN-1" to "sku-1"
    And the product dictionary maps "ASIN-2" to "sku-2"

  @bdd
  Scenario: With capability offers not enabled the route does not exist
    Given capability offers are not enabled
    When I GET "/capability-offers"
    Then the response status is 404

  @bdd
  Scenario: Enabled but not yet computed, the list is an empty array
    When I list the capability offers
    Then the response status is 200
    And the capability offers list has 0 offers
    And the response body contains "capabilityOffers":[]

  @bdd
  Scenario Outline: The advertised quantity is the lesser of physical stock and path capacity
    Given inventory-storage reports <physical> usable units of "sku-1"
    And process-path-management reports the next cutoff in 120 minutes
    And path "p1" is eligible with a cycle time of 30 minutes
    And path "p1" has <capacity> units of remaining capacity
    When the capability offers are recomputed
    And I list the capability offers
    Then the capability offer for "sku-1" at "site-1" advertises <advertised> with basis "<basis>"
    And no capability offer advertises more than the physical stock

    Examples:
      | physical | capacity | advertised | basis                  | case                                   |
      | 40       | 25       | 25         | THROUGHPUT_CONSTRAINED | the path is the binding constraint     |
      | 40       | 40       | 40         | PHYSICAL               | capacity equals stock                  |
      | 40       | 100      | 40         | PHYSICAL               | stock is the binding constraint        |
      | 40       | 0        | 0          | THROUGHPUT_CONSTRAINED | the path is full                       |
      | 0        | 10       | 0          | PHYSICAL               | nothing on hand                        |

  @bdd
  Scenario: Remaining capacity is summed across every eligible path
    Given inventory-storage reports 40 usable units of "sku-1"
    And process-path-management reports the next cutoff in 120 minutes
    And path "p1" is eligible with a cycle time of 30 minutes
    And path "p2" is eligible with a cycle time of 30 minutes
    And path "p1" has 10 units of remaining capacity
    And path "p2" has 15 units of remaining capacity
    When the capability offers are recomputed
    And I list the capability offers
    Then the capability offer for "sku-1" at "site-1" advertises 25 with basis "THROUGHPUT_CONSTRAINED"

  @bdd
  Scenario: With no cutoff schedule known, capacity is not a proven constraint and physical stock is advertised
    Given inventory-storage reports 40 usable units of "sku-1"
    When the capability offers are recomputed
    And I list the capability offers
    Then the capability offer for "sku-1" at "site-1" advertises 40 with basis "PHYSICAL"

  @bdd
  Scenario: A path whose capacity was never observed does not throttle the offer
    Given inventory-storage reports 40 usable units of "sku-1"
    And process-path-management reports the next cutoff in 120 minutes
    And path "p1" is eligible with a cycle time of 30 minutes
    When the capability offers are recomputed
    And I list the capability offers
    Then the capability offer for "sku-1" at "site-1" advertises 40 with basis "PHYSICAL"

  @bdd
  Scenario Outline: A path counts only if its cycle time fits the time left to the cutoff
    Given inventory-storage reports 40 usable units of "sku-1"
    And process-path-management reports the next cutoff in 120 minutes
    And path "p1" is eligible with a cycle time of <cycle> minutes
    And path "p1" has 25 units of remaining capacity
    When the capability offers are recomputed
    And I list the capability offers
    Then the capability offer for "sku-1" at "site-1" advertises <advertised> with basis "THROUGHPUT_CONSTRAINED"

    Examples:
      | cycle | advertised | case                                             |
      | 60    | 25         | fits comfortably                                 |
      | 120   | 25         | equal to the time left is still eligible         |
      | 121   | 0          | too slow for the cutoff: a proven zero, not a fallback |

  @bdd
  Scenario: A path with an unknown cycle time stays eligible
    Given inventory-storage reports 40 usable units of "sku-1"
    And process-path-management reports the next cutoff in 120 minutes
    And path "p1" is eligible with an unknown cycle time
    And path "p1" has 25 units of remaining capacity
    When the capability offers are recomputed
    And I list the capability offers
    Then the capability offer for "sku-1" at "site-1" advertises 25 with basis "THROUGHPUT_CONSTRAINED"

  @bdd
  Scenario: A path too slow for the cutoff is excluded while a fast one still counts
    Given inventory-storage reports 40 usable units of "sku-1"
    And process-path-management reports the next cutoff in 120 minutes
    And path "slow" is eligible with a cycle time of 180 minutes
    And path "fast" is eligible with a cycle time of 30 minutes
    And path "slow" has 100 units of remaining capacity
    And path "fast" has 10 units of remaining capacity
    When the capability offers are recomputed
    And I list the capability offers
    Then the capability offer for "sku-1" at "site-1" advertises 10 with basis "THROUGHPUT_CONSTRAINED"

  @bdd
  Scenario: One offer per known SKU, stamped with the time it was computed
    Given inventory-storage reports 40 usable units of "sku-1"
    And inventory-storage reports 7 usable units of "sku-2"
    When the capability offers are recomputed
    And I list the capability offers
    Then the capability offers list has 2 offers
    And the capability offer for "sku-2" at "site-1" advertises 7 with basis "PHYSICAL"
    And every capability offer was computed at the current time

  @bdd
  Scenario: Recomputing replaces the previous offer instead of accumulating history
    Given inventory-storage reports 40 usable units of "sku-1"
    When the capability offers are recomputed
    And inventory-storage reports 12 usable units of "sku-1"
    And 10 minutes pass
    And the capability offers are recomputed
    And I list the capability offers
    Then the capability offers list has 2 offers
    And the capability offer for "sku-1" at "site-1" advertises 12 with basis "PHYSICAL"
    And every capability offer was computed at the current time

  @bdd
  Scenario: A SKU whose stock cannot be looked up is skipped and the others are still offered
    Given inventory-storage reports 7 usable units of "sku-2"
    And inventory-storage is unreachable for "sku-1"
    When the capability offers are recomputed
    And I list the capability offers
    Then the capability offers list has 1 offer
    And the capability offer for "sku-2" at "site-1" advertises 7 with basis "PHYSICAL"
