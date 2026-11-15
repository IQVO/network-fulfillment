# Derived from the context documentation (warehouse-docs network-fulfillment:
# use-cases.md FeedInventory, aggregate-design-canvas.md CapabilityOffer
# "Created Events", business-context.md "The idea worth building") and
# docs/adr/0027 part 2. The feed is built from local data only: the persisted
# CapabilityOffers and the product dictionary. The network is a recording
# double: the stub gateway sends nothing and keeps no feed.
Feature: Inventory feed, documented rules
  Each site is told what it claims it can ship: the offers of its newest pass
  for the SKUs it lists, a complete feed only when every listed SKU has a fresh
  offer, split in partial parts above 5000 lines, never guessed.

  Background:
    Given the product dictionary maps "ASIN-1" to "sku-1"
    And the product dictionary maps "ASIN-2" to "sku-2"

  @bdd
  Scenario: A site whose listed SKUs all have a fresh offer is fed one complete feed, sorted by SKU
    Given the site "site-1" lists the SKUs "sku-2", "sku-1" and "sku-3"
    And an offer for "sku-3" at "site-1" advertises 7 on basis "THROUGHPUT_CONSTRAINED" computed 0 minutes ago
    And an offer for "sku-1" at "site-1" advertises 40 on basis "PHYSICAL" computed 0 minutes ago
    And an offer for "sku-2" at "site-1" advertises 12 on basis "PHYSICAL" computed 0 minutes ago
    When the inventory feed runs
    Then 1 inventory feed was submitted for site "site-1"
    And part 1 of 1 of the inventory feed for "site-1" is complete with 3 lines
    And the inventory feed for "site-1" carries the lines:
      | sku   | quantity | basis                  |
      | sku-1 | 40       | PHYSICAL               |
      | sku-2 | 12       | PHYSICAL               |
      | sku-3 | 7        | THROUGHPUT_CONSTRAINED |
    And every part of the inventory feed for "site-1" is as of "2026-10-08T12:00:00Z"
    And the inventory feed pass ended 1 time with "sent"

  @bdd
  Scenario: An offer of an earlier pass is not fed and keeps the feed from being complete
    Given the site "site-1" lists the SKUs "sku-1" and "sku-2"
    And an offer for "sku-1" at "site-1" advertises 40 on basis "PHYSICAL" computed 0 minutes ago
    And an offer for "sku-2" at "site-1" advertises 12 on basis "PHYSICAL" computed 10 minutes ago
    When the inventory feed runs
    Then part 1 of 1 of the inventory feed for "site-1" is partial with 1 lines
    And the inventory feed for "site-1" carries the lines:
      | sku   | quantity | basis    |
      | sku-1 | 40       | PHYSICAL |

  @bdd
  Scenario: A SKU the site has no listing for is not fed, and its absence is noted once per process
    Given the site "site-1" lists the SKU "sku-1"
    And an offer for "sku-1" at "site-1" advertises 40 on basis "PHYSICAL" computed 0 minutes ago
    And an offer for "sku-2" at "site-1" advertises 12 on basis "PHYSICAL" computed 0 minutes ago
    When the inventory feed runs
    And the inventory feed runs
    Then 2 inventory feeds were submitted for site "site-1"
    And part 1 of 1 of the inventory feed for "site-1" is complete with 1 lines
    And the inventory feed for "site-1" carries the lines:
      | sku   | quantity | basis    |
      | sku-1 | 40       | PHYSICAL |
    And the no-listing note for "sku-2" was logged 1 time

  @bdd
  Scenario: A SKU whose stock lookup failed is left out of the feed, which is then not complete
    Given the site "site-1" lists the SKUs "sku-1" and "sku-2"
    And inventory-storage reports 40 usable units of "sku-1"
    And inventory-storage reports 12 usable units of "sku-2"
    When the capability offers are recomputed
    And 1 minute passes
    And inventory-storage is unreachable for "sku-2"
    And the capability offers are recomputed
    And the inventory feed runs
    Then part 1 of 1 of the inventory feed for "site-1" is partial with 1 lines
    And the inventory feed for "site-1" carries the lines:
      | sku   | quantity | basis    |
      | sku-1 | 40       | PHYSICAL |
    And every part of the inventory feed for "site-1" is as of "2026-10-08T12:01:00Z"

  @bdd
  Scenario Outline: A site above 5000 lines is split into partial parts of the same moment
    Given the site "site-1" lists <skus> SKUs each with a fresh offer
    When the inventory feed runs
    Then <parts> inventory feeds were submitted for site "site-1"
    And part 1 of <parts> of the inventory feed for "site-1" is <kind> with <first> lines
    And part <parts> of <parts> of the inventory feed for "site-1" is <kind> with <last> lines
    And every part of the inventory feed for "site-1" is as of "2026-10-08T12:00:00Z"

    Examples:
      | skus  | parts | kind     | first | last | case                                    |
      | 5000  | 1     | complete | 5000  | 5000 | exactly the most one feed may carry     |
      | 5001  | 2     | partial  | 5000  | 1    | one line over: split, and so incomplete |
      | 10000 | 2     | partial  | 5000  | 5000 | two full parts                          |
      | 10001 | 3     | partial  | 5000  | 1    | a third part for the last line          |

  @bdd
  Scenario: A site with no offer yet is skipped, not fed an empty feed
    Given the site "site-1" lists the SKU "sku-1"
    When the inventory feed runs
    Then 0 inventory feeds were submitted for site "site-1"
    And the inventory feed pass ended 1 time with "skipped_no_offers"

  @bdd
  Scenario: A site with offers but no listing for any of them is skipped
    Given an offer for "sku-1" at "site-1" advertises 40 on basis "PHYSICAL" computed 0 minutes ago
    When the inventory feed runs
    Then 0 inventory feeds were submitted for site "site-1"
    And the inventory feed pass ended 1 time with "skipped_no_listings"

  @bdd
  Scenario: A site with no node on the network yet is skipped, not failed
    Given the site "site-1" lists the SKU "sku-1"
    And an offer for "sku-1" at "site-1" advertises 40 on basis "PHYSICAL" computed 0 minutes ago
    And the network has no node for the inventory feed of site "site-1"
    When the inventory feed runs
    Then the inventory feed pass ended 1 time with "skipped_no_node"
    And the inventory feed pass ended 0 times with "failed"

  @bdd
  Scenario: One site's failure never stops the others
    Given the inventory feed serves site "site-1"
    And the inventory feed serves site "site-2"
    And the site "site-1" lists the SKU "sku-1"
    And the site "site-2" lists the SKU "sku-2"
    And an offer for "sku-1" at "site-1" advertises 40 on basis "PHYSICAL" computed 0 minutes ago
    And an offer for "sku-2" at "site-2" advertises 12 on basis "PHYSICAL" computed 0 minutes ago
    And the network fails for the inventory feed of site "site-1"
    When the inventory feed runs
    Then the inventory feed pass ended 1 time with "failed"
    And the inventory feed pass ended 1 time with "sent"
    And 1 inventory feed was submitted for site "site-2"

  @bdd
  Scenario: Offers that cannot be read fail every site for the pass
    Given the inventory feed serves site "site-1"
    And the inventory feed serves site "site-2"
    And the site "site-1" lists the SKU "sku-1"
    And an offer for "sku-1" at "site-1" advertises 40 on basis "PHYSICAL" computed 0 minutes ago
    And the capability offers cannot be read
    When the inventory feed runs
    Then the inventory feed pass ended 2 times with "failed"
    And 0 inventory feeds were submitted for site "site-1"
    And 0 inventory feeds were submitted for site "site-2"

  @bdd
  Scenario: The fingerprint says what the feed says, not when it was said
    Given the site "site-1" lists the SKUs "sku-1" and "sku-2"
    And an offer for "sku-1" at "site-1" advertises 40 on basis "PHYSICAL" computed 0 minutes ago
    And an offer for "sku-2" at "site-1" advertises 12 on basis "PHYSICAL" computed 0 minutes ago
    When the inventory feed runs
    And 30 minutes pass
    And an offer for "sku-1" at "site-1" advertises 40 on basis "PHYSICAL" computed 0 minutes ago
    And an offer for "sku-2" at "site-1" advertises 12 on basis "PHYSICAL" computed 0 minutes ago
    And the inventory feed runs
    Then the first and the last inventory feed for "site-1" have the same fingerprint

  @bdd
  Scenario: A changed quantity is a different feed
    Given the site "site-1" lists the SKU "sku-1"
    And an offer for "sku-1" at "site-1" advertises 40 on basis "PHYSICAL" computed 0 minutes ago
    When the inventory feed runs
    And 30 minutes pass
    And an offer for "sku-1" at "site-1" advertises 38 on basis "PHYSICAL" computed 0 minutes ago
    And the inventory feed runs
    Then the first and the last inventory feed for "site-1" have the different fingerprint

  @bdd
  Scenario: Whether the feed is complete is part of what it says
    Given the site "site-1" lists the SKU "sku-1"
    And an offer for "sku-1" at "site-1" advertises 40 on basis "PHYSICAL" computed 0 minutes ago
    When the inventory feed runs
    And 30 minutes pass
    And the site "site-1" lists the SKU "sku-2"
    And an offer for "sku-1" at "site-1" advertises 40 on basis "PHYSICAL" computed 0 minutes ago
    And the inventory feed runs
    Then part 1 of 1 of the inventory feed for "site-1" is partial with 1 lines
    And the inventory feed for "site-1" carries the lines:
      | sku   | quantity | basis    |
      | sku-1 | 40       | PHYSICAL |
    And the first and the last inventory feed for "site-1" have the different fingerprint
