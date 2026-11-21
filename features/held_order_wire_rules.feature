# Derived from: docs/adr/0035-site-map-for-held-orders.md (Decision 3),
# docs/adr/0029-fulfillment-site-on-the-held-order.md (the site-code rule the
# map sits in front of), docs/adr/0037-retry-the-refusals-the-callees-release.md
# (Decision 4/5, the classes read for the two keyed routes) and the planner's
# own invariants (releaseOnAllocation/allowPartialShipment, the deterministic
# Idempotency-Keys, the SKU-sorted lines that keep a retried raise
# byte-identical). The suite drives the REAL planner adapter over an
# in-process double of order-management's REST API, so the bytes on the wire,
# not the port call, are what the scenarios assert.
Feature: The held order on the wire
  What order-management receives for one network order is pinned byte by byte:
  the site translated through the map when it has an entry and only sent at all
  when it is a site code, the hold and ship-complete flags, the network's
  deadline handed over unchanged, lines sorted by SKU so a retry under the same
  Idempotency-Key replays, and the refusal classes of the keyed routes.

  Background:
    Given order-management answers the raise with a promise

  @bdd
  Scenario: A mapped network site is raised as its warehouse site
    Given the site map translates "site-1" to "AUS3"
    When a held order is raised for "PO-2001" at site "site-1" shipping 3 of "sku-a"
    Then the raise carried fulfillmentSiteId "AUS3"
    And the raise carried the Idempotency-Key "netful-raise-PO-2001"

  @bdd
  Scenario: An unmapped site that is a site code is sent as itself
    When a held order is raised for "PO-2002" at site "WH1" shipping 1 of "sku-a"
    Then the raise carried fulfillmentSiteId "WH1"

  @bdd
  Scenario: An unmapped network site that is not a site code is omitted
    When a held order is raised for "PO-2003" at site "site-1" shipping 1 of "sku-a"
    Then the raise omitted fulfillmentSiteId

  @bdd
  Scenario: A map entry whose warehouse site is not a site code sends nothing
    Given the site map translates "site-9" to "aus-3"
    When a held order is raised for "PO-2004" at site "site-9" shipping 1 of "sku-a"
    Then the raise omitted fulfillmentSiteId

  @bdd
  Scenario: The raise asks for a held, ship-complete order with the network's deadline
    When a held order is raised for "PO-2005" at site "WH1" shipping 2 of "sku-a" and 5 of "sku-b"
    Then the raise was for a held, ship-complete order
    And the raise carried the network's deadline unchanged
    And the verdict was feasible

  @bdd
  Scenario: No promise is the verdict not feasible, never an error
    Given order-management answers the raise with no promise
    When a held order is raised for "PO-2006" at site "WH1" shipping 1 of "sku-a"
    Then the verdict was infeasible, keeping the local order "ord-0217"

  @bdd
  Scenario: Lines go out sorted by SKU, so a retry is byte-identical under the same key
    When a held order is raised for "PO-2007" at site "WH1" shipping 3 of "sku-c" and 1 of "sku-a" and 4 of "sku-d" and 2 of "sku-b"
    And the raise is retried
    Then the raise carried the lines in SKU order "sku-a", "sku-b", "sku-c", "sku-d"
    And the retried raise was byte-identical under the same key

  @bdd
  Scenario: The release carries its own deterministic key
    Given a held order is raised for "PO-2008" at site "WH1" shipping 1 of "sku-a"
    When the held order "ord-0217" is released
    Then the release called "/orders/ord-0217/releases"
    And the release was keyed "netful-release-ord-0217"

  @bdd
  Scenario: A refusal order-management releases may pass on a retry under the same key
    Given order-management answers the next raise with 422 and the problem "unknown-process-path"
    When a held order is raised for "PO-2009" at site "WH1" shipping 1 of "sku-a"
    Then the raise was refused in a way a retry can fix
    When the raise is retried
    Then the verdict was feasible
    And both raises carried the Idempotency-Key "netful-raise-PO-2009"

  @bdd
  Scenario: A final refusal of the raise is answered the same on every retry
    Given order-management answers the next raise with 422 and the problem "empty-sku"
    When a held order is raised for "PO-2010" at site "WH1" shipping 1 of "sku-a"
    Then the raise was refused with a final verdict

  @bdd
  Scenario: A refusal without a problem document is terminal
    Given order-management answers the next raise with 409 and no problem document
    When a held order is raised for "PO-2011" at site "WH1" shipping 1 of "sku-a"
    Then the raise was refused with a final verdict

  @bdd
  Scenario: A 5xx is a plain error, never a refusal
    Given order-management answers the next raise with 503 and no problem document
    When a held order is raised for "PO-2012" at site "WH1" shipping 1 of "sku-a"
    Then the raise failed as a plain error, not a refusal

  @bdd
  Scenario: An empty network ref refuses the raise before any request is made
    When a held order is raised for "" at site "WH1" shipping 1 of "sku-a"
    Then the raise failed as a plain error, not a refusal
    And no request reached order-management
