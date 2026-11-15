# Derived from: apis/openapi.yaml (getNetworkOrderByPurchaseOrder, schemas
# NetworkOrder / Problem) and docs/adr/0036-purchase-order-keyed-read.md; the
# localOrderId rules are those of getNetworkOrder (ADR 0024: absent until
# acknowledged).
#
# The network ref is minted by this context and nobody outside can know it; the
# purchase-order number is known outside. A caller that holds only the number
# reads the order, and the order-management order raised for it, through this
# keyed read. It is not a listing and not a filter.
Feature: Reading a network order by its purchase-order number
  A caller that knows only the network's purchase-order number reads the
  same network order as `GET /network-orders/{networkRef}` returns, with the
  order-management order raised for it once the order was acknowledged.

  Background:
    Given the product dictionary maps "ASIN-1" to "sku-1"

  @bdd
  Scenario: An acknowledged purchase order answers with the order-management order raised for it
    Given a network order "NR-acked" in state "ACKNOWLEDGED"
    And purchase order "PO-7f3c" was filed under network order "NR-acked"
    When I request purchase order "PO-7f3c"
    Then the response status is 200
    And the network order is "NR-acked" for site "site-1"
    And the network order state is "ACKNOWLEDGED"
    And the network order has a local order
    And the network order has line "1" for product "ASIN-1" translated to SKU "sku-1" with quantity 2
    And the network order carries only the documented properties
    And the same order is read by network ref "NR-acked"

  @bdd
  Scenario: A purchase order still NEW has no local order
    Given a network order "NR-new" in state "NEW"
    And purchase order "PO-new" was filed under network order "NR-new"
    When I request purchase order "PO-new"
    Then the response status is 200
    And the network order state is "NEW"
    And the network order has no local order
    And the same order is read by network ref "NR-new"

  @bdd
  Scenario: A rejected purchase order has no local order
    Given a network order "NR-rejected" in state "REJECTED"
    And purchase order "PO-rejected" was filed under network order "NR-rejected"
    When I request purchase order "PO-rejected"
    Then the response status is 200
    And the network order state is "REJECTED"
    And the network order has no local order

  @bdd
  Scenario: The purchase order is found by its number and not by the network ref
    Given a network order "NR-acked" in state "ACKNOWLEDGED"
    And purchase order "PO-7f3c" was filed under network order "NR-acked"
    When I request purchase order "NR-acked"
    Then the response status is 404
    And the problem detail type is "purchase-order-not-found"

  @bdd
  Scenario: A purchase order nothing was received for is a 404 problem document
    When I request purchase order "PO-404"
    Then the response status is 404
    And the problem detail type is "purchase-order-not-found"

  @bdd
  Scenario: A purchase-order number whose order is not stored yet is the same 404
    Given purchase order "PO-early" was filed under network order "NR-not-stored-yet"
    When I request purchase order "PO-early"
    Then the response status is 404
    And the problem detail type is "purchase-order-not-found"

  @bdd
  Scenario Outline: A malformed purchase-order number is a 400 problem document
    When I GET "<path>"
    Then the response status is 400
    And the problem detail type is "invalid-purchase-order-number"

    Examples:
      | path                              | case                         |
      | /purchase-orders/PO%207f3c        | a space                      |
      | /purchase-orders/PO%2F7f3c        | an escaped slash             |
      | /purchase-orders/PO:7f3c          | a character that is not safe |

  @bdd
  Scenario: A number of 129 characters is malformed
    When I GET "/purchase-orders/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    Then the response status is 400
    And the problem detail type is "invalid-purchase-order-number"

  @bdd
  Scenario Outline: It is a keyed read: no query parameter is accepted
    Given a network order "NR-acked" in state "ACKNOWLEDGED"
    And purchase order "PO-7f3c" was filed under network order "NR-acked"
    When I GET "<path>"
    Then the response status is 400
    And the problem detail type is "unknown-query-parameter"

    Examples:
      | path                              | case                 |
      | /purchase-orders/PO-7f3c?limit=10  | a paging parameter   |
      | /purchase-orders/PO-7f3c?cursor=a  | a paging parameter   |
      | /purchase-orders/PO-7f3c?state=NEW | a filter             |

  @bdd
  Scenario: There is no listing of purchase orders
    Given a network order "NR-acked" in state "ACKNOWLEDGED"
    And purchase order "PO-7f3c" was filed under network order "NR-acked"
    When I GET "/purchase-orders"
    Then the response status is 404

  @bdd
  Scenario: The listing of network orders is unchanged and still takes no filter
    Given a network order "NR-new" in state "NEW"
    And purchase order "PO-new" was filed under network order "NR-new"
    When I GET "/network-orders?poNumber=PO-new"
    Then the response status is 400
    And the problem detail type is "unknown-query-parameter"
