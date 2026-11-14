# Derived from the in-repo documentation (docs/docs/ddd/use-cases.md
# ConfirmNetworkOrderShipment, docs/docs/ddd/domain-events.md) and
# docs/adr/0014 with its 2026-11 addendum. The warehouse-docs copy of
# use-cases.md still says "driven by POST only": the repository's own page is the
# newer one and is authoritative (see the PR body). shipment_confirmation.feature
# pins the REST route; these scenarios pin the event path that is now primary:
# outbound-shipping's ShipmentConfirmed joins orders[].order_id on the held order
# of a NetworkOrder, and two triggers reach one use case so neither can
# double-submit.
Feature: Shipment confirmation by event, documented rules
  A shipment outbound-shipping reports confirms the network orders it carries,
  once, whichever trigger arrives first or arrives again; the labels are asked
  for before anything is committed; a load that mixes in orders of ours with
  orders of customers is not an error.

  Background:
    Given the product dictionary maps "ASIN-1" to "sku-1"

  @bdd
  Scenario: A ShipmentConfirmed confirms every network order the load carries, with its package count
    Given a network order "po-1" in state "ACKNOWLEDGED"
    And a network order "po-2" in state "ACKNOWLEDGED"
    When outbound-shipping confirms load "load-9" carrying:
      | order              | packages |
      | held order of po-1 | 2        |
      | held order of po-2 |          |
    Then the shipment consumer handled the message
    And the network accepted 1 shipment confirmation for "po-1"
    And the network accepted 1 shipment confirmation for "po-2"
    And the network was asked for 2 labels for "po-1"
    And the network was asked for 1 label for "po-2"
    And the domain event "NetworkOrderShipmentConfirmed" was published 1 time for "po-1"
    And the domain event "NetworkOrderShipmentConfirmed" was published 1 time for "po-2"
    And the "NetworkOrderShipmentConfirmed" event for "po-1" names the local order of "po-1"
    When I request network order "po-1"
    Then the network order state is "CONFIRMED"
    When I request network order "po-2"
    Then the network order state is "CONFIRMED"

  @bdd
  Scenario: The same event delivered again confirms nothing twice
    Given a network order "po-1" in state "ACKNOWLEDGED"
    When outbound-shipping confirms load "load-9" carrying:
      | order              | packages |
      | held order of po-1 |          |
    And that shipment event is delivered again
    Then the shipment consumer handled the message
    And the network accepted 1 shipment confirmation for "po-1"
    And the network was asked for labels 1 time for "po-1"
    And the domain event "NetworkOrderShipmentConfirmed" was published 1 time for "po-1"

  @bdd
  Scenario: The same load published again under a new event id confirms nothing twice
    Given a network order "po-1" in state "ACKNOWLEDGED"
    When outbound-shipping confirms load "load-9" carrying:
      | order              | packages |
      | held order of po-1 |          |
    And outbound-shipping confirms load "load-9" carrying:
      | order              | packages |
      | held order of po-1 |          |
    Then the shipment consumer handled the message
    And the network accepted 1 shipment confirmation for "po-1"
    And the domain event "NetworkOrderShipmentConfirmed" was published 1 time for "po-1"

  @bdd
  Scenario: An order of a customer on a mixed load is skipped and the network's orders are still confirmed
    Given a network order "po-1" in state "ACKNOWLEDGED"
    When outbound-shipping confirms load "load-9" carrying:
      | order                  | packages |
      | local order ord-cust-7 |          |
      | held order of po-1     |          |
    Then the shipment consumer handled the message
    And the network accepted 1 shipment confirmation for "po-1"
    When I request network order "po-1"
    Then the network order state is "CONFIRMED"

  @bdd
  Scenario: A load with no orders is legal and confirms nothing
    Given a network order "po-1" in state "ACKNOWLEDGED"
    When outbound-shipping confirms load "load-9" carrying:
      | order | packages |
    Then the shipment consumer handled the message
    And the network accepted 0 shipment confirmations for "po-1"

  @bdd
  Scenario: An event of another type on the topic is ignored
    Given a network order "po-1" in state "ACKNOWLEDGED"
    When an outbound-shipping event of another type reaches the shipment consumer
    Then the shipment consumer handled the message
    And the network accepted 0 shipment confirmations for "po-1"

  @bdd
  Scenario Outline: A package count of zero or none means one label, otherwise the count is asked for
    Given a network order "po-1" in state "ACKNOWLEDGED"
    When outbound-shipping confirms load "load-9" carrying:
      | order              | packages   |
      | held order of po-1 | <packages> |
    Then the network was asked for <labels> labels for "po-1"

    Examples:
      | packages | labels | case                                         |
      |          | 1      | the producer does not send the field yet     |
      | 0        | 1      | zero is the single-package placeholder       |
      | 1        | 1      | one package                                  |
      | 3        | 3      | the real count flows the day it ships        |

  # ---- the order of the steps and the two triggers --------------------------------

  # ADR 0014 addendum D7. A load can depart while the network has not settled the
  # acknowledgement: for a SUBMITTED order that is a race, not a broken contract, so
  # the consumer records the shipment on the order and commits the message (no dead
  # letter); ConfirmPendingShipments confirms it once the order is ACKNOWLEDGED.
  @bdd
  Scenario: A load carrying an order not yet acknowledged is deferred, and nothing is sent
    Given a network order "po-1" in state "SUBMITTED"
    When outbound-shipping confirms load "load-9" carrying:
      | order              | packages |
      | held order of po-1 | 3        |
    Then the shipment consumer handled the message
    And network order "po-1" is waiting for the shipment of load "load-9"
    And the network was asked for labels 0 times for "po-1"
    And the network was asked to confirm shipment of 0 times for "po-1"
    And the domain event "NetworkOrderShipmentConfirmed" was not published for "po-1"
    When I request network order "po-1"
    Then the network order state is "SUBMITTED"

  @bdd
  Scenario: A deferred shipment is confirmed once, after the acknowledgement, with its package count
    Given a network order "po-1" in state "SUBMITTED"
    When outbound-shipping confirms load "load-9" carrying:
      | order              | packages |
      | held order of po-1 | 3        |
    And pending shipment confirmations run
    Then the network was asked for labels 0 times for "po-1"
    When the acknowledgement reconciliation runs
    And pending shipment confirmations run
    And pending shipment confirmations run
    Then the network accepted 1 shipment confirmation for "po-1"
    And the network was asked for labels 1 time for "po-1"
    And the network was asked for 3 labels for "po-1"
    And the domain event "NetworkOrderShipmentConfirmed" was published 1 time for "po-1"
    And network order "po-1" is waiting for no shipment
    When I request network order "po-1"
    Then the network order state is "CONFIRMED"

  @bdd
  Scenario: The same early event delivered again changes nothing, and the order is confirmed once
    Given a network order "po-1" in state "SUBMITTED"
    When outbound-shipping confirms load "load-9" carrying:
      | order              | packages |
      | held order of po-1 |          |
    And that shipment event is delivered again
    Then the shipment consumer handled the message
    When the acknowledgement reconciliation runs
    And pending shipment confirmations run
    Then the network accepted 1 shipment confirmation for "po-1"
    And the domain event "NetworkOrderShipmentConfirmed" was published 1 time for "po-1"

  @bdd
  Scenario: A deferred shipment of an order the network then refuses is dropped, never confirmed
    Given a network order "po-1" in state "SUBMITTED"
    And the network will refuse the submission for "po-1"
    When outbound-shipping confirms load "load-9" carrying:
      | order              | packages |
      | held order of po-1 |          |
    And the acknowledgement reconciliation runs
    And pending shipment confirmations run
    Then network order "po-1" is waiting for no shipment
    And the network was asked to confirm shipment of 0 times for "po-1"
    And the domain event "NetworkOrderShipmentConfirmed" was not published for "po-1"
    When I request network order "po-1"
    Then the network order state is "REJECTED"

  # A shipped order the network never took is a genuine anomaly for an operator:
  # the confirm error stands, so the consumer retries and then dead-letters.
  @bdd
  Scenario: A load carrying an order the network refused is still refused
    Given a network order "po-1" in state "REJECTED"
    When outbound-shipping confirms load "load-9" carrying:
      | order              | packages |
      | held order of po-1 |          |
    Then the shipment consumer refused the message as "before acknowledgement"
    And network order "po-1" is waiting for no shipment
    And the network was asked for labels 0 times for "po-1"
    And the domain event "NetworkOrderShipmentConfirmed" was not published for "po-1"

  @bdd
  Scenario: A label that cannot be issued leaves the order ACKNOWLEDGED and the event retryable
    Given a network order "po-1" in state "ACKNOWLEDGED"
    And the network cannot issue labels
    When outbound-shipping confirms load "load-9" carrying:
      | order              | packages |
      | held order of po-1 |          |
    Then the shipment consumer refused the message as "request label"
    And the network was asked to confirm shipment of 0 times for "po-1"
    And the domain event "NetworkOrderShipmentConfirmed" was not published for "po-1"
    When I request network order "po-1"
    Then the network order state is "ACKNOWLEDGED"
    When the network issues labels again
    And that shipment event is delivered again
    Then the shipment consumer handled the message
    And the network accepted 1 shipment confirmation for "po-1"
    When I request network order "po-1"
    Then the network order state is "CONFIRMED"

  @bdd
  Scenario: The operator route fails on a label too, and the retry then answers 204
    Given a network order "po-1" in state "ACKNOWLEDGED"
    And the network cannot issue labels
    When I confirm shipment of network order "po-1"
    Then the response status is 500
    And the problem detail type is "internal-error"
    And the network was asked to confirm shipment of 0 times for "po-1"
    When I request network order "po-1"
    Then the network order state is "ACKNOWLEDGED"
    When the network issues labels again
    And I confirm shipment of network order "po-1"
    Then the response status is 204
    And the network accepted 1 shipment confirmation for "po-1"

  @bdd
  Scenario: The operator route after the event confirms nothing twice
    Given a network order "po-1" in state "ACKNOWLEDGED"
    When outbound-shipping confirms load "load-9" carrying:
      | order              | packages |
      | held order of po-1 |          |
    And I confirm shipment of network order "po-1"
    Then the response status is 204
    And the network accepted 1 shipment confirmation for "po-1"
    And the domain event "NetworkOrderShipmentConfirmed" was published 1 time for "po-1"

  @bdd
  Scenario: The event after the operator route confirms nothing twice
    Given a network order "po-1" in state "ACKNOWLEDGED"
    When I confirm shipment of network order "po-1"
    And outbound-shipping confirms load "load-9" carrying:
      | order              | packages |
      | held order of po-1 |          |
    Then the shipment consumer handled the message
    And the network accepted 1 shipment confirmation for "po-1"
    And the domain event "NetworkOrderShipmentConfirmed" was published 1 time for "po-1"

  # ---- the contract with outbound-shipping ------------------------------------------

  @bdd
  Scenario: Without a load id in the payload the Kafka key is the shipment reference
    Given a network order "po-1" in state "ACKNOWLEDGED"
    When outbound-shipping confirms a load whose payload names none and whose Kafka key is "load-7", carrying:
      | order              | packages |
      | held order of po-1 |          |
    And that shipment event is delivered again
    Then the shipment consumer handled the message
    And the network accepted 1 shipment confirmation for "po-1"

  @bdd
  Scenario: A load that names no load id anywhere is refused
    Given a network order "po-1" in state "ACKNOWLEDGED"
    When outbound-shipping confirms a load whose payload names none and whose Kafka key is "", carrying:
      | order              | packages |
      | held order of po-1 |          |
    Then the shipment consumer refused the message as "names no load_id"
    And the network accepted 0 shipment confirmations for "po-1"

  @bdd
  Scenario: An orders entry without an order id breaks the contract
    Given a network order "po-1" in state "ACKNOWLEDGED"
    When outbound-shipping confirms load "load-9" carrying:
      | order       | packages |
      | no order id |          |
    Then the shipment consumer refused the message as "without order_id"

  @bdd
  Scenario: A negative package count breaks the contract
    Given a network order "po-1" in state "ACKNOWLEDGED"
    When outbound-shipping confirms load "load-9" carrying:
      | order              | packages |
      | held order of po-1 | -1       |
    Then the shipment consumer refused the message as "negative package_count"
    And the network accepted 0 shipment confirmations for "po-1"

  @bdd
  Scenario: A message that is not a CloudEvent is refused
    When a message that is not a CloudEvent reaches the shipment consumer
    Then the shipment consumer refused the message as "not a valid CloudEvents 1.0 event"
