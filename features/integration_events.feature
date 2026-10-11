# Derived from: .claude/rules/domain-model.md (domain events NetworkOrderReceived,
# NetworkOrderSubmitted, NetworkOrderAcknowledged, NetworkOrderRejected,
# NetworkOrderShipmentConfirmed, AcknowledgementDeadlineAtRisk) and
# apis/asyncapi.yaml; docs/adr/0008 (CloudEvents 1.0 is the mandatory
# envelope), 0016 (NetworkOrderAcknowledged is published as type/dataschema v2),
# 0005 (per-aggregate partition affinity). The events are produced by the real
# Kafka encoders into a recording writer; no broker is involved.
Feature: Integration events as CloudEvents 1.0
  Every Kafka message this context publishes is a CloudEvents 1.0 envelope in
  structured mode, on both the integration and the analytics stream. There is
  no other envelope, and a consumer refuses the retired flat one.

  Background:
    Given the product dictionary maps "ASIN-1" to "sku-1"

  @bdd
  Scenario: A full order lifecycle publishes only valid CloudEvents 1.0 envelopes
    Given a network order "po-1" in state "CONFIRMED"
    And a network order "po-2" rejected because of "INFEASIBLE_DEADLINE"
    And a network order "po-3" in state "NEW"
    And 25 hours pass
    When the acknowledgement deadline sweep runs
    Then every published message is a valid CloudEvents 1.0 envelope
    And every domain event was published on both the events and the analytics stream
    And no two published messages share a CloudEvents id

  @bdd
  Scenario Outline: Each domain event has its own CloudEvents type and dataschema on every stream
    Given a network order "po-1" in state "CONFIRMED"
    And a network order "po-2" rejected because of "INFEASIBLE_DEADLINE"
    And a network order "po-3" in state "NEW"
    And 25 hours pass
    When the acknowledgement deadline sweep runs
    Then a CloudEvent of type "<type>" with dataschema "urn:warehouse:network-fulfillment:<stream>:<schema>" was published on the "<stream>" stream for "<ref>"

    Examples:
      | stream    | ref  | type                                                                       | schema                        |
      | events    | po-1 | com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderReceived                | NetworkOrderReceived:v1       |
      | events    | po-1 | com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderSubmitted               | NetworkOrderSubmitted:v1      |
      | events    | po-1 | com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderAcknowledged.v2         | NetworkOrderAcknowledged:v2   |
      | events    | po-1 | com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderShipmentConfirmed       | NetworkOrderShipmentConfirmed:v1 |
      | events    | po-2 | com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderRejected                | NetworkOrderRejected:v1       |
      | events    | po-3 | com.warehouse.wes.network-fulfillment.networkorder.AcknowledgementDeadlineAtRisk       | AcknowledgementDeadlineAtRisk:v1 |
      | analytics | po-1 | com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderReceived                | NetworkOrderReceived:v1       |
      | analytics | po-1 | com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderAcknowledged.v2         | NetworkOrderAcknowledged:v2   |
      | analytics | po-1 | com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderShipmentConfirmed       | NetworkOrderShipmentConfirmed:v1 |
      | analytics | po-2 | com.warehouse.wes.network-fulfillment.networkorder.NetworkOrderRejected                | NetworkOrderRejected:v1       |
      | analytics | po-3 | com.warehouse.wes.network-fulfillment.networkorder.AcknowledgementDeadlineAtRisk       | AcknowledgementDeadlineAtRisk:v1 |

  @bdd
  Scenario: Every event of one order is keyed by its networkRef so the partition preserves order
    Given a network order "po-1" in state "CONFIRMED"
    Then every message for "po-1" is keyed by "po-1"

  @bdd
  Scenario Outline: A rejection carries the reason it was refused for
    Given a network order "po-1" rejected because of "<reason>"
    Then the NetworkOrderRejected event for "po-1" carries reason "<reason>"

    Examples:
      | reason                          |
      | UNTRANSLATABLE_SKU              |
      | INFEASIBLE_DEADLINE             |
      | ACKNOWLEDGEMENT_DEADLINE_MISSED |
      | SUBMISSION_FAILED               |

  @bdd
  Scenario: The retired flat envelope is refused by the analytics consumer
    When a legacy flat-envelope message reaches the analytics consumer
    Then the analytics consumer rejects it as not a CloudEvent
