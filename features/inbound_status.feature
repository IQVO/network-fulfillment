# Derived from: apis/openapi.yaml (getInboundStatus, schema InboundStatus:
# polls, received, failed, since, unanswered, overdue, networkMode) and
# .claude/rules/domain-model.md (poller feeds ReceiveNetworkDemand; a failed
# unit does not advance the watermark); docs/adr/0001 §5, 0009.
Feature: Inbound status
  A stalled poller is SILENT - no orders, no errors, a healthy liveness probe -
  while the 24-hour acknowledgement clock keeps running. The inbound status is
  how an operator tells a quiet network from a dead poller.

  Background:
    Given the product dictionary maps "ASIN-1" to "sku-1"

  @bdd
  Scenario: Before any pass nothing has been polled and there is no watermark
    When I request the inbound status
    Then the response status is 200
    And the inbound status reports network mode "stub"
    And the inbound status reports polls 0
    And the inbound status reports received 0
    And the inbound status reports failed 0
    And the inbound status reports unanswered 0
    And the inbound status reports overdue 0
    And the inbound status has no watermark

  @bdd
  Scenario: A clean pass with nothing to fetch advances the watermark
    When the poller runs a pass
    And I request the inbound status
    Then the inbound status reports polls 1
    And the inbound status reports received 0
    And the inbound status watermark is "2026-10-08T12:00:00Z"

  @bdd
  Scenario: Processed demand is counted as received
    Given the network has demand "po-1" for 2 units of "ASIN-1"
    And the network has demand "po-2" for 1 unit of "ASIN-1"
    When the poller runs a pass
    And I request the inbound status
    Then the inbound status reports polls 1
    And the inbound status reports received 2
    And the inbound status reports failed 0
    And the inbound status reports unanswered 0
    And the inbound status watermark is "2026-10-08T12:00:00Z"

  @bdd
  Scenario: A failing pass counts the failure and does not advance the watermark
    Given the poller runs a pass
    And 10 minutes pass
    And order-management is unavailable
    And the network has demand "po-1" for 2 units of "ASIN-1"
    When the poller runs a pass
    And I request the inbound status
    Then the inbound status reports polls 2
    And the inbound status reports received 0
    And the inbound status reports failed 1
    And the inbound status reports unanswered 1
    And the inbound status watermark is "2026-10-08T12:00:00Z"

  @bdd
  Scenario: A pass that has never succeeded leaves the watermark absent
    Given order-management is unavailable
    And the network has demand "po-1" for 2 units of "ASIN-1"
    When the poller runs a pass
    And I request the inbound status
    Then the inbound status reports polls 1
    And the inbound status reports failed 1
    And the inbound status has no watermark

  @bdd
  Scenario: The unanswered working set and how much of it is already overdue
    Given a network order "po-old" in state "NEW"
    And 2 hours pass
    And a network order "po-recent" in state "NEW"
    And a network order "po-answered" in state "SUBMITTED"
    And 23 hours pass
    When I request the inbound status
    Then the inbound status reports unanswered 2
    And the inbound status reports overdue 1
