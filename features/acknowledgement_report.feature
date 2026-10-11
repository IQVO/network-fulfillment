# Derived from: apis/openapi.yaml (getAcknowledgementReport,
# getAcknowledgementReportFreshness, schemas AcknowledgementReport /
# AcknowledgementReportRow / Freshness, and the 400 problem document) and
# .claude/rules/domain-model.md; docs/adr/0002 (analytics data product),
# 0015 (ordersRejectedSubmissionFailed), 0016 (the report counts SETTLED
# acknowledgements). The read model is projected from the context's own
# analytics CloudEvents by the real analytics consumer, and served by the real
# chi router of the netfulfil-reports deployable.
Feature: Network Order Acknowledgement & Translation report
  Per day: how much demand arrived, how it was answered, why it was refused,
  and how fast. It is derived entirely from this context's own events, never
  from the OLTP store, and says how far it lags real time.

  Background:
    Given the product dictionary maps "ASIN-1" to "sku-1"

  @bdd
  Scenario: With nothing projected the report is an empty rows array
    When I request the acknowledgement report from "2026-10-08T00:00:00Z" to "2026-10-09T00:00:00Z"
    Then the response status is 200
    And the report has 0 rows
    And the report has an empty rows array

  @bdd
  Scenario: Receipts and settled acknowledgements are counted with their average latency
    Given a network order "po-1" in state "SUBMITTED"
    And 90 seconds pass
    When the acknowledgement reconciliation runs
    And I request the acknowledgement report from "2026-10-08T00:00:00Z" to "2026-10-09T00:00:00Z"
    Then the report has 1 row
    And the report row for day "2026-10-08T00:00:00Z" has "ordersReceived" equal to 1
    And the report row for day "2026-10-08T00:00:00Z" has "ordersAcknowledged" equal to 1
    And the report row for day "2026-10-08T00:00:00Z" has "avgAcknowledgementLatencySeconds" equal to 90

  @bdd
  Scenario: A submission that is not yet settled is received but not yet acknowledged, with latency zero
    Given a network order "po-1" in state "SUBMITTED"
    When I request the acknowledgement report from "2026-10-08T00:00:00Z" to "2026-10-09T00:00:00Z"
    Then the report row for day "2026-10-08T00:00:00Z" has "ordersReceived" equal to 1
    And the report row for day "2026-10-08T00:00:00Z" has "ordersAcknowledged" equal to 0
    And the report row for day "2026-10-08T00:00:00Z" has "avgAcknowledgementLatencySeconds" equal to 0

  @bdd
  Scenario Outline: Rejections are split by cause, never lumped together
    Given a network order "po-1" rejected because of "<reason>"
    When I request the acknowledgement report from "2026-10-08T00:00:00Z" to "2026-10-10T00:00:00Z"
    Then the report row for day "<day>" has "<column>" equal to 1
    And the report row for day "2026-10-08T00:00:00Z" has "ordersReceived" equal to 1
    And the report row for day "2026-10-08T00:00:00Z" has "ordersAcknowledged" equal to 0

    Examples:
      | reason                          | column                          | day                  |
      | UNTRANSLATABLE_SKU              | ordersRejectedUntranslatableSku | 2026-10-08T00:00:00Z |
      | INFEASIBLE_DEADLINE             | ordersRejectedDomain            | 2026-10-08T00:00:00Z |
      | ACKNOWLEDGEMENT_DEADLINE_MISSED | acknowledgementDeadlinesMissed  | 2026-10-09T00:00:00Z |
      | SUBMISSION_FAILED               | ordersRejectedSubmissionFailed  | 2026-10-08T00:00:00Z |

  @bdd
  Scenario: Each UTC day is its own row
    Given a network order "po-1" in state "SUBMITTED"
    And 24 hours pass
    And a network order "po-2" in state "SUBMITTED"
    When I request the acknowledgement report from "2026-10-08T00:00:00Z" to "2026-10-10T00:00:00Z"
    Then the report has 2 rows
    And the report row for day "2026-10-08T00:00:00Z" has "ordersReceived" equal to 1
    And the report row for day "2026-10-09T00:00:00Z" has "ordersReceived" equal to 1

  @bdd
  Scenario Outline: The window is inclusive of from and exclusive of to
    Given a network order "po-1" in state "SUBMITTED"
    When I request the acknowledgement report from "<from>" to "<to>"
    Then the report has <rows> rows

    Examples:
      | from                 | to                   | rows | case                                  |
      | 2026-10-08T00:00:00Z | 2026-10-09T00:00:00Z | 1    | the day itself                        |
      | 2026-10-07T00:00:00Z | 2026-10-08T00:00:00Z | 0    | to is exclusive of the day's bucket   |
      | 2026-10-09T00:00:00Z | 2026-10-10T00:00:00Z | 0    | the day is before the window          |
      | 2026-10-08T00:00:00Z | 2026-10-08T00:00:00Z | 0    | an empty window                       |

  @bdd
  Scenario: The only granularity is day, and it is also the default
    Given a network order "po-1" in state "SUBMITTED"
    When I request the acknowledgement report with query "from=2026-10-08T00:00:00Z&to=2026-10-09T00:00:00Z&granularity=day"
    Then the response status is 200
    And the report has 1 row

  @bdd
  Scenario Outline: A malformed report query is a 400 problem document
    When I request the acknowledgement report with query "<query>"
    Then the response status is 400
    And the problem detail type is "invalid-report-query"

    Examples:
      | query                                                                        | case                    |
      | to=2026-10-09T00:00:00Z                                                      | from is missing         |
      | from=2026-10-08T00:00:00Z                                                    | to is missing           |
      | from=yesterday&to=2026-10-09T00:00:00Z                                       | from is not RFC3339     |
      | from=2026-10-08T00:00:00Z&to=tomorrow                                        | to is not RFC3339       |
      | from=2026-10-08T00:00:00Z&to=2026-10-09T00:00:00Z&granularity=week           | granularity is not day  |

  @bdd
  Scenario: A query with no parameters at all is a 400 problem document
    When I request the acknowledgement report with no query
    Then the response status is 400
    And the problem detail type is "invalid-report-query"

  @bdd
  Scenario: Redelivering an analytics event does not double count
    Given a network order "po-1" in state "SUBMITTED"
    When the last analytics message is delivered again
    And I request the acknowledgement report from "2026-10-08T00:00:00Z" to "2026-10-09T00:00:00Z"
    Then the report row for day "2026-10-08T00:00:00Z" has "ordersReceived" equal to 1

  @bdd
  Scenario: A retired flat-envelope message changes nothing in the report
    Given a legacy flat-envelope message reaches the analytics consumer
    When I request the acknowledgement report from "2026-10-08T00:00:00Z" to "2026-10-09T00:00:00Z"
    Then the report has 0 rows

  @bdd
  Scenario: Freshness is zero when nothing has been projected
    When I request the report freshness
    Then the response status is 200
    And the freshness lag is 0 seconds

  @bdd
  Scenario: Freshness is the time since the most recently projected event
    Given a network order "po-1" in state "SUBMITTED"
    And 90 seconds pass
    When I request the report freshness
    Then the freshness lag is 90 seconds

  @bdd
  Scenario: A newer event resets the freshness lag
    Given a network order "po-1" in state "SUBMITTED"
    And 10 minutes pass
    And a network order "po-2" in state "SUBMITTED"
    And 30 seconds pass
    When I request the report freshness
    Then the freshness lag is 30 seconds

  @bdd
  Scenario: The report endpoints require no credentials
    When I GET "/reports/acknowledgement/freshness" on the reports service
    Then the response status is 200
    And the response is not an authentication challenge
