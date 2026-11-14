# Derived from the context documentation (warehouse-docs network-fulfillment:
# use-cases.md DeclareNodeProfiles, troubleshooting.md "Boot and readiness"
# rows on the node profile, business-context.md) and docs/adr/0027 part 1. The
# profile is derived only from the local caches (the CPT schedule and cycle
# times, the remaining capacity): a profile the caches cannot support is never
# declared, because the handling time declared here becomes part of the deadline
# the network judges our acknowledgements by. The network is a recording double.
Feature: Node profile declaration, documented rules
  Per site, the whole weekly schedule is declared with a handling time, a
  ceiling per cutoff and a throughput per day: derived, never guessed, and a
  declared ceiling is never lowered by an observation.

  # Every scenario runs at Thursday 2026-10-08 12:00 UTC.

  @bdd
  Scenario: A site with no cutoff schedule in the cache declares nothing
    Given the site "site-1" is served by the node profile worker
    When the node profiles are declared
    Then the node profile pass ended 1 time with "skipped_no_schedule"
    And no node profile was declared for "site-1"

  @bdd
  Scenario: A schedule whose paths have no cycle time declares nothing
    Given the CPT schedule of site "site-1" is in timezone "America/Chicago"
    And the schedule has a cutoff at "18:00" on every day, next in 6 hours
    And path "p1" serves that cutoff with an unknown cycle time
    And path "p1" has 300 units of remaining capacity
    When the node profiles are declared
    Then the node profile pass ended 1 time with "skipped_incomplete"
    And no node profile was declared for "site-1"

  @bdd
  Scenario: A schedule with no capacity figure known declares nothing
    Given the CPT schedule of site "site-1" is in timezone "America/Chicago"
    And the schedule has a cutoff at "18:00" on every day, next in 6 hours
    And path "p1" serves that cutoff with a cycle time of 95 minutes
    When the node profiles are declared
    Then the node profile pass ended 1 time with "skipped_incomplete"
    And no node profile was declared for "site-1"

  @bdd
  Scenario Outline: The handling time is the longest cycle time, rounded UP to whole minutes
    Given the CPT schedule of site "site-1" is in timezone "America/Chicago"
    And the schedule has a cutoff at "18:00" on every day, next in 6 hours
    And path "p1" serves that cutoff with a cycle time of <seconds> seconds
    And path "p1" has 300 units of remaining capacity
    When the node profiles are declared
    Then the node profile declared for "site-1" has a handling time of <minutes> minutes

    Examples:
      | seconds | minutes | case                                   |
      | 5400    | 90      | already whole minutes                  |
      | 5401    | 91      | one second over is a whole extra minute |
      | 5460    | 91      | exactly 91 minutes stays 91            |
      | 5461    | 92      | and one second more is 92              |

  @bdd
  Scenario: A path with no cycle time does not set the handling time but still counts its capacity
    Given the CPT schedule of site "site-1" is in timezone "America/Chicago"
    And the schedule has a cutoff at "18:00" on every day, next in 6 hours
    And path "p-known" serves that cutoff with a cycle time of 60 minutes
    And path "p-blank" serves that cutoff with an unknown cycle time
    And path "p-known" has 40 units of remaining capacity
    And path "p-blank" has 25 units of remaining capacity
    When the node profiles are declared
    Then the node profile declared for "site-1" has a handling time of 60 minutes
    And the node profile declared for "site-1" allows 65 orders at the cutoff at "18:00"

  @bdd
  Scenario Outline: A path counts for the handling time only if its cycle time fits the longest gap of the schedule
    Given the CPT schedule of site "site-1" is in timezone "America/Chicago"
    And the schedule has a cutoff at "11:00" on every day, next in 23 hours
    And path "p-fast" serves that cutoff with a cycle time of 120 minutes
    And the schedule has a cutoff at "18:00" on every day, next in 6 hours
    And path "p-fast" serves that cutoff with a cycle time of 120 minutes
    And path "p-slow" serves that cutoff with a cycle time of <cycle> minutes
    And path "p-fast" has 50 units of remaining capacity
    And path "p-slow" has 100 units of remaining capacity
    When the node profiles are declared
    Then the node profile declared for "site-1" has a handling time of <handling> minutes

    Examples:
      | cycle | handling | case                                                                    |
      | 1020  | 1020     | the longest gap is 17h (18:00 to 11:00): a 17h path fits it             |
      | 1021  | 120      | one minute more than the longest gap: ruled out, no data is not evidence |

  @bdd
  Scenario Outline: A cutoff's ceiling counts only the paths whose cycle time fits the time left to it
    Given the CPT schedule of site "site-1" is in timezone "America/Chicago"
    And the schedule has a cutoff at "18:00" on every day, next in 3 hours
    And path "p-fast" serves that cutoff with a cycle time of 30 minutes
    And path "p-slow" serves that cutoff with a cycle time of <cycle> minutes
    And path "p-fast" has 40 units of remaining capacity
    And path "p-slow" has 100 units of remaining capacity
    When the node profiles are declared
    Then the node profile declared for "site-1" allows <orders> orders at the cutoff at "18:00"

    Examples:
      | cycle | orders | case                                       |
      | 180   | 140    | equal to the time left still fits          |
      | 181   | 40     | one minute too slow for this occurrence    |

  @bdd
  Scenario Outline: A declared ceiling is never lowered by an observation
    Given the CPT schedule of site "site-1" is in timezone "America/Chicago"
    And the schedule has a cutoff at "18:00" on every day, next in 6 hours
    And path "p1" serves that cutoff with a cycle time of 95 minutes
    And path "p1" has <observed> units of remaining capacity
    And the profile last declared for site "site-1" allowed 500 orders at "18:00" on every day
    When the node profiles are declared
    Then the node profile declared for "site-1" allows <declared> orders at the cutoff at "18:00"

    Examples:
      | observed | declared | case                                                   |
      | 120      | 500      | work was admitted: the figure fell, the ceiling stays  |
      | 500      | 500      | unchanged                                              |
      | 800      | 800      | a higher observation raises it                         |

  @bdd
  Scenario: Cutoffs that share a local time are one cutoff, with their days and paths united
    Given the CPT schedule of site "site-1" is in timezone "America/Chicago"
    And the schedule has a cutoff at "18:00" on Monday, Tuesday and Wednesday, next in 6 hours
    And path "p1" serves that cutoff with a cycle time of 95 minutes
    And the schedule has a cutoff at "18:00" on Thursday and Friday, next in 54 hours
    And path "p2" serves that cutoff with a cycle time of 95 minutes
    And path "p1" has 100 units of remaining capacity
    And path "p2" has 60 units of remaining capacity
    When the node profiles are declared
    Then the node profile declared for "site-1" has 1 cutoff
    And the cutoff at "18:00" of the node profile declared for "site-1" applies on Monday, Tuesday, Wednesday, Thursday and Friday
    And the node profile declared for "site-1" allows 160 orders at the cutoff at "18:00"

  @bdd
  Scenario: The throughput per day is what the busiest weekday's cutoffs can take together
    Given the CPT schedule of site "site-1" is in timezone "America/Chicago"
    And the schedule has a cutoff at "11:00" on weekdays, next in 23 hours
    And path "p1" serves that cutoff with a cycle time of 95 minutes
    And the schedule has a cutoff at "18:00" on Monday, next in 6 hours
    And path "p2" serves that cutoff with a cycle time of 60 minutes
    And path "p1" has 100 units of remaining capacity
    And path "p2" has 70 units of remaining capacity
    When the node profiles are declared
    Then the node profile declared for "site-1" has 2 cutoffs
    And the node profile declared for "site-1" has a throughput of 170 orders per day

  @bdd
  Scenario: The declaration carries the schedule's own timezone
    Given the CPT schedule of site "site-1" is in timezone "America/Chicago"
    And the schedule has a cutoff at "18:00" on every day, next in 6 hours
    And path "p1" serves that cutoff with a cycle time of 95 minutes
    And path "p1" has 300 units of remaining capacity
    When the node profiles are declared
    Then the node profile pass ended 1 time with "declared"
    And the node profile declared for "site-1" is in timezone "America/Chicago"
    And the node profile declared for "site-1" has 1 cutoff
    And the node profile declared for "site-1" has a throughput of 300 orders per day

  @bdd
  Scenario: A site with no node on the network yet is skipped, not failed
    Given the CPT schedule of site "site-1" is in timezone "America/Chicago"
    And the schedule has a cutoff at "18:00" on every day, next in 6 hours
    And path "p1" serves that cutoff with a cycle time of 95 minutes
    And path "p1" has 300 units of remaining capacity
    And the network has no node for the node profile of site "site-1"
    When the node profiles are declared
    Then the node profile pass ended 1 time with "skipped_no_node"
    And the node profile pass ended 0 times with "failed"

  @bdd
  Scenario: One site's failure never stops the others
    Given the CPT schedule of site "site-1" is in timezone "America/Chicago"
    And the schedule has a cutoff at "18:00" on every day, next in 6 hours
    And path "p1" serves that cutoff with a cycle time of 95 minutes
    And the CPT schedule of site "site-2" is in timezone "Europe/Berlin"
    And the schedule has a cutoff at "17:00" on weekdays, next in 5 hours
    And path "p2" serves that cutoff with a cycle time of 45 minutes
    And path "p1" has 300 units of remaining capacity
    And path "p2" has 80 units of remaining capacity
    And the network fails for the node profile of site "site-1"
    When the node profiles are declared
    Then the node profile pass ended 1 time with "failed"
    And the node profile pass ended 1 time with "declared"
    And the node profile declared for "site-2" has a handling time of 45 minutes

  @bdd
  Scenario: A memory that cannot be read leaves the site for the next pass instead of risking a lower ceiling
    Given the CPT schedule of site "site-1" is in timezone "America/Chicago"
    And the schedule has a cutoff at "18:00" on every day, next in 6 hours
    And path "p1" serves that cutoff with a cycle time of 95 minutes
    And path "p1" has 120 units of remaining capacity
    And the profile last declared for site "site-1" allowed 500 orders at "18:00" on every day
    And the profile history cannot be read
    When the node profiles are declared
    Then the node profile pass ended 1 time with "failed"
    And no node profile was declared for "site-1"
    When the profile history can be read again
    And the node profiles are declared
    Then the node profile declared for "site-1" allows 500 orders at the cutoff at "18:00"
