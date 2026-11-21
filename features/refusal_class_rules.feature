# Derived from: docs/adr/0037-retry-the-refusals-the-callees-release.md
# (Decision 3, the ASN-registration half) and the fleet idempotency rule of
# 2026-11-18 (.claude/rules/fleet/idempotency-and-outbox.md). A refusal
# inbound-receiving RELEASES committed nothing and freed the Idempotency-Key,
# so the same ASN is registered again under the SAME key and may pass; the
# loop is bounded where it had no bound before. The suite drives the real
# intake use case; the double answers with the classified refusals the real
# adapter derives from inbound-receiving's replay-class table.
Feature: Refusal classes on the ASN registration
  A released refusal is not a verdict: the shipment stays PENDING and is asked
  again under the same ASN number and so the same Idempotency-Key, until the
  attempt bound ends it as a final refusal would. A final refusal is a verdict
  at once. A transport failure was never a verdict and is never bounded.

  Background:
    Given product-master declares each GTIN "00614141000036" as "sku-a"
    And product-master declares each GTIN "00614141000043" as "sku-b"

  @bdd
  Scenario: A released refusal keeps the shipment PENDING under the same key
    Given inbound-receiving refuses ASN "NF-SHP-40" with a released "unknown-sku"
    And the network has shipment "SHP-40" from seller "S-100" with a pallet carton of 12 of "00614141000036" and a mixed carton of 6 of "00614141000036" and 4 of "00614141000043"
    When the inbound intake runs
    And 2 hours pass
    And the inbound intake runs
    Then shipment "SHP-40" is "PENDING" because "unknown-sku"
    And inbound-receiving was asked to register 2 ASNs
    And every refusal was for the same ASN "NF-SHP-40"

  @bdd
  Scenario: A released refusal the callee later accepts registers the shipment
    Given inbound-receiving refuses ASN "NF-SHP-41" with a released "duplicate-sscc"
    And the network has shipment "SHP-41" from seller "S-100" with a pallet carton of 12 of "00614141000036" and a mixed carton of 6 of "00614141000036" and 4 of "00614141000043"
    When the inbound intake runs
    Then shipment "SHP-41" is "PENDING"
    Given inbound-receiving stops refusing ASN "NF-SHP-41"
    When 2 hours pass
    And the inbound intake runs
    Then shipment "SHP-41" is "REGISTERED"
    And inbound-receiving holds ASN "NF-SHP-41" owned by "S-100"

  @bdd
  Scenario: The attempt bound ends the shipment exactly at the last attempt
    Given the intake gives up after 3 released refusals
    And inbound-receiving refuses ASN "NF-SHP-42" with a released "unknown-sku"
    And the network has shipment "SHP-42" from seller "S-100" with a pallet carton of 12 of "00614141000036" and a mixed carton of 6 of "00614141000036" and 4 of "00614141000043"
    When the inbound intake runs
    Then shipment "SHP-42" is "PENDING"
    When 1 minute passes
    And the inbound intake runs
    Then shipment "SHP-42" is "PENDING"
    And inbound-receiving was asked to register 2 ASNs
    When 2 minutes pass
    And the inbound intake runs
    Then shipment "SHP-42" is "REJECTED" because "still released after 3 attempts, giving up"
    And inbound-receiving was asked to register 3 ASNs

  @bdd
  Scenario: A final refusal is a verdict at once, whatever the bound allows
    Given the intake gives up after 3 released refusals
    And inbound-receiving refuses ASN "NF-SHP-43" with a final "receipt-closed"
    And the network has shipment "SHP-43" from seller "S-100" with a pallet carton of 12 of "00614141000036" and a mixed carton of 6 of "00614141000036" and 4 of "00614141000043"
    When the inbound intake runs
    Then shipment "SHP-43" is "REJECTED" because "receipt-closed"
    And inbound-receiving was asked to register 1 ASN

  @bdd
  Scenario: A transport failure is never a verdict and never counts toward the bound
    Given the intake gives up after 3 released refusals
    And inbound-receiving is unreachable
    And the network has shipment "SHP-44" from seller "S-100" with a pallet carton of 12 of "00614141000036" and a mixed carton of 6 of "00614141000036" and 4 of "00614141000043"
    When the inbound intake runs
    And 1 minute passes
    And the inbound intake runs
    And 2 minutes pass
    And the inbound intake runs
    Then shipment "SHP-44" is "PENDING" because "unreachable"
    And inbound-receiving was asked to register 3 ASNs

  @bdd
  Scenario: The default bound is thirty attempts, about a day of backoff
    Given inbound-receiving refuses ASN "NF-SHP-45" with a released "unknown-sku"
    And the network has shipment "SHP-45" from seller "S-100" with a pallet carton of 12 of "00614141000036" and a mixed carton of 6 of "00614141000036" and 4 of "00614141000043"
    When the inbound intake runs
    And the intake keeps retrying shipment "SHP-45" for 26 hours in one-minute passes
    Then shipment "SHP-45" is "REJECTED" because "still released after 30 attempts, giving up"
    And inbound-receiving was asked to register 30 ASNs
