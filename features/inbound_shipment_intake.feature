# Derived from: docs/adr/0028-inbound-loop-seller-shipments-and-receipts.md and
# the Vendor API's inbound shipments (vendor-connectivity ADR 0005). Sellers ship
# SSCC-labelled cartons to a site; the network tells us by polling; each shipment
# becomes an ASN in inbound-receiving, once. The suite drives the real intake use
# case over the real stub gateway, with in-process doubles for the record,
# product-master and inbound-receiving.
Feature: Inbound shipment intake
  A polled seller shipment becomes one ASN with hierarchical cartons and per-SKU
  lines in eaches. A replay is a no-op, a GTIN nobody declared makes the shipment
  wait (never guessed), and a verdict about the data parks it.

  Background:
    Given product-master declares each GTIN "00614141000036" as "sku-a"
    And product-master declares each GTIN "00614141000043" as "sku-b"

  @bdd
  Scenario: A shipment becomes an ASN with its cartons and rolled-up lines
    Given the network has shipment "SHP-1" from seller "S-100" with a pallet carton of 12 of "00614141000036" and a mixed carton of 6 of "00614141000036" and 4 of "00614141000043"
    When the inbound intake runs
    Then inbound-receiving holds ASN "NF-SHP-1" owned by "S-100"
    And ASN "NF-SHP-1" has line 1 "sku-a" for 18 eaches
    And ASN "NF-SHP-1" has line 2 "sku-b" for 4 eaches
    And ASN "NF-SHP-1" has 2 cartons and the first is on a pallet
    And shipment "SHP-1" is "REGISTERED"

  @bdd
  Scenario: Polling the same shipment again registers nothing twice
    Given the network has shipment "SHP-1" from seller "S-100" with a pallet carton of 12 of "00614141000036" and a mixed carton of 6 of "00614141000036" and 4 of "00614141000043"
    When the inbound intake runs
    And the inbound intake runs
    And the inbound intake runs
    Then inbound-receiving was asked to register 1 ASN

  @bdd
  Scenario: A GTIN nobody declared makes the shipment wait, then it registers
    Given the network has shipment "SHP-2" from seller "S-100" with a pallet carton of 12 of "00614141000036" and a mixed carton of 6 of "00614141000036" and 4 of "00614141000050"
    When the inbound intake runs
    Then shipment "SHP-2" is "PENDING" because "GTIN not found"
    And inbound-receiving was asked to register 0 ASNs
    When product-master declares each GTIN "00614141000050" as "sku-c"
    And 2 minutes pass
    And the inbound intake runs
    Then shipment "SHP-2" is "REGISTERED"

  @bdd
  Scenario: A verdict from inbound-receiving parks the shipment
    Given inbound-receiving refuses ASN "NF-SHP-3" as a duplicate SSCC
    And the network has shipment "SHP-3" from seller "S-100" with a pallet carton of 12 of "00614141000036" and a mixed carton of 6 of "00614141000036" and 4 of "00614141000043"
    When the inbound intake runs
    And 2 hours pass
    And the inbound intake runs
    Then shipment "SHP-3" is "REJECTED"
    And inbound-receiving was asked to register 1 ASN
