# Derived from: docs/adr/0028-inbound-loop-seller-shipments-and-receipts.md and the
# Vendor API's POST /vendor/inbound-receipts (vendor-connectivity ADR 0005). When
# the dock closes a receipt for an ASN this service registered, what it found
# (per carton: received, missing, unexpected, damaged, with compliance codes from
# the carton discrepancies) goes back to the network, once. The suite drives the
# real use case and the real stub gateway over an in-process receipt projection.
Feature: Inbound receipt return
  The network learns from the receipt which cartons arrived, which did not, which
  were not on the manifest and which were damaged, in eaches per GTIN.

  Background:
    Given the manifest of shipment "SHP-1" lists cartons "006141410000000012", "006141410000000029" and "006141410000000036"

  @bdd
  Scenario: Every carton gets the status the dock gave it
    Given the dock received carton "006141410000000012" with 12 eaches of "00614141000036" in good condition
    And the dock received carton "006141410000000029" with 5 eaches of "00614141000043" in damaged condition
    And the dock found carton "006141410000000099" that is not on the manifest
    When receiving closes on shipment "SHP-1"
    And the receipt return runs
    Then the network holds a receipt for "SHP-1" with 4 cartons
    And carton "006141410000000012" of the receipt is "RECEIVED" with 12 eaches of "00614141000036"
    And carton "006141410000000029" of the receipt is "DAMAGED" with 5 eaches of "00614141000043"
    And carton "006141410000000036" of the receipt is "MISSING" with no items
    And carton "006141410000000099" of the receipt is "UNEXPECTED" with no items

  @bdd
  Scenario: Discrepancies of a carton become its compliance codes
    Given the dock received carton "006141410000000012" with 11 eaches of "00614141000036" in good condition
    And the dock raised "CaseQtyMismatch" on carton "006141410000000012"
    And the dock raised "CartonOverLimit" on carton "006141410000000012"
    When receiving closes on shipment "SHP-1"
    And the receipt return runs
    Then carton "006141410000000012" of the receipt has the compliance codes "CARTON_OVER_LIMIT,CASE_QTY_MISMATCH"

  @bdd
  Scenario: A receipt is reported once however often the loop runs
    Given the dock received carton "006141410000000012" with 12 eaches of "00614141000036" in good condition
    When receiving closes on shipment "SHP-1"
    And the receipt return runs
    And the receipt return runs
    Then the network was sent 1 receipt

  @bdd
  Scenario: A network that is down is retried with a backoff
    Given the network is down
    And the dock received carton "006141410000000012" with 12 eaches of "00614141000036" in good condition
    When receiving closes on shipment "SHP-1"
    And the receipt return runs
    Then the network holds no receipt for "SHP-1"
    When the network is back
    And the receipt return runs
    Then the network holds no receipt for "SHP-1"
    When 2 minutes pass
    And the receipt return runs
    Then the network holds a receipt for "SHP-1" with 3 cartons
