# Ubiquitous Language

The terms this context speaks, each mapped to the code identifier that
implements it. Where the business word and the code name differ, the
difference is flagged in the last section. Code wins over this page.

## Network side (the counterpart's concepts, translated at the ACL)

| Term | Meaning | Code identifier |
| --- | --- | --- |
| **Network** | The external retail fulfillment network this context is Conformist to. Since ADR 0009 the fleet's own `retail-network` plays this role. | `ports.NetworkGateway`; `internal/adapters/outbound/network` |
| **NetworkRef** | The network's own opaque identity for one unit of demand (its purchase-order number). It is never parsed, and it is the primary key. | `shared.NetworkRef`; column `network_orders.network_ref` |
| **NetworkLineRef** | The network's identity for one line, unique only within its order. | `shared.NetworkLineRef`; `network_order_lines.network_line_ref` |
| **NetworkProductId** | The network's product identifier. It is **not** a SKU. | `shared.NetworkProductId`; `network_order_lines.network_product_id` |
| **Inbound demand** | One unit of demand as polled, before translation. | `contract.InboundDemand`, `contract.InboundLine` |
| **Network mode** | Which gateway is wired: `stub` (default) or `live` (refuses to boot). | `network.Mode`, `ModeStub`, `ModeLive`; env `NETWORK_MODE` |
| **Submission status** | The network's transaction-status answer for a submission: `PENDING`, `SUCCESS` or `FAILURE`. | `contract.SubmissionStatusValue`, `SubmissionPending/Success/Failure` |
| **Label** | What a label request may return: `{labelRef, trackingNumber, carrier}` and never ship-to PII. | `contract.LabelResult` (port only; no caller) |

## Our side

| Term | Meaning | Code identifier |
| --- | --- | --- |
| **NetworkOrder** | Demand from the network with a deadline we did not choose, answered exactly once, in full or not at all. | `networkorder.NetworkOrder` |
| **Line** | One line already translated into our vocabulary. It keeps the network's product id, because answers go back in the network's terms. | `networkorder.Line`, `NewLine` |
| **SKU** | Our product identity. It is only ever the output of translation. | `shared.SKU` |
| **Product translation** | The ACL dictionary from `NetworkProductId` to `SKU`. | `ports.ProductTranslation` (`ToSKU`, `KnownSKUs`); file `PRODUCT_TRANSLATION_FILE` |
| **Untranslatable demand** | Demand naming a product with no mapping. It is recorded lineless and rejected. | `networkorder.ReceiveUntranslatable`; `shared.ErrUnknownProduct` |
| **SiteId** | The fulfillment site the demand is destined for. | `shared.SiteId` |
| **requiredShipBy** | The network's deadline, passed to `order-management` unchanged. | `NetworkOrder.RequiredShipBy()`; `HeldOrderRequest.RequiredShipBy` |
| **Acknowledgement window** | 24h. A fact about the counterparty, not configuration. | `networkorder.AcknowledgementWindow` |
| **acknowledgeBy** | `receivedAt + 24h`, fixed at receipt and persisted. | `NetworkOrder.AcknowledgeBy()`; `network_orders.acknowledge_by` |
| **Overdue** | Still `NEW` and past `acknowledgeBy`. | `NetworkOrder.AcknowledgementOverdue(now)` |
| **Held order** | An `order-management` order raised allocated but not released (`releaseOnAllocation: false`, ship-complete). | `FulfillmentPlanner.RaiseHeldOrder`; `contract.HeldOrderRequest/Result` |
| **Feasible** | `order-management`'s verdict that the deadline can be met, read from the presence of a `promiseDate`. | `contract.HeldOrderResult.Feasible` |
| **LocalOrderId** | `order-management`'s `OrderId` for the held order. A persisted one-to-one mapping. | `shared.LocalOrderId`; `network_orders.local_order_id` |
| **Submit** | Decide to accept in full and tell the network. A commitment in flight. | `NetworkOrder.Submit()`; state `SUBMITTED` |
| **Reconcile** | Ask the network whether a submission settled. | `ReconcileSubmittedOrders`; `NetworkGateway.SubmissionStatus` |
| **Acknowledged** | The network has confirmed our acceptance. A settled commitment. | `NetworkOrder.ConfirmAcknowledgement()`; state `ACKNOWLEDGED` |
| **Rejected** | We told the network no. Terminal. | `NetworkOrder.Reject()`; state `REJECTED` |
| **Rejection reason** | Why we refused: untranslatable SKU, infeasible deadline, acknowledgement deadline missed, or submission failed. | `shared.RejectionReason` (`UNTRANSLATABLE_SKU`, `INFEASIBLE_DEADLINE`, `ACKNOWLEDGEMENT_DEADLINE_MISSED`, `SUBMISSION_FAILED`) |
| **Release** | Commit the held order to the floor, after `SUCCESS`. | `FulfillmentPlanner.ReleaseHeldOrder` |
| **Cancel hold** | Abandon the held order and free its reservations. | `FulfillmentPlanner.CancelHeldOrder` |
| **Shipment confirmation** | Tell the network the order shipped, which closes it. | `NetworkOrder.ConfirmShipment()`; `ConfirmNetworkOrderShipment`; state `CONFIRMED` |
| **Deadline at risk** | The reported fact that an order is overdue. Re-fired on every sweep pass. | `shared.AcknowledgementDeadlineAtRisk`; `SweepAcknowledgementDeadlines` |
| **CapabilityOffer** | What we would tell the network we can ship for one `(sku, siteId)`. | `capabilityoffer.CapabilityOffer` |
| **Advertised quantity** | `min(physicalAvailable, throughputFeasible)`, never above physical stock. | `CapabilityOffer.AdvertisedQuantity()`; `capabilityoffer.Compute` |
| **Basis** | Which rule produced the quantity: `PHYSICAL` or `THROUGHPUT_CONSTRAINED`. | `capabilityoffer.Basis`, `BasisPhysical`, `BasisThroughputConstrained` |
| **Physical available / usable inventory** | On-hand minus reservations minus held stock, from `inventory-storage`. | `ports.InventoryAvailability.UsableQuantity` |
| **Next cutoff (CPT)** | The site's next Critical Pull Time and the paths eligible for it. | `ports.ProcessPathCapability.NextCutoff`; `contract.NextCutoff`, `EligiblePath` |
| **Remaining capacity** | What a path can still admit before a cutoff, from `wes-work-planning`. | `ports.PathCapacity.RemainingCapacity` |
| **Throughput feasible** | Remaining capacity summed over the eligible paths with a known figure. | `RecomputeCapabilityOffers.throughputFeasible` |

## Terms whose code name differs

| Business term | Code name | Note |
| --- | --- | --- |
| "Acknowledge" (business) | `Submit` then `ConfirmAcknowledgement`; `Acknowledge()` does both | `Acknowledge()` exists for tests and fixtures. The real flow splits it across `ReceiveNetworkDemand` and `ReconcileSubmittedOrders`. |
| "Acknowledged" event | `NetworkOrderAcknowledged` | Raised at **submission** time (state `SUBMITTED`), not when the order reaches `ACKNOWLEDGED`. |
| "Not acknowledged" error | `ErrNotAcknowledged` | Returned by `LinkLocalOrder` when the state is not `SUBMITTED`/`ACKNOWLEDGED`. The name predates the `SUBMITTED` state. |
| "Deadline at risk" | `AcknowledgementDeadlineAtRisk` | ADR 0001 §6 says "approaching" the deadline. The code fires only once it is already **past** (`AcknowledgementOverdue`). |
| Purchase order / ASIN / `poNumber` | `NetworkRef` / `NetworkProductId` | By design (Hard rule 1). The counterpart's names never appear outside `adapters/outbound/network/`. |
| "Planner" | `FulfillmentPlanner` | This is `order-management` seen through a port. It does not plan anything itself. |
