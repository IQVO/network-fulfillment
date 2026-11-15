package usecases

import "errors"

// ErrPurchaseOrderNotFound is returned by the keyed read of a purchase order
// (ADR 0036) when this context holds no network order for that purchase-order
// number: the number was never recorded, or the order it was recorded for has
// not been stored yet. It is not ErrOrderNotFound: the caller named a purchase
// order, not a NetworkRef, and the REST surface answers with its own problem
// type.
var ErrPurchaseOrderNotFound = errors.New("no network order was received for this purchase order number")
