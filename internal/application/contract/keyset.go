package contract

// Keyset is the position a page starts after (fleet REST guideline section
// 3.4): the last returned row's sort key and id. Repositories order by
// (Sort, ID) so rows sharing a sort key are never skipped or repeated. For
// network orders Sort is acknowledgeBy (RFC 3339, nanoseconds, UTC) and ID
// the networkRef; for capability offers Sort is the sku and ID the siteId.
type Keyset struct {
	Sort string
	ID   string
}
