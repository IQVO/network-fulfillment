package retailnetwork

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

// nodesFile is the wire shape of NETWORK_NODES_FILE (ADR 0026 Decision 7): what
// the network needs to register each of our sites as a vendor node. It is
// declarative configuration, loaded the way the product dictionary is (ADR
// 0013): strict JSON, every failure returned, nothing guessed.
//
//	{"nodes":[{"siteCode":"site-1","nodeType":"FULFILLMENT_CENTER","subtype":"SORTABLE",
//	  "regionId":"SP","timezone":"America/Sao_Paulo","postalCode":"01000-000",
//	  "countryCode":"BR","lat":-23.55,"lon":-46.63}]}
//
// subtype, lat and lon are optional (lat and lon together or not at all).
type nodesFile struct {
	Nodes []nodeEntry `json:"nodes"`
}

type nodeEntry struct {
	SiteCode    string   `json:"siteCode"`
	NodeType    string   `json:"nodeType"`
	Subtype     string   `json:"subtype"`
	RegionID    string   `json:"regionId"`
	Timezone    string   `json:"timezone"`
	PostalCode  string   `json:"postalCode"`
	CountryCode string   `json:"countryCode"`
	Lat         *float64 `json:"lat"`
	Lon         *float64 `json:"lon"`
}

// LoadNodesFile reads the node definitions of path.
//
// A typo'd key, a missing required field, a half-given coordinate or a site
// listed twice is a boot error naming the entry: a definition that silently
// failed to load would leave a site that is never registered, and the
// symptom (no demand ever arrives for it) points at the network, not at this
// file. The values themselves (node type and subtype enums, formats) are the
// network's to judge; a registration it refuses is recorded as FAILURE in the
// submission ledger.
func LoadNodesFile(path string) ([]NodeDefinition, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // path is operator-controlled config (a boot-time env value), not user input.
	if err != nil {
		return nil, fmt.Errorf("read nodes file: %w", err)
	}
	var file nodesFile
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&file); err != nil {
		return nil, fmt.Errorf("parse nodes file %s: %w", path, err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("parse nodes file %s: unexpected content after the JSON document", path)
	}
	out := make([]NodeDefinition, 0, len(file.Nodes))
	seen := make(map[string]bool, len(file.Nodes))
	for i, e := range file.Nodes {
		if err := e.validate(); err != nil {
			return nil, fmt.Errorf("nodes file %s: node %d (%q): %w", path, i, e.SiteCode, err)
		}
		if seen[e.SiteCode] {
			return nil, fmt.Errorf("nodes file %s: node %d: site %q is defined twice", path, i, e.SiteCode)
		}
		seen[e.SiteCode] = true
		out = append(out, NodeDefinition(e))
	}
	return out, nil
}

func (e nodeEntry) validate() error {
	for _, f := range []struct{ name, value string }{
		{"siteCode", e.SiteCode}, {"nodeType", e.NodeType}, {"regionId", e.RegionID},
		{"timezone", e.Timezone}, {"postalCode", e.PostalCode}, {"countryCode", e.CountryCode},
	} {
		if f.value == "" {
			return fmt.Errorf("%s is required", f.name)
		}
	}
	if (e.Lat == nil) != (e.Lon == nil) {
		return errors.New("lat and lon go together: give both or neither")
	}
	if e.Lat != nil && (*e.Lat < -90 || *e.Lat > 90) {
		return fmt.Errorf("lat %v is outside -90..90", *e.Lat)
	}
	if e.Lon != nil && (*e.Lon < -180 || *e.Lon > 180) {
		return fmt.Errorf("lon %v is outside -180..180", *e.Lon)
	}
	return nil
}
