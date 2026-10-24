package retailnetwork

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeNodes(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "nodes.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

const fullNode = `{"siteCode":"site-1","nodeType":"FULFILLMENT_CENTER","subtype":"SORTABLE","regionId":"SP",` +
	`"timezone":"America/Sao_Paulo","postalCode":"01000-000","countryCode":"BR","lat":-23.55,"lon":-46.63}`

func TestLoadNodesFile_Valid(t *testing.T) {
	minimal := `{"siteCode":"site-2","nodeType":"RETURNS_CENTER","regionId":"RJ","timezone":"America/Sao_Paulo","postalCode":"20000-000","countryCode":"BR"}`
	got, err := LoadNodesFile(writeNodes(t, `{"nodes":[`+fullNode+`,`+minimal+`]}`))
	if err != nil {
		t.Fatalf("LoadNodesFile: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("nodes = %d, want 2", len(got))
	}
	first := got[0]
	if first.SiteCode != "site-1" || first.NodeType != "FULFILLMENT_CENTER" || first.Subtype != "SORTABLE" || first.RegionID != "SP" ||
		first.Timezone != "America/Sao_Paulo" || first.PostalCode != "01000-000" || first.CountryCode != "BR" {
		t.Fatalf("first node = %+v", first)
	}
	if first.Lat == nil || *first.Lat != -23.55 || first.Lon == nil || *first.Lon != -46.63 {
		t.Fatalf("first node coordinates = %v, %v", first.Lat, first.Lon)
	}
	if got[1].Subtype != "" || got[1].Lat != nil || got[1].Lon != nil {
		t.Fatalf("optional fields of the second node = %+v, want zero values", got[1])
	}
}

func TestLoadNodesFile_EmptyListIsValid(t *testing.T) {
	got, err := LoadNodesFile(writeNodes(t, `{"nodes":[]}`))
	if err != nil || len(got) != 0 {
		t.Fatalf("got %v, %v; want no nodes and no error", got, err)
	}
}

func TestLoadNodesFile_Refusals(t *testing.T) {
	cases := []struct {
		name, body, want string
	}{
		{"not JSON", `nodes: []`, "parse nodes file"},
		{"unknown key", `{"nodes":[],"sites":[]}`, "unknown field"},
		{"unknown node key", `{"nodes":[` + strings.Replace(fullNode, `"timezone"`, `"timezones"`, 1) + `]}`, "unknown field"},
		{"trailing content", `{"nodes":[]} {"nodes":[]}`, "after the JSON document"},
		{"missing siteCode", `{"nodes":[` + strings.Replace(fullNode, `"siteCode":"site-1"`, `"siteCode":""`, 1) + `]}`, "siteCode is required"},
		{"missing nodeType", `{"nodes":[` + strings.Replace(fullNode, `"nodeType":"FULFILLMENT_CENTER"`, `"nodeType":""`, 1) + `]}`, "nodeType is required"},
		{"missing regionId", `{"nodes":[` + strings.Replace(fullNode, `"regionId":"SP"`, `"regionId":""`, 1) + `]}`, "regionId is required"},
		{"missing timezone", `{"nodes":[` + strings.Replace(fullNode, `"timezone":"America/Sao_Paulo"`, `"timezone":""`, 1) + `]}`, "timezone is required"},
		{"missing postalCode", `{"nodes":[` + strings.Replace(fullNode, `"postalCode":"01000-000"`, `"postalCode":""`, 1) + `]}`, "postalCode is required"},
		{"missing countryCode", `{"nodes":[` + strings.Replace(fullNode, `"countryCode":"BR"`, `"countryCode":""`, 1) + `]}`, "countryCode is required"},
		{"lat without lon", `{"nodes":[` + strings.Replace(fullNode, `,"lon":-46.63`, ``, 1) + `]}`, "lat and lon go together"},
		{"lon without lat", `{"nodes":[` + strings.Replace(fullNode, `,"lat":-23.55`, ``, 1) + `]}`, "lat and lon go together"},
		{"lat out of range", `{"nodes":[` + strings.Replace(fullNode, `"lat":-23.55`, `"lat":-123.55`, 1) + `]}`, "outside -90..90"},
		{"lon out of range", `{"nodes":[` + strings.Replace(fullNode, `"lon":-46.63`, `"lon":246.63`, 1) + `]}`, "outside -180..180"},
		{"duplicate site", `{"nodes":[` + fullNode + `,` + fullNode + `]}`, "defined twice"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadNodesFile(writeNodes(t, tc.body))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want one containing %q", err, tc.want)
			}
		})
	}
}

func TestLoadNodesFile_MissingFile(t *testing.T) {
	_, err := LoadNodesFile(filepath.Join(t.TempDir(), "absent.json"))
	if err == nil || !strings.Contains(err.Error(), "read nodes file") {
		t.Fatalf("err = %v, want a read error", err)
	}
}
