package architecture

// Zone-database sensor (ADR 0031 §2): the process-path capability cache reads
// every CPT cutoff's local time in the schedule's IANA timezone. A binary that
// runs the cache must embed the zone database (a blank import of time/tzdata),
// or on a base image without /usr/share/zoneinfo every zone is "unknown" and
// silently read as UTC: the defect ADR 0031 fixes.
//
//   - TestEveryCmdRunningTheScheduleCacheEmbedsTZData walks cmd/* and fails for a
//     binary directory whose non-test files import processpathcache but none
//     imports time/tzdata.
//   - TestTZDataSensorBites proves the rule on fixtures.

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const scheduleCacheSuffix = "/internal/adapters/outbound/processpathcache"

// cmdsMissingTZData returns the binaries (dir -> import paths of its non-test
// files) that use the schedule cache without embedding the zone database.
func cmdsMissingTZData(imports map[string][]string) []string {
	var missing []string
	for dir, paths := range imports {
		usesCache, embeds := false, false
		for _, p := range paths {
			usesCache = usesCache || strings.HasSuffix(p, scheduleCacheSuffix)
			embeds = embeds || p == "time/tzdata"
		}
		if usesCache && !embeds {
			missing = append(missing, dir)
		}
	}
	sort.Strings(missing)
	return missing
}

func TestEveryCmdRunningTheScheduleCacheEmbedsTZData(t *testing.T) {
	cmdRoot := filepath.Join("..", "..", "cmd")
	entries, err := os.ReadDir(cmdRoot)
	if err != nil {
		t.Fatalf("read cmd/: %v", err)
	}
	imports := map[string][]string{}
	usingCache := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		for _, f := range goFilesUnder(t, filepath.Join(cmdRoot, e.Name()), false) {
			parsed, err := parser.ParseFile(token.NewFileSet(), f, nil, parser.ImportsOnly)
			if err != nil {
				t.Fatalf("parse %s: %v", f, err)
			}
			for _, imp := range parsed.Imports {
				p, _ := strconv.Unquote(imp.Path.Value)
				imports[e.Name()] = append(imports[e.Name()], p)
				if strings.HasSuffix(p, scheduleCacheSuffix) {
					usingCache++
				}
			}
		}
	}
	if usingCache == 0 {
		t.Fatal("no cmd imports processpathcache: the sensor would pass vacuously")
	}
	if missing := cmdsMissingTZData(imports); len(missing) > 0 {
		t.Fatalf("cmd/%v run the CPT schedule cache without embedding the zone database: add a file with `import _ \"time/tzdata\"` (ADR 0031)", missing)
	}
}

func TestTZDataSensorBites(t *testing.T) {
	cache := "github.com/claudioed/network-fulfillment" + scheduleCacheSuffix
	got := cmdsMissingTZData(map[string][]string{
		"uses-cache-no-tz":  {"fmt", cache},
		"uses-cache-and-tz": {cache, "time/tzdata"},
		"no-cache":          {"fmt", "time"},
		"look-alike":        {cache + "x", "time"},
	})
	if len(got) != 1 || got[0] != "uses-cache-no-tz" {
		t.Fatalf("missing = %v, want [uses-cache-no-tz]", got)
	}
}
