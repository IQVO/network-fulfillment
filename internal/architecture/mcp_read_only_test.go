package architecture

import (
	"fmt"
	"go/ast"
	"os"
	"path/filepath"
	"testing"
)

// mcpWriteCalls are the method names that change state somewhere in this
// context: the repositories' Save, the EventPublisher's Publish, a use
// case's Execute, and the raw SQL / store verbs. The MCP adapter calls none
// of them, which is what makes the server's "Read-only access" instructions
// and every tool's ReadOnlyHint true (gap fix D17).
var mcpWriteCalls = map[string]bool{
	"Save": true, "Publish": true, "Execute": true, "Exec": true,
	"Insert": true, "Update": true, "Upsert": true, "Delete": true,
}

// mcpReadOnlyViolations parses one Go file and reports every call to a
// state-changing method and every ReadOnlyHint that is not true.
func mcpReadOnlyViolations(t *testing.T, path string) []string {
	t.Helper()
	fset, f := parseFile(t, path)
	var out []string
	ast.Inspect(f, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.CallExpr:
			if sel, ok := node.Fun.(*ast.SelectorExpr); ok && mcpWriteCalls[sel.Sel.Name] {
				out = append(out, fmt.Sprintf("%s: the MCP adapter calls %s, a state-changing method, while the server "+
					"advertises read-only access and every tool carries ReadOnlyHint=true. FIX: keep MCP tools read-only, or "+
					"drop ReadOnlyHint on the writing tool and the \"Read-only\" server instructions in the same change (new ADR)",
					fset.Position(node.Pos()), sel.Sel.Name))
			}
		case *ast.KeyValueExpr:
			if key, ok := node.Key.(*ast.Ident); ok && key.Name == "ReadOnlyHint" {
				if val, ok := node.Value.(*ast.Ident); ok && val.Name == "false" {
					out = append(out, fmt.Sprintf("%s: ReadOnlyHint: false in an adapter whose server instructions say "+
						"\"Read-only access\". FIX: make the instructions and annotations agree", fset.Position(node.Pos())))
				}
			}
		}
		return true
	})
	return out
}

// TestMCPAdapterStaysReadOnly enforces gap fix D17 for this repository: the
// MCP server's instructions say "Read-only access" and every registered tool
// carries ReadOnlyHint=true, so no non-test file of the MCP inbound adapter
// may call a state-changing method. Agents trust these annotations to decide
// when to ask a human for confirmation.
func TestMCPAdapterStaysReadOnly(t *testing.T) {
	files := goFilesUnder(t, "../adapters/inbound/mcp", false)
	if len(files) == 0 {
		t.Fatal("no MCP adapter sources found; the sensor is looking in the wrong place")
	}
	for _, path := range files {
		for _, v := range mcpReadOnlyViolations(t, path) {
			t.Error(v)
		}
	}
}

// TestMCPReadOnlySensorFailsOnBadFixtures proves the sensor can fail and
// does not flag reads.
func TestMCPReadOnlySensorFailsOnBadFixtures(t *testing.T) {
	cases := []struct {
		name    string
		src     string
		wantBad bool
	}{
		{"repository save", "package x\n\nfunc f() { _ = d.Orders.Save(ctx, o) }\n", true},
		{"event publish", "package x\n\nfunc f() { _ = d.Events.Publish(ctx, e) }\n", true},
		{"use case execute", "package x\n\nfunc f() { _, _ = d.Confirm.Execute(ctx, ref) }\n", true},
		{"annotation false", "package x\n\nvar a = mcp.ToolAnnotations{ReadOnlyHint: false}\n", true},
		{"reads only", "package x\n\nfunc f() { _, _ = d.Orders.FindByRef(ctx, ref); _, _ = d.Offers.ListAll(ctx) }\n", false},
		{"annotation true", "package x\n\nvar a = mcp.ToolAnnotations{ReadOnlyHint: readOnly}\n", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "fixture.go")
			if err := os.WriteFile(p, []byte(tc.src), 0o600); err != nil {
				t.Fatalf("write fixture: %v", err)
			}
			got := mcpReadOnlyViolations(t, p)
			if tc.wantBad != (len(got) > 0) {
				t.Fatalf("violations=%v, want bad=%v", got, tc.wantBad)
			}
		})
	}
}
