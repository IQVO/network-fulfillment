package architecture

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"testing"

	archgo "github.com/arch-go/arch-go/api"
	"github.com/arch-go/arch-go/api/configuration"
)

// TestA2AAdapterDependencyRule encodes ADR 0030: the A2A inbound adapter is
// additive, never load-bearing for the OLTP composition root, exactly like the
// MCP adapter (ADR 0002 / TestMCPAdapterDependencyRule). It may depend only on
// the application and domain layers (never on outbound adapters, never on
// cmd), and nothing else in this codebase may depend on it.
func TestA2AAdapterDependencyRule(t *testing.T) {
	moduleInfo := configuration.Load(modulePath)

	t.Run("a2a adapter depends only on application and domain", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{
				{
					Package: "**.internal.adapters.inbound.a2a.**",
					ShouldOnlyDependsOn: &configuration.Dependencies{
						Internal: []string{
							"**.internal.adapters.inbound.a2a.**",
							"**.internal.application.**",
							"**.internal.domain.**",
						},
					},
				},
			},
		})
		assertArchGoPasses(t, result)
	})

	t.Run("nothing else depends on the a2a adapter", func(t *testing.T) {
		result := archgo.CheckArchitecture(moduleInfo, configuration.Config{
			DependenciesRules: []*configuration.DependenciesRule{
				{
					Package: "**.internal.**",
					ShouldNotDependsOn: &configuration.Dependencies{
						Internal: []string{"**.internal.adapters.inbound.a2a.**"},
					},
				},
			},
		})
		assertArchGoPasses(t, result)
	})
}

// a2aClientImport is the SDK package an OUTBOUND A2A client would import. This
// context only SERVES A2A; delegating out would be a new decision (new ADR),
// not a quiet import.
const a2aClientImport = "github.com/a2aproject/a2a-go/v2/a2aclient"

// a2aClientViolations reports a non-test file that imports the A2A client.
func a2aClientViolations(t *testing.T, path string) []string {
	t.Helper()
	fset, f := parseFile(t, path)
	var out []string
	for _, imp := range f.Imports {
		if strings.Trim(imp.Path.Value, `"`) == a2aClientImport {
			out = append(out, fmt.Sprintf("%s: imports the A2A client (%s). This context is an A2A SERVER only; an outbound A2A client needs its own ADR", fset.Position(imp.Pos()), a2aClientImport))
		}
	}
	return out
}

// TestNoOutboundA2AClient keeps the A2A SDK's client out of every runtime
// package: only tests may drive the server through it.
func TestNoOutboundA2AClient(t *testing.T) {
	var files []string
	for _, root := range []string{"../../cmd", "../../internal"} {
		files = append(files, goFilesUnder(t, root, false)...)
	}
	if len(files) == 0 {
		t.Fatal("no sources found; the sensor is looking in the wrong place")
	}
	for _, path := range files {
		for _, v := range a2aClientViolations(t, path) {
			t.Error(v)
		}
	}
}

func TestNoOutboundA2AClientSensorFailsOnBadFixtures(t *testing.T) {
	cases := []struct {
		name    string
		src     string
		wantBad bool
	}{
		{"client import", "package x\n\nimport _ \"" + a2aClientImport + "\"\n", true},
		{"server imports", "package x\n\nimport (\n\t_ \"github.com/a2aproject/a2a-go/v2/a2a\"\n\t_ \"github.com/a2aproject/a2a-go/v2/a2asrv\"\n)\n", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := a2aClientViolations(t, writeFixture(t, tc.src))
			if tc.wantBad != (len(got) > 0) {
				t.Fatalf("violations=%v, want bad=%v", got, tc.wantBad)
			}
		})
	}
}

// a2aForbiddenDepsTypes are type-name fragments that make a Deps field a WRITE
// dependency: a use case, an event publisher, a unit of work or an outbox.
var a2aForbiddenDepsTypes = []string{"usecases.", "EventPublisher", "UnitOfWork", "Outbox"}

// a2aReadOnlyViolations parses one Go file and reports every call to a
// state-changing method and every Deps field typed as a write dependency.
func a2aReadOnlyViolations(t *testing.T, path string) []string {
	t.Helper()
	fset, f := parseFile(t, path)
	var out []string
	ast.Inspect(f, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.CallExpr:
			out = append(out, writeCallViolation(fset, node)...)
		case *ast.TypeSpec:
			out = append(out, depsFieldViolations(fset, node)...)
		}
		return true
	})
	return out
}

func writeCallViolation(fset *token.FileSet, call *ast.CallExpr) []string {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || !mcpWriteCalls[sel.Sel.Name] {
		return nil
	}
	return []string{fmt.Sprintf("%s: the A2A adapter calls %s, a state-changing method, but its card advertises read-only skills and the MCP surface it mirrors is read-only. FIX: keep A2A skills read-only, or add the write to the MCP surface first (new ADR) and then here", fset.Position(call.Pos()), sel.Sel.Name)}
}

func depsFieldViolations(fset *token.FileSet, spec *ast.TypeSpec) []string {
	st, ok := spec.Type.(*ast.StructType)
	if !ok || spec.Name.Name != "Deps" {
		return nil
	}
	var out []string
	for _, field := range st.Fields.List {
		typ := types.ExprString(field.Type)
		for _, bad := range a2aForbiddenDepsTypes {
			if strings.Contains(typ, bad) {
				out = append(out, fmt.Sprintf("%s: Deps carries a write dependency (%s): the A2A agent is read-only", fset.Position(field.Pos()), typ))
			}
		}
	}
	return out
}

// TestA2AAdapterStaysReadOnly enforces the write gate: an A2A skill may change
// state only if the context's MCP surface already offers that write. The MCP
// charter here is read-only (TestMCPAdapterStaysReadOnly), so no non-test file
// of the A2A adapter may call a state-changing method or hold a write
// dependency. Delegation must never widen what this context exposes.
func TestA2AAdapterStaysReadOnly(t *testing.T) {
	files := goFilesUnder(t, "../adapters/inbound/a2a", false)
	if len(files) == 0 {
		t.Fatal("no A2A adapter sources found; the sensor is looking in the wrong place")
	}
	for _, path := range files {
		for _, v := range a2aReadOnlyViolations(t, path) {
			t.Error(v)
		}
	}
}

func TestA2AReadOnlySensorFailsOnBadFixtures(t *testing.T) {
	cases := []struct {
		name    string
		src     string
		wantBad bool
	}{
		{"repository save", "package x\n\nfunc f() { _ = d.Orders.Save(ctx, o) }\n", true},
		{"event publish", "package x\n\nfunc f() { _ = d.Events.Publish(ctx, e) }\n", true},
		{"use case execute", "package x\n\nfunc f() { _, _ = d.Confirm.Execute(ctx, ref) }\n", true},
		{"use case in Deps", "package x\n\ntype Deps struct {\n\tConfirm *usecases.ConfirmNetworkOrderShipment\n}\n", true},
		{"publisher in Deps", "package x\n\ntype Deps struct {\n\tEvents ports.EventPublisher\n}\n", true},
		{"unit of work in Deps", "package x\n\ntype Deps struct {\n\tUoW ports.UnitOfWork\n}\n", true},
		{"reads only", "package x\n\ntype Deps struct {\n\tOrders ports.NetworkOrderRepo\n\tOffers ports.CapabilityOfferRepo\n}\n\nfunc f() { _, _ = d.Orders.FindByRef(ctx, ref); _, _ = d.Offers.ListAll(ctx) }\n", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := a2aReadOnlyViolations(t, writeFixture(t, tc.src))
			if tc.wantBad != (len(got) > 0) {
				t.Fatalf("violations=%v, want bad=%v", got, tc.wantBad)
			}
		})
	}
}

func writeFixture(t *testing.T, src string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "fixture.go")
	if err := os.WriteFile(p, []byte(src), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return p
}
