package architecture

// Consumer-shutdown dead-letter sensor (fleet escape 18b, 2026-10-14).
//
//   - TestDeadLetterCallsGuardCancellation: every function in a non-test file
//     under internal/adapters/inbound/kafka that SENDS to the dead-letter topic
//     must either reference ctx.Err() (any context-named value) or
//     context.Canceled somewhere in its body, or carry a
//     `//dlq:deterministic <reason>` comment on the call line or the line
//     directly above it.
//
//     The escape (network-fulfillment PR #89, then the fleet audit): a
//     consumer's handler fails BECAUSE the process is stopping (ctx cancelled
//     mid-handle), the retry gives up on the cancelled context, and the loop
//     dead-letters a perfectly good message and commits past it. The guard has
//     to sit in the function that DECIDES to dead-letter: pre-#89 the consume
//     loop (Run) already checked errors.Is(err, context.Canceled) on
//     FetchMessage, yet handleMessage dead-lettered the cancelled message
//     unguarded. So a guard in the caller does NOT count.
//
//     What counts as a dead-letter send: a call whose callee name matches
//     (?i)(dead_?letter|dlq) (deadLetter, dlqPublish, publishDeadLetter,
//     SendToDeadLetter, writeDLQ, ...) or a `send` / `WriteMessages` method on a
//     value whose name matches it (c.dlq().send, c.dlqWriter.WriteMessages),
//     AND that passes a context (an argument named ctx/...Ctx, or a
//     context.X(...) call): talking to the broker needs one, so builders such as
//     newDLQWriter(brokers, topic) or DeadLetterTopic(topic) are not sends.
//
//     A function whose own name matches the pattern (deadLetter, dlqPublish,
//     writeDLQ, a func literal bound to a dlq-named variable or field) IS a
//     dead-letter function: the sends inside it are delegated, and the guard
//     belongs to its callers, which this sensor checks because calling it is a
//     send. Known blind spot: a guard anywhere in the deciding function passes
//     it (the sensor does not prove the guard dominates the call).
//   - TestDeadLetterShutdownSensorBites proves the detector flags the pre-#89
//     shape and passes the fixed one, the annotation, and the delegations.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// dlqsPattern matches a dead-letter function or sink name.
var dlqsPattern = regexp.MustCompile(`(?i)(dead_?letter|dlq)`)

// dlqsAnnotation is the opt-out marker for a provably deterministic poison
// path; it must be followed by a reason.
const dlqsAnnotation = "//dlq:deterministic"

// dlqsNameOf returns the terminal name of an expression (c.dlqWriter ->
// dlqWriter, c.dlq() -> dlq, w[topic] -> w), or "".
func dlqsNameOf(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.SelectorExpr:
		return x.Sel.Name
	case *ast.CallExpr:
		return dlqsNameOf(x.Fun)
	case *ast.IndexExpr:
		return dlqsNameOf(x.X)
	case *ast.ParenExpr:
		return dlqsNameOf(x.X)
	case *ast.StarExpr:
		return dlqsNameOf(x.X)
	}
	return ""
}

// dlqsIsContextArg reports whether e looks like a context: an identifier
// named ctx / ...Ctx, or a context.X(...) call (context.WithoutCancel(ctx)).
func dlqsIsContextArg(e ast.Expr) bool {
	if id, ok := e.(*ast.Ident); ok {
		return strings.HasSuffix(strings.ToLower(id.Name), "ctx")
	}
	call, ok := e.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "context"
}

// dlqsCalleeIsDeadLetter reports whether the callee of call names a
// dead-letter function or a send on a dead-letter sink.
func dlqsCalleeIsDeadLetter(call *ast.CallExpr) bool {
	if sel, ok := call.Fun.(*ast.SelectorExpr); ok && (sel.Sel.Name == "send" || sel.Sel.Name == "WriteMessages") {
		return dlqsPattern.MatchString(dlqsNameOf(sel.X))
	}
	return dlqsPattern.MatchString(dlqsNameOf(call.Fun))
}

// dlqsIsSend reports whether call is a dead-letter send (see the file comment).
func dlqsIsSend(call *ast.CallExpr) bool {
	if !dlqsCalleeIsDeadLetter(call) {
		return false
	}
	for _, a := range call.Args {
		if dlqsIsContextArg(a) {
			return true
		}
	}
	return false
}

// dlqsIsGuard reports whether n is a cancellation guard: <ctx>.Err() or a
// reference to context.Canceled.
func dlqsIsGuard(n ast.Node) bool {
	if call, ok := n.(*ast.CallExpr); ok {
		sel, isSel := call.Fun.(*ast.SelectorExpr)
		return isSel && sel.Sel.Name == "Err" && len(call.Args) == 0 && dlqsIsContextArg(sel.X)
	}
	sel, ok := n.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Canceled" {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "context"
}

// dlqsHasGuard reports whether body references a cancellation guard anywhere.
func dlqsHasGuard(body ast.Node) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if !found && n != nil && dlqsIsGuard(n) {
			found = true
		}
		return !found
	})
	return found
}

// dlqsBoundLits returns the func literals bound to a dlq-named variable,
// field or composite key (loop.deadLetter = func(...) error {...}): they are
// dead-letter functions themselves.
func dlqsBoundLits(body ast.Node) map[*ast.FuncLit]bool {
	out := map[*ast.FuncLit]bool{}
	ast.Inspect(body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			for i, rhs := range x.Rhs {
				if lit, ok := rhs.(*ast.FuncLit); ok && i < len(x.Lhs) && dlqsPattern.MatchString(dlqsNameOf(x.Lhs[i])) {
					out[lit] = true
				}
			}
		case *ast.KeyValueExpr:
			if lit, ok := x.Value.(*ast.FuncLit); ok && dlqsPattern.MatchString(dlqsNameOf(x.Key)) {
				out[lit] = true
			}
		}
		return true
	})
	return out
}

// dlqsAnnotatedLines returns the lines carrying `//dlq:deterministic <reason>`
// with a non-empty reason.
func dlqsAnnotatedLines(fset *token.FileSet, f *ast.File) map[int]bool {
	out := map[int]bool{}
	for _, cg := range f.Comments {
		for _, c := range cg.List {
			rest, ok := strings.CutPrefix(c.Text, dlqsAnnotation)
			if ok && strings.TrimSpace(rest) != "" {
				out[fset.Position(c.Pos()).Line] = true
			}
		}
	}
	return out
}

// dlqsSends returns every dead-letter send in body, skipping the bodies of
// the given dead-letter func literals.
func dlqsSends(body ast.Node, skip map[*ast.FuncLit]bool) []*ast.CallExpr {
	var out []*ast.CallExpr
	ast.Inspect(body, func(n ast.Node) bool {
		if lit, ok := n.(*ast.FuncLit); ok && skip[lit] {
			return false
		}
		if call, ok := n.(*ast.CallExpr); ok && dlqsIsSend(call) {
			out = append(out, call)
		}
		return true
	})
	return out
}

// dlqsUnguardedSends returns how many dead-letter sends f contains (for the
// vacuity check) and the file:line (function) of each unguarded,
// unannotated one in a deciding function.
func dlqsUnguardedSends(fset *token.FileSet, f *ast.File) (sends int, unguarded []string) {
	annotated := dlqsAnnotatedLines(fset, f)
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Body == nil {
			continue
		}
		sends += len(dlqsSends(fd.Body, nil))
		if dlqsPattern.MatchString(fd.Name.Name) || dlqsHasGuard(fd.Body) {
			continue
		}
		for _, call := range dlqsSends(fd.Body, dlqsBoundLits(fd.Body)) {
			line := fset.Position(call.Pos()).Line
			if !annotated[line] && !annotated[line-1] {
				unguarded = append(unguarded, fmt.Sprintf("%s (in %s)", fset.Position(call.Pos()), fd.Name.Name))
			}
		}
	}
	return sends, unguarded
}

func deadLetterShutdownViolation(pos string) string {
	return fmt.Sprintf("ARCHITECTURE VIOLATION (contents rule)\n"+
		"  RULE:    a consumer never dead-letters a message that only cancellation interrupted\n"+
		"  WHAT:    %s: dead-letter send in a function that neither checks ctx.Err() nor context.Canceled\n"+
		"  WHY:     on shutdown the handler fails because ctx was cancelled; dead-lettering then commits a good "+
		"message past the consumer group (fleet escape 18b, network-fulfillment PR #89).\n"+
		"  FIX:     in the function that decides to dead-letter, return ctx.Err() first when ctx.Err() != nil "+
		"(or errors.Is(err, context.Canceled)) so the offset stays uncommitted; only when the error is provably a "+
		"deterministic poison failure, add `//dlq:deterministic <reason>` on the call line. Do NOT loosen this test.\n",
		pos)
}

// dlqsSourceFiles lists every non-test .go file under root.
func dlqsSourceFiles(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(root, func(p string, e os.DirEntry, err error) error {
		if err == nil && !e.IsDir() && strings.HasSuffix(p, ".go") && !strings.HasSuffix(p, "_test.go") {
			out = append(out, p)
		}
		return err
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return out
}

func TestDeadLetterCallsGuardCancellation(t *testing.T) {
	root := filepath.Join("..", "..", "internal", "adapters", "inbound", "kafka")
	if _, err := os.Stat(root); err != nil {
		t.Skip("no internal/adapters/inbound/kafka in this repo: no consumer to check")
	}
	total := 0
	for _, p := range dlqsSourceFiles(t, root) {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, p, nil, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", p, err)
		}
		sends, unguarded := dlqsUnguardedSends(fset, f)
		total += sends
		for _, pos := range unguarded {
			t.Errorf("%s", deadLetterShutdownViolation(pos))
		}
	}
	if total == 0 {
		t.Fatal("no dead-letter send found under internal/adapters/inbound/kafka; the consumers here dead-letter " +
			"poison messages, so the sensor's name pattern no longer matches them")
	}
}

// dlqsPre89 is network-fulfillment's analytics consumer BEFORE PR #89: Run
// checks context.Canceled on FetchMessage, but handleMessage dead-letters
// whatever error the retry returns, cancellation included.
const dlqsPre89 = `package kafka
import ("context"; "errors"; kafkago "github.com/segmentio/kafka-go")
func (c *C) Run(ctx context.Context) error {
	for {
		msg, err := c.Reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) { return nil }
			return err
		}
		if err := c.handleMessage(ctx, msg); err != nil { return err }
	}
}
func (c *C) handleMessage(ctx context.Context, msg kafkago.Message) error {
	if _, err := decode(msg.Value); err != nil {
		if dlqErr := c.dlqPublish(ctx, msg, err); dlqErr != nil { return dlqErr }
		return c.commit(ctx, msg)
	}
	if err := c.applyWithRetry(ctx, msg); err != nil {
		return c.deadLetter(ctx, msg, "apply", err)
	}
	return c.commit(ctx, msg)
}
func (c *C) deadLetter(ctx context.Context, msg kafkago.Message, phase string, cause error) error {
	if err := c.dlqPublish(ctx, msg, cause); err != nil { return err }
	return c.commit(ctx, msg)
}
func (c *C) dlqPublish(ctx context.Context, msg kafkago.Message, cause error) error {
	return c.dlqWriter.WriteMessages(ctx, kafkago.Message{Key: msg.Key, Value: msg.Value})
}`

// dlqsPost89 is the same consumer after PR #89: handleMessage returns
// ctx.Err() before it dead-letters.
var dlqsPost89 = strings.Replace(dlqsPre89,
	`return c.deadLetter(ctx, msg, "apply", err)`,
	`if ctx.Err() != nil { return ctx.Err() }
		return c.deadLetter(ctx, msg, "apply", err)`, 1)

type dlqsCase struct {
	name          string
	src           string
	wantSends     int
	wantUnguarded int
}

func dlqsCases() []dlqsCase {
	return []dlqsCase{
		{"pre-#89 shape is flagged (guard in the consume loop does not count)", dlqsPre89, 4, 2},
		{"post-#89 shape passes", dlqsPost89, 4, 0},
		{"errors.Is(err, context.Canceled) is a guard", `package kafka
import ("context"; "errors")
func (c *C) handle(ctx context.Context, m M) error {
	err := c.apply(ctx, m)
	if errors.Is(err, context.Canceled) { return err }
	return c.SendToDeadLetter(ctx, m, err)
}`, 1, 0},
		{"annotated deterministic send passes, a reasonless annotation does not", `package kafka
import "context"
func (c *C) handle(ctx context.Context, m M) error {
	if err := decode(m); err != nil {
		//dlq:deterministic a payload that does not decode never decodes
		return c.dlq().send(ctx, m, err)
	}
	return c.dlq().send(ctx, m, nil) //dlq:deterministic
}`, 2, 1},
		{"a sink send without a guard is flagged", `package kafka
import "context"
func (c *TransferFactConsumer) apply(ctx context.Context, m M) error {
	if err := c.svc.Apply(ctx, m); err != nil { return c.dlq().send(ctx, m, err) }
	return nil
}`, 1, 1},
		{"a dlq-bound func literal delegates; the loop that calls it must guard", `package kafka
import "context"
func NewC(b []string) *C {
	loop := &consumeLoop{}
	loop.deadLetter = func(ctx context.Context, m M, cause error) error { return publishDeadLetter(ctx, w, m, cause) }
	return &C{loop: loop}
}
func (l *consumeLoop) run(ctx context.Context, m M) error {
	if err := l.handle(ctx, m); err != nil { return l.deadLetter(ctx, m, err) }
	return nil
}`, 2, 1},
		{"builders and getters without a context are not sends", `package kafka
func NewC(b []string, topic string) *C {
	return &C{DLQ: newDLQWriter(b, topic+DLQSuffix), Topic: DeadLetterTopic(topic), sink: c.dlq()}
}`, 0, 0},
	}
}

func TestDeadLetterShutdownSensorBites(t *testing.T) {
	if dlqsPost89 == dlqsPre89 {
		t.Fatal("fixture edit did not apply: the post-#89 fixture equals the pre-#89 one")
	}
	for _, tc := range dlqsCases() {
		t.Run(tc.name, func(t *testing.T) {
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, "src.go", tc.src, parser.ParseComments)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			sends, unguarded := dlqsUnguardedSends(fset, f)
			if sends != tc.wantSends || len(unguarded) != tc.wantUnguarded {
				t.Fatalf("sends=%d unguarded=%v, want %d send(s) and %d unguarded", sends, unguarded, tc.wantSends, tc.wantUnguarded)
			}
		})
	}
}
