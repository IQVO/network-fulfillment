package usecases_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/network-fulfillment/internal/application/contract"
	"github.com/claudioed/network-fulfillment/internal/application/usecases"
)

// scriptedRegistrar answers each call from a script (the last answer repeats) and
// records the ASN it was asked for. The ASN number is what the adapter turns into
// the Idempotency-Key (nf-asn-<number>): the same number is the same key.
type scriptedRegistrar struct {
	script []error
	asked  []string
}

func (r *scriptedRegistrar) RegisterAsn(_ context.Context, a contract.AsnRegistration) error {
	r.asked = append(r.asked, a.AsnNumber)
	i := min(len(r.asked)-1, len(r.script)-1)
	return r.script[i]
}

func releasedRefusal(slug string) error {
	return &contract.AsnRefusedError{Problem: slug, Detail: "not yet", Retryable: true}
}

func oneShipment() *intakeFixture {
	return newIntake(shipment("SHP-1", intakeNow, carton("S1", "", contract.CartonMixed, content(gEachA, 1))))
}

// runPass moves the clock past the shipment's backoff and runs one pass.
func (f *intakeFixture) runPass(t *testing.T) usecases.IntakeResult {
	t.Helper()
	f.clock.t = f.clock.t.Add(2 * time.Hour)
	res, err := f.uc.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestIntake_AReleasedRefusalKeepsTheShipmentPendingAndALaterPassRegistersTheSameAsn(t *testing.T) {
	f := oneShipment()
	reg := &scriptedRegistrar{script: []error{releasedRefusal("unknown-sku"), releasedRefusal("concurrent-modification"), nil}}
	f.uc.Registrar = reg

	res, err := f.uc.Execute(context.Background())
	row := f.repo.rows["SHP-1"]
	if err != nil || res.Retrying != 1 || res.Rejected != 0 || row.status != contract.InboundPending {
		t.Fatalf("first pass = %+v, %v, status %s: a released refusal keeps the shipment pending", res, err, row.status)
	}
	if !strings.Contains(row.lastError, "unknown-sku") {
		t.Errorf("last_error = %q: the operator reads the refusal there", row.lastError)
	}
	if res = f.runPass(t); res.Retrying != 1 || row.status != contract.InboundPending {
		t.Fatalf("second pass = %+v, status %s", res, row.status)
	}
	if res = f.runPass(t); res.Registered != 1 || row.status != contract.InboundRegistered {
		t.Fatalf("third pass = %+v, status %s: the later success completes the shipment", res, row.status)
	}
	if len(reg.asked) != 3 || reg.asked[0] != "NF-SHP-1" || reg.asked[1] != reg.asked[0] || reg.asked[2] != reg.asked[0] {
		t.Errorf("asked = %v: every attempt is the same ASN, so the same Idempotency-Key", reg.asked)
	}
}

func TestIntake_AFinalRefusalIsTerminalAndNeverAsksAgain(t *testing.T) {
	f := oneShipment()
	reg := &scriptedRegistrar{script: []error{&contract.AsnRefusedError{Problem: "asn-terminal", Detail: "closed"}, nil}}
	f.uc.Registrar = reg
	res, _ := f.uc.Execute(context.Background())
	row := f.repo.rows["SHP-1"]
	if res.Rejected != 1 || row.status != contract.InboundRejected || !strings.Contains(row.lastError, "asn-terminal") {
		t.Fatalf("pass = %+v, status %s, last_error %q", res, row.status, row.lastError)
	}
	f.runPass(t)
	if len(reg.asked) != 1 {
		t.Errorf("asked %d times: a rejected shipment is not attempted again", len(reg.asked))
	}
}

func TestIntake_TheBoundEndsAForeverReleasedRefusalRejected(t *testing.T) {
	f := oneShipment()
	reg := &scriptedRegistrar{script: []error{releasedRefusal("unknown-sku")}}
	f.uc.Registrar = reg
	f.uc.MaxReleasedRefusalAttempts = 4
	for i := 1; i <= 3; i++ {
		if res := f.runPass(t); res.Retrying != 1 || f.repo.rows["SHP-1"].status != contract.InboundPending {
			t.Fatalf("attempt %d = %+v, status %s: still inside the bound", i, res, f.repo.rows["SHP-1"].status)
		}
	}
	res := f.runPass(t)
	row := f.repo.rows["SHP-1"]
	if res.Rejected != 1 || res.Retrying != 0 || row.status != contract.InboundRejected {
		t.Fatalf("attempt 4 = %+v, status %s: the bound ends it in the terminal state a final refusal has", res, row.status)
	}
	if !strings.Contains(row.lastError, "unknown-sku") || !strings.Contains(row.lastError, "4 attempts") {
		t.Errorf("last_error = %q: it names the refusal and the bound", row.lastError)
	}
	f.runPass(t)
	if len(reg.asked) != 4 {
		t.Errorf("asked %d times, want 4", len(reg.asked))
	}
}

func TestIntake_TheDefaultBoundSpansAboutTheCalleesKeyRetention(t *testing.T) {
	if usecases.DefaultReleasedRefusalAttempts != 30 {
		t.Fatalf("DefaultReleasedRefusalAttempts = %d, want 30", usecases.DefaultReleasedRefusalAttempts)
	}
	var total time.Duration
	for attempt := 1; attempt < usecases.DefaultReleasedRefusalAttempts; attempt++ {
		total += usecases.IntakeBackoff(attempt)
	}
	if total < 24*time.Hour || total > 26*time.Hour {
		t.Errorf("the default bound waits %v in total, want about 24 h (the callee keeps a stored answer 24 h)", total)
	}
	f := oneShipment()
	reg := &scriptedRegistrar{script: []error{releasedRefusal("unknown-sku")}}
	f.uc.Registrar = reg
	for range usecases.DefaultReleasedRefusalAttempts + 5 {
		f.runPass(t)
	}
	if f.repo.rows["SHP-1"].status != contract.InboundRejected || len(reg.asked) != usecases.DefaultReleasedRefusalAttempts {
		t.Errorf("status %s after %d attempts, want REJECTED after exactly %d", f.repo.rows["SHP-1"].status, len(reg.asked), usecases.DefaultReleasedRefusalAttempts)
	}
}

// A transport failure was never a verdict and keeps its behaviour: retried with
// backoff however many times, never ended by the bound of released refusals.
func TestIntake_TheBoundDoesNotEndATransportFailure(t *testing.T) {
	f := oneShipment()
	f.uc.Registrar = &scriptedRegistrar{script: []error{errors.New("connection refused")}}
	f.uc.MaxReleasedRefusalAttempts = 2
	for range 6 {
		f.runPass(t)
	}
	if got := f.repo.rows["SHP-1"].status; got != contract.InboundPending {
		t.Errorf("status = %s, want PENDING: only a released refusal is bounded", got)
	}
}
