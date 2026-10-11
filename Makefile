# Makefile — the local quality gate template (warehouse-harness-template v1).
#
# Every target below mirrors a sensor in .github/workflows/ci.yml, so the
# same feedback CI gives you post-push is available locally, pre-commit.
# This is the "keep quality left" idea from harness engineering: shift the
# sensors left so an agent (or a human) can self-correct before code leaves
# the machine.
#
# INSTANTIATING THIS TEMPLATE: replace every networkfulfillment, network-fulfillment
# and networkorder placeholder -- `scripts/new-service.sh` does
# this substitution for you. See HARNESS.md.

GO                 ?= go
GOLANGCI_LINT      ?= golangci-lint
GOLANGCI_VERSION   := v2.14.0
GREMLINS           ?= gremlins
GREMLINS_VERSION   := v0.6.0
GOVULNCHECK        ?= govulncheck

COVERAGE_OUT       := coverage.out
COVERAGE_PKGS      := ./internal/domain/...,./internal/application/...
COVERAGE_THRESHOLD := 90

# The fast mutation subset — kept in sync with the `mutation-fast` CI job.
# Thresholds/workers/timeout live in .gremlins.yaml so both use identical
# settings. Pick your richest, most behaviourally-dense aggregate here —
# not necessarily the biggest package, the one with the most branching
# domain logic (see HARNESS.md's mutation-testing section).
MUTATION_FAST_PKG  := ./internal/domain/networkorder
# The exhaustive scheduled run — kept in sync with the `mutation` CI job.
MUTATION_FULL_PKG  := ./internal/domain

.DEFAULT_GOAL := help

.PHONY: help build vet fmt fmt-check lint test coverage integration bdd arch-test mutation mutation-full vuln check check-all

help:
	@echo "networkfulfillment — local quality gate (targets mirror .github/workflows/ci.yml)"
	@echo ""
	@echo "  help           Print this list of targets (default target)"
	@echo "  build          go build ./..."
	@echo "  vet            go vet ./..."
	@echo "  fmt            gofmt -w . — format the tree in place"
	@echo "  fmt-check      Fail if gofmt -l . is non-empty (the CI-style check)"
	@echo "  lint           golangci-lint run ./... (pinned $(GOLANGCI_VERSION) in CI)"
	@echo "  test           go test ./... -race — unit + httptest + bdd, no DB needed"
	@echo "  coverage       CI coverage command + the $(COVERAGE_THRESHOLD)% gate"
	@echo "  integration    go test -tags=integration ./... -race -count=1"
	@echo "                 (needs a running Postgres; not in check)"
	@echo "  bdd            go test ./... -run TestFeatures -v — godog/Gherkin acceptance"
	@echo "  arch-test      go test ./internal/architecture/... -v — hexagonal fitness"
	@echo "  mutation       gremlins on $(MUTATION_FAST_PKG) — the fast blocking subset"
	@echo "  mutation-full  gremlins on $(MUTATION_FULL_PKG) — the exhaustive scheduled run"
	@echo "  vuln           govulncheck ./... — supply-chain / stdlib CVE sensor"
	@echo ""
	@echo "  check          FAST pre-commit bundle: fmt-check vet build lint test"
	@echo "  check-all      check + coverage arch-test bdd — run this before pushing"

build:
	$(GO) build ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -w .

fmt-check:
	@files=$$(gofmt -l .); \
	if [ -n "$$files" ]; then \
		echo "gofmt: the following files are not formatted:"; \
		echo "$$files" | sed 's/^/  /'; \
		echo "run 'make fmt' to fix them"; \
		exit 1; \
	fi; \
	echo "gofmt: clean"

lint:
	@if ! command -v $(GOLANGCI_LINT) >/dev/null 2>&1; then \
		echo "golangci-lint is not installed (or not on PATH)."; \
		echo "Install the exact version CI pins:"; \
		echo "  go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_VERSION)"; \
		exit 1; \
	fi
	$(GOLANGCI_LINT) run ./...

test:
	$(GO) test ./... -race

coverage:
	$(GO) test ./... -race -coverprofile=$(COVERAGE_OUT) -coverpkg=$(COVERAGE_PKGS)
	@COVERAGE=$$($(GO) tool cover -func=$(COVERAGE_OUT) | awk '/^total:/ {print $$3}' | tr -d '%'); \
	echo "Coverage: $${COVERAGE}% (gate: $(COVERAGE_THRESHOLD)%)"; \
	if awk -v c="$$COVERAGE" -v t="$(COVERAGE_THRESHOLD)" 'BEGIN { exit !(c < t) }'; then \
		echo "coverage $${COVERAGE}% is below the $(COVERAGE_THRESHOLD)% gate"; \
		exit 1; \
	fi

# Needs only Docker: every integration test boots its own Postgres/Kafka via
# testcontainers (no DATABASE_URL; a fitness test fails CI if a Postgres
# integration test reintroduces an env-gated skip).
# Deliberately NOT part of `check` / `check-all`.
integration:
	$(GO) test -tags=integration ./... -race -count=1

bdd:
	$(GO) test ./... -run TestFeatures -v

arch-test:
	$(GO) test ./internal/architecture/... -v

mutation:
	@if ! command -v $(GREMLINS) >/dev/null 2>&1; then \
		echo "gremlins is not installed (or not on PATH)."; \
		echo "Install the exact version CI pins:"; \
		echo "  go install github.com/go-gremlins/gremlins/cmd/gremlins@$(GREMLINS_VERSION)"; \
		exit 1; \
	fi
	$(GREMLINS) unleash $(MUTATION_FAST_PKG)

mutation-full:
	@if ! command -v $(GREMLINS) >/dev/null 2>&1; then \
		echo "gremlins is not installed (or not on PATH)."; \
		echo "Install the exact version CI pins:"; \
		echo "  go install github.com/go-gremlins/gremlins/cmd/gremlins@$(GREMLINS_VERSION)"; \
		exit 1; \
	fi
	$(GREMLINS) unleash $(MUTATION_FULL_PKG) --workers 1 --timeout-coefficient 30

vuln:
	@if ! command -v $(GOVULNCHECK) >/dev/null 2>&1; then \
		echo "govulncheck is not installed (or not on PATH)."; \
		echo "Install it with:"; \
		echo "  go install golang.org/x/vuln/cmd/govulncheck@latest"; \
		exit 1; \
	fi
	$(GOVULNCHECK) ./...

# The fast self-correction loop: run this after every change, before committing.
check: fmt-check vet build lint test

# The fuller gate a human runs before pushing. Still excludes `integration`
# (needs a DB) and `mutation` (slow).
check-all: check coverage arch-test bdd

# --- agent harness (harness-template v3) -----------------------------------
.PHONY: check-fast guide-lint harness-test
# Fast local gate used by the agent Stop hook: format, vet, fitness tests, and the tests of
# the packages changed vs HEAD. The full gate stays `make check` / `make check-all`.
check-fast: fmt-check vet arch-test
	@pkgs="$$(python3 scripts/harness/hook.py changed-pkgs)"; \
	if [ -n "$$pkgs" ]; then go test $$pkgs; else echo "check-fast: no changed Go packages"; fi

guide-lint: ## lint agent guides: skills load, references resolve, context budget
	python3 scripts/harness/guide_lint.py

harness-test: ## unit-test the agent hooks (pre/post/stop)
	python3 scripts/harness/test_hook.py
