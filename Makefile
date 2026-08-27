GO_PROJECT_NAME := s3d
SHELL := /bin/bash

export GOFIPS140 := v1.0.0

# Quiet-mode filters (active when QUIET=1, set by preflight via recursive make)
# Note: grep pipelines use PIPESTATUS[0] so the exit status of `go test`
# propagates through the filter — otherwise a test failure is swallowed by
# grep's own (success) exit code and preflight prints "passed" on red.
ifdef QUIET
  _Q     = @
  _COVQ  = 2>&1 | { grep -Ev '^\s*(ok|PASS|\?|=== RUN|--- PASS:)\s' | grep -v 'coverage: 0\.0%' || true; }; exit $${PIPESTATUS[0]}
  _RACEQ = 2>&1 | { grep -Ev '^\s*(ok|PASS|\?|=== RUN|--- PASS:)\s' || true; }; exit $${PIPESTATUS[0]}
else
  _Q     =
  _COVQ  =
  _RACEQ =
endif

# Generate self-signed dev TLS certs (no-op if they already exist)
certs:
	@mkdir -p certs
	@test -f certs/server.pem || openssl req -x509 -newkey rsa:2048 -nodes \
		-keyout certs/server.key -out certs/server.pem \
		-days 3650 -subj '/CN=localhost' \
		-addext 'subjectAltName=DNS:localhost,IP:127.0.0.1,IP:10.11.12.1,IP:10.11.12.2,IP:10.11.12.3,IP:10.11.12.4,IP:10.11.12.5,IP:10.11.12.6,IP:10.11.12.7'

build:
	$(MAKE) go_build

# GO commands
go_build:
	@echo -e "\n....Building $(GO_PROJECT_NAME)"
	go build -ldflags "-s -w" -o ./bin/s3d ./cmd/s3d

# Preflight — runs the same checks as GitHub Actions (lint + security + tests).
# Use this before committing to catch CI failures locally.
preflight:
	@$(MAKE) --no-print-directory QUIET=1 lint govulncheck test-cover diff-coverage test-integration
	@echo -e "\n ✅ Preflight passed — safe to commit."

# Run unit tests
test:
	@echo -e "\n....Running tests for $(GO_PROJECT_NAME)...."
	LOG_IGNORE=1 go test -timeout 120s ./...

# Run unit tests with coverage profile
COVERPROFILE ?= coverage.out
test-cover:
	@echo -e "\n....Running tests with coverage for $(GO_PROJECT_NAME)...."
	$(_Q)LOG_IGNORE=1 go test -timeout 120s -coverprofile=$(COVERPROFILE) -covermode=atomic ./... $(_COVQ)
	@scripts/check-coverage.sh $(COVERPROFILE) $(QUIET)

# Run unit tests with race detector
test-race:
	@echo -e "\n....Running tests with race detector for $(GO_PROJECT_NAME)...."
	$(_Q)LOG_IGNORE=1 go test -race -timeout 300s ./... $(_RACEQ)

# Run tests behind the 'integration' build tag, for suites that bind real
# network ports. No file carries the tag at present, so this currently runs
# the same set as `test`. certs is a prerequisite: tagged suites serve TLS,
# and certs/ is gitignored, so a fresh worktree has none.
test-integration: certs
	@echo -e "\n....Running integration tests for $(GO_PROJECT_NAME)...."
	$(_Q)LOG_IGNORE=1 go test -tags=integration -timeout 300s ./... $(_RACEQ)

# Check that new/changed code meets coverage threshold (runs tests first)
diff-coverage: test-cover
	@QUIET=$(QUIET) scripts/diff-coverage.sh $(COVERPROFILE)

clean:
	rm -f ./bin/s3d

lint:
	golangci-lint run ./...

fix:
	golangci-lint run --fix ./...

govulncheck:
	go tool govulncheck ./...

# Warp is pinned so a before/after comparison is not measuring a client change.
TOOLS_DIR    := $(CURDIR)/bin/tools
WARP_VERSION ?= v1.1.4
WARP         ?= $(TOOLS_DIR)/warp

warp-install:
	@if [ ! -x "$(WARP)" ]; then \
		echo -e "\n....Installing warp $(WARP_VERSION)"; \
		mkdir -p "$(TOOLS_DIR)"; \
		GOBIN="$(TOOLS_DIR)" go install github.com/minio/warp@$(WARP_VERSION); \
	fi

# Local, self-contained correctness and performance run. The default smoke
# preset records every request for 30 seconds per workload; PERF_PRESET=compare
# uses two-minute samples for before/after decisions.
PERF_PRESET  ?= smoke
PERF_CONFIGS ?= 1host 3host
e2e-performance: build certs warp-install
	@PERF_PRESET="$(PERF_PRESET)" PERF_CONFIGS="$(PERF_CONFIGS)" WARP="$(WARP)" \
		WARP_VERSION="$(WARP_VERSION)" \
		./scripts/bench/e2e-performance.sh

# Usage: make e2e-performance-compare PERF_BEFORE=/path/to/before PERF_AFTER=/path/to/after
e2e-performance-compare: warp-install
	@test -n "$(PERF_BEFORE)" || { echo "PERF_BEFORE is required" >&2; exit 2; }
	@test -n "$(PERF_AFTER)" || { echo "PERF_AFTER is required" >&2; exit 2; }
	@WARP="$(WARP)" ./scripts/bench/compare-performance.sh "$(PERF_BEFORE)" "$(PERF_AFTER)"

# Fault injection under live load: a four-host cluster has one host frozen
# with SIGSTOP and is asserted to keep serving and then to rejoin. This is a
# liveness test, not a benchmark, so it takes roughly four minutes regardless
# of how fast the machine is.
#
# The default freezes a follower. STRESS_HOST=leader freezes whichever host
# raft elected instead, which currently fails: a gate keeps the frozen node
# first in its meta read order and pays the full client timeout per key, so
# listing slows in proportion to the number of objects.
#
# STRESS_SCENARIO=partial-put runs a different fault entirely: a client that
# stops sending mid-body. That one is not about the cluster surviving a host,
# it is about a stalled upload neither running forever nor damaging the object
# it is overwriting.
#
# STRESS_SCENARIO=torn-overwrite stops the one host holding a named shard and
# overwrites the object while it is down. The write fails, as it must with a
# shard node unreachable, and the run then asks what the object is afterwards.
# It currently fails: an overwrite has no commit point across its shards, so a
# failed one leaves the object part new and part old.
STRESS_CONFIG   ?= 4host
STRESS_FREEZE   ?= 90
STRESS_HOST     ?= follower
STRESS_SCENARIO ?= freeze
e2e-stress: build certs warp-install
	@STRESS_CONFIG="$(STRESS_CONFIG)" STRESS_FREEZE="$(STRESS_FREEZE)" \
		STRESS_HOST="$(STRESS_HOST)" STRESS_SCENARIO="$(STRESS_SCENARIO)" \
		WARP="$(WARP)" \
		./scripts/bench/e2e-stress.sh

# NilAway — advisory nil-panic analysis. Not in preflight: it has a known
# false-positive rate, so findings are triaged by hand rather than gating commits.
nilaway:
	go tool nilaway -include-pkgs=github.com/mulgadc/predastore -exclude-test-files ./...

.PHONY: certs build go_build preflight test test-cover test-race test-integration diff-coverage \
	clean lint fix govulncheck nilaway warp-install e2e-performance e2e-performance-compare \
	e2e-stress
