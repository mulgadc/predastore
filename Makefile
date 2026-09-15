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

# Container targets. The image is the compatibility loop: it starts in seconds
# and the smoke suite drives real S3 clients at it, which is how three of the
# open S3-compatibility defects were found in the first place.
DOCKER_IMAGE   ?= predastore:dev
COMPOSE_SINGLE := deploy/compose.single.yml
COMPOSE_CLUSTER:= deploy/compose.cluster.yml
COMPOSE_FILE   ?= $(COMPOSE_CLUSTER)
SMOKE_BASELINE ?= scripts/smoke-baseline.txt

docker-build:
	@echo -e "\n....Building $(DOCKER_IMAGE)"
	docker build -t $(DOCKER_IMAGE) .

# The cluster profile needs one keypair shared by every host: peers verify
# against the trust store, so hosts holding their own identities never elect a
# leader. The single profile generates its own and needs nothing here.
compose-up: docker-build
	@if [ "$(COMPOSE_FILE)" = "$(COMPOSE_CLUSTER)" ]; then deploy/docker/gen-certs.sh; fi
	PREDA_IMAGE=$(DOCKER_IMAGE) docker compose -f $(COMPOSE_FILE) up -d

compose-down:
	PREDA_IMAGE=$(DOCKER_IMAGE) docker compose -f $(COMPOSE_FILE) down -v

# Fails only on a regression against the baseline, since predastore does not
# pass every check today. SMOKE_SUITES narrows the run: `make docker-smoke
# SMOKE_SUITES=aws`.
SMOKE_SUITES ?=
docker-smoke:
	@PREDA_IMAGE=$(DOCKER_IMAGE) PREDA_BASELINE=$(SMOKE_BASELINE) \
		./scripts/smoke.sh $(SMOKE_SUITES)

# Fails on any failing check. This is the honest question — is predastore S3
# compatible — and it is red until the remaining gaps are closed. The
# regression form above is what a pull request runs, so a branch is not red
# for defects it did not introduce.
docker-smoke-strict:
	@PREDA_IMAGE=$(DOCKER_IMAGE) PREDA_BASELINE=$(SMOKE_BASELINE) PREDA_STRICT=1 \
		./scripts/smoke.sh $(SMOKE_SUITES)

# Re-record the baseline. Run it when a fix lands, in the same change.
docker-smoke-baseline:
	@PREDA_IMAGE=$(DOCKER_IMAGE) PREDA_WRITE_BASELINE=$(SMOKE_BASELINE) \
		./scripts/smoke.sh $(SMOKE_SUITES)

# ceph/s3-tests conformance. The smoke suite asks whether real clients work;
# this asks whether the operations match the specification. It needs a cluster
# on the s3tests profile, the only one carrying the three accounts and the
# us-east-1 region the suite requires:
#
#   ./scripts/start.sh -w s3tests
#   make s3-tests
S3TESTS_BASELINE ?= scripts/s3-tests-baseline.txt

s3-tests:
	@PREDA_BASELINE=$(S3TESTS_BASELINE) ./scripts/s3-tests.sh

# Fails on any failing case. Predastore is a long way from passing s3-tests, so
# this is red by design: it exists to be read, not to gate anything.
s3-tests-strict:
	@PREDA_BASELINE=$(S3TESTS_BASELINE) PREDA_STRICT=1 ./scripts/s3-tests.sh

# Re-record the baseline. Run it when a fix lands, in the same change.
s3-tests-baseline:
	@PREDA_WRITE_BASELINE=$(S3TESTS_BASELINE) ./scripts/s3-tests.sh

# GO commands
go_build:
	@echo -e "\n....Building $(GO_PROJECT_NAME)"
	go build -ldflags "-s -w" -o ./bin/s3d ./cmd/s3d

# Standalone install, for a host running predastore as a systemd service rather
# than under Spinifex or in a container. DESTDIR and PREFIX are honoured so a
# package build can stage into a buildroot.
#
# It installs files and nothing else. daemon-reload, systemd-sysusers,
# systemd-tmpfiles, `sysctl --system` and `systemctl enable` are activation, and
# a target that performed them could not be used by a package build.
PREFIX      ?= /usr/local
DESTDIR     ?=
SYSCONFDIR  ?= /etc
# The vendor directory, not /etc/systemd/system: that one is the operator's, and
# writing there would collide with a local override and survive uninstall.
UNITDIR     ?= /usr/lib/systemd/system
SYSUSERSDIR ?= /usr/lib/sysusers.d
TMPFILESDIR ?= /usr/lib/tmpfiles.d
# /etc/sysctl.d rather than /usr/lib/sysctl.d, because the value is a property
# of the machine's NICs and workload and operators do edit it.
SYSCTLDIR   ?= $(SYSCONFDIR)/sysctl.d

SYSTEMD_SRC := deploy/systemd

# A file prerequisite, not the phony `build`: an existing binary is left alone,
# so `sudo make install` works without `go` on root's PATH. Run `make build`
# first to pick up source changes.
./bin/s3d:
	$(MAKE) go_build

install: ./bin/s3d
	@echo -e "\n....Installing $(GO_PROJECT_NAME) to $(DESTDIR)$(PREFIX)"
	install -D -m 0755 ./bin/s3d '$(DESTDIR)$(PREFIX)/bin/s3d'
	install -D -m 0755 scripts/predastore-keygen.sh '$(DESTDIR)$(PREFIX)/bin/predastore-keygen'
	install -D -m 0644 $(SYSTEMD_SRC)/predastore.service '$(DESTDIR)$(UNITDIR)/predastore.service'
	install -D -m 0644 $(SYSTEMD_SRC)/predastore.sysusers.conf '$(DESTDIR)$(SYSUSERSDIR)/predastore.conf'
	install -D -m 0644 $(SYSTEMD_SRC)/predastore.tmpfiles.conf '$(DESTDIR)$(TMPFILESDIR)/predastore.conf'
	install -D -m 0644 $(SYSTEMD_SRC)/99-predastore-net.conf '$(DESTDIR)$(SYSCTLDIR)/99-predastore-net.conf'
# Both land as examples. Installing them as the live config would clobber an
# operator's credentials and topology on every upgrade, and the unit carries
# in-unit defaults so a host with neither file still starts.
	install -D -m 0644 $(SYSTEMD_SRC)/predastore.toml.example '$(DESTDIR)$(SYSCONFDIR)/predastore/predastore.toml.example'
	install -D -m 0644 $(SYSTEMD_SRC)/predastore.env '$(DESTDIR)$(SYSCONFDIR)/predastore/predastore.env.example'
# Modes only, no owner: the predastore account does not exist during a package
# build. systemd-sysusers creates it and systemd-tmpfiles takes the ownership.
	install -d -m 0750 '$(DESTDIR)$(SYSCONFDIR)/predastore'
	install -d -m 0700 '$(DESTDIR)/var/lib/predastore'
	@echo -e "\n ✅ Installed. To activate:"
	@echo "      systemd-sysusers && systemd-tmpfiles --create"
	@echo "      sysctl --system                                 # required for multi-host"
	@echo "      cp $(SYSCONFDIR)/predastore/predastore.toml.example $(SYSCONFDIR)/predastore/predastore.toml"
	@echo "      \$$EDITOR $(SYSCONFDIR)/predastore/predastore.toml   # replace every CHANGEME"
	@echo "      $(PREFIX)/bin/predastore-keygen $(SYSCONFDIR)/predastore"
	@echo "      systemctl daemon-reload && systemctl enable --now predastore"

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

# Fault injection on a four-host cluster. Thirteen scenarios, all run by
# default, and none is a benchmark: the run is mostly deliberate waiting, so it
# takes about the same wall clock regardless of how fast the machine is.
#
# They cover shard repair and handoff, the three node re-entry paths, object
# size and timestamp semantics, concurrent and torn overwrites, stale shards, a
# frozen host under load, multipart upload, and a client that stops sending
# mid-body. STRESS_HOST=leader freezes whichever host raft elected instead,
# which still fails: a gate keeps the frozen node first in its meta read order
# and pays the full client timeout per key, so listing slows in proportion to
# the number of objects.
#
# STRESS_SCENARIO narrows a run to one of them.
STRESS_CONFIG   ?= 4host
STRESS_FREEZE   ?= 90
STRESS_HOST     ?= follower
STRESS_SCENARIO ?=
e2e-stress: build certs warp-install
	@STRESS_CONFIG="$(STRESS_CONFIG)" STRESS_FREEZE="$(STRESS_FREEZE)" \
		STRESS_HOST="$(STRESS_HOST)" STRESS_SCENARIO="$(STRESS_SCENARIO)" \
		WARP="$(WARP)" \
		./scripts/bench/e2e-stress.sh

# CI gates on a regression against scripts/stress-baseline.txt rather than on a
# clean run, so a branch is not red for a gap it did not introduce.
# STRESS_SCENARIOS narrows it: `make e2e-stress-gate STRESS_SCENARIOS=freeze`.
STRESS_BASELINE  ?= scripts/stress-baseline.txt
STRESS_SCENARIOS ?=
e2e-stress-gate: build certs warp-install
	@STRESS_CONFIG="$(STRESS_CONFIG)" STRESS_FREEZE="$(STRESS_FREEZE)" \
		STRESS_HOST="$(STRESS_HOST)" STRESS_BASELINE="$(STRESS_BASELINE)" \
		WARP="$(WARP)" \
		./scripts/bench/stress-gate.sh $(STRESS_SCENARIOS)

# Fails on any failing scenario, which is what CI runs. The baseline records
# every scenario green, so this and the gate above currently agree.
e2e-stress-strict: build certs warp-install
	@STRESS_CONFIG="$(STRESS_CONFIG)" STRESS_FREEZE="$(STRESS_FREEZE)" \
		STRESS_HOST="$(STRESS_HOST)" STRESS_BASELINE="$(STRESS_BASELINE)" \
		STRESS_STRICT=1 WARP="$(WARP)" \
		./scripts/bench/stress-gate.sh $(STRESS_SCENARIOS)

# Re-record the baseline. Run it when a fix lands, in the same change.
e2e-stress-baseline: build certs warp-install
	@STRESS_CONFIG="$(STRESS_CONFIG)" STRESS_FREEZE="$(STRESS_FREEZE)" \
		STRESS_HOST="$(STRESS_HOST)" STRESS_WRITE_BASELINE="$(STRESS_BASELINE)" \
		WARP="$(WARP)" \
		./scripts/bench/stress-gate.sh $(STRESS_SCENARIOS)

# NilAway — advisory nil-panic analysis. Not in preflight: it has a known
# false-positive rate, so findings are triaged by hand rather than gating commits.
nilaway:
	go tool nilaway -include-pkgs=github.com/mulgadc/predastore -exclude-test-files ./...

.PHONY: certs build go_build install preflight test test-cover test-race test-integration diff-coverage \
	clean lint fix govulncheck nilaway warp-install e2e-performance e2e-performance-compare \
	e2e-stress e2e-stress-gate e2e-stress-strict e2e-stress-baseline \
	docker-build docker-smoke docker-smoke-strict docker-smoke-baseline \
	s3-tests s3-tests-strict s3-tests-baseline \
	compose-up compose-down
