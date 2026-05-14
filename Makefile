GO_PROJECT_NAME := s3d
SHELL := /bin/bash

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
	GOFIPS140=v1.0.0 go build -ldflags "-s -w" -o ./bin/s3d cmd/s3d/main.go

# Build multi-arch for docker, TODO add ARM
go_build_docker:
	@echo -e "\n....Building $(GO_PROJECT_NAME)"
	GOOS=linux GOARCH=amd64 GOFIPS140=v1.0.0 go build -ldflags "-s -w" --ldflags '-extldflags "-static"' -o ./bin/linux/s3d cmd/s3d/main.go

	GOOS=darwin GOARCH=$(GOARCH) GOFIPS140=v1.0.0 go build -ldflags "-s -w" -o ./bin/darwin/s3d cmd/s3d/main.go

# Preflight — runs the same checks as GitHub Actions (lint + security + tests).
# Use this before committing to catch CI failures locally.
preflight:
	@$(MAKE) --no-print-directory QUIET=1 lint govulncheck test-cover diff-coverage test-race test-integration
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

# Run integration tests (gated behind the 'integration' build tag — these
# bind real network ports and are excluded from default/test-race runs).
test-integration:
	@echo -e "\n....Running integration tests for $(GO_PROJECT_NAME)...."
	$(_Q)LOG_IGNORE=1 go test -tags=integration -timeout 300s ./... $(_RACEQ)

# Check that new/changed code meets coverage threshold (runs tests first)
diff-coverage: test-cover
	@QUIET=$(QUIET) scripts/diff-coverage.sh $(COVERPROFILE)

# Docker builds
docker_s3d:
	@echo "Building docker (s3d)"
	docker build -t mulgadc/predastore:latest -f- . < docker/Dockerfile-s3d

docker_compose_up:
	@echo "Running docker-compose"
	docker-compose -f docker/docker-compose.yaml up --build -d

docker_compose_down:
	@echo "Stopping docker-compose"
	docker-compose -f docker/docker-compose.yaml down

docker: go_build_docker docker_s3d

docker_clean:
	@echo "Removing Docker images and volumes"
	docker rmi mulgadc/predastore:latest

docker_test: docker docker_compose_up test docker_compose_down docker_clean

clean:
	rm -f ./bin/s3d

lint:
	golangci-lint run ./...

fix:
	golangci-lint run --fix ./...

govulncheck:
	go tool govulncheck ./...

.PHONY: certs build go_build go_build_docker preflight test test-cover test-race test-integration diff-coverage \
	docker_s3d docker_compose_up docker_compose_down docker docker_clean docker_test \
	clean lint fix govulncheck
