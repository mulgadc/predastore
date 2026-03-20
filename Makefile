GO_PROJECT_NAME := s3d
SHELL := /bin/bash

# Quiet-mode filters (active when QUIET=1, set by preflight via recursive make)
ifdef QUIET
  _Q     = @
  _COVQ  = 2>&1 | grep -Ev '^\s*(ok|PASS|\?|=== RUN|--- PASS:)\s' | grep -v 'coverage: 0\.0%' || true
  _RACEQ = 2>&1 | { grep -Ev '^\s*(ok|PASS|\?|=== RUN|--- PASS:)\s' || true; }; exit $${PIPESTATUS[0]}
else
  _Q     =
  _COVQ  = || true
  _RACEQ =
endif

build:
	$(MAKE) go_build

# GO commands
go_build:
	@echo -e "\n....Building $(GO_PROJECT_NAME)"
	go build -ldflags "-s -w" -o ./bin/s3d cmd/s3d/main.go

# Build multi-arch for docker, TODO add ARM
go_build_docker:
	@echo -e "\n....Building $(GO_PROJECT_NAME)"
	GOOS=linux GOARCH=amd64 go build -ldflags "-s -w" --ldflags '-extldflags "-static"' -o ./bin/linux/s3d cmd/s3d/main.go

	GOOS=darwin GOARCH=$(GOARCH) go build -ldflags "-s -w" -o ./bin/darwin/s3d cmd/s3d/main.go

go_run:
	@echo -e "\n....Running $(GO_PROJECT_NAME)...."
	$(GOPATH)/bin/$(GO_PROJECT_NAME)

# Preflight — runs the same checks as GitHub Actions (lint + security + tests).
# Use this before committing to catch CI failures locally.
preflight:
	@$(MAKE) --no-print-directory QUIET=1 lint govulncheck test-cover diff-coverage test-race
	@echo -e "\n ✅ Preflight passed — safe to commit."

# Run unit tests
test:
	@echo -e "\n....Running tests for $(GO_PROJECT_NAME)...."
	LOG_IGNORE=1 go test -timeout 120s ./...

# Run unit tests with coverage profile
# Note: go test may exit non-zero due to Go version mismatch in coverage instrumentation
# for packages without test files. We check actual test results + coverage threshold instead.
COVERPROFILE ?= coverage.out
test-cover:
	@echo -e "\n....Running tests with coverage for $(GO_PROJECT_NAME)...."
	$(_Q)LOG_IGNORE=1 go test -timeout 120s -coverprofile=$(COVERPROFILE) -covermode=atomic ./... $(_COVQ)
	@scripts/check-coverage.sh $(COVERPROFILE) $(QUIET)

# Run unit tests with race detector
test-race:
	@echo -e "\n....Running tests with race detector for $(GO_PROJECT_NAME)...."
	$(_Q)LOG_IGNORE=1 go test -race -timeout 300s ./... $(_RACEQ)

# Check that new/changed code meets coverage threshold (runs tests first)
diff-coverage: test-cover
	@QUIET=$(QUIET) scripts/diff-coverage.sh $(COVERPROFILE)

bench:
	@echo -e "\n....Running benchmarks for $(GO_PROJECT_NAME)...."
	LOG_IGNORE=1 go test -benchmem -run=. -bench=. ./...

dev:
	air go run cmd/s3d/main.go

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

run:
	$(MAKE) go_build
	$(MAKE) go_run

clean:
	rm -f ./bin/s3d

lint:
	golangci-lint run ./...

fix:
	golangci-lint run --fix ./...

govulncheck:
	go tool govulncheck ./...

.PHONY: build go_build go_build_docker go_run preflight test test-cover test-race diff-coverage bench dev \
	docker_s3d docker_compose_up docker_compose_down docker docker_clean docker_test \
	run clean lint fix govulncheck
