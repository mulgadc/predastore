GO_PROJECT_NAME := s3d
SHELL := /bin/bash

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

# Preflight — runs the same checks as GitHub Actions (format + lint + security + tests).
# Use this before committing to catch CI failures locally.
preflight: check-format vet security-check test
	@echo -e "\n ✅ Preflight passed — safe to commit."

# Run unit tests
test:
	@echo -e "\n....Running tests for $(GO_PROJECT_NAME)...."
	LOG_IGNORE=1 go test -v -timeout 120s ./...

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

# Format all Go files in place
format:
	gofmt -w .

# Check that all Go files are formatted (CI-compatible, fails on diff)
check-format:
	@echo "Checking gofmt..."
	@UNFORMATTED=$$(gofmt -l .); \
	if [ -n "$$UNFORMATTED" ]; then \
		echo "Files not formatted:"; \
		echo "$$UNFORMATTED"; \
		echo "Run 'make format' to fix."; \
		exit 1; \
	fi
	@echo "  gofmt ok"

# Go vet (fails on issues, matches CI)
vet:
	@echo "Running go vet..."
	go vet ./...
	@echo "  go vet ok"

# Security checks — each tool fails the build on findings (matches CI).
# Reports are also saved to tests/ for review.
security-check:
	@echo -e "\n....Running security checks for $(GO_PROJECT_NAME)...."
	set -o pipefail && go tool govulncheck ./... 2>&1 | tee tests/govulncheck-report.txt
	@echo "  govulncheck ok"
	set -o pipefail && go tool gosec -exclude=G104,G204,G304,G402 -exclude-generated ./... 2>&1 | tee tests/gosec-report.txt
	@echo "  gosec ok"
	set -o pipefail && go tool staticcheck -checks="all,-ST1000,-ST1003,-ST1016,-ST1020,-ST1021,-ST1022,-SA1019,-SA9005" ./... 2>&1 | tee tests/staticcheck-report.txt
	@echo "  staticcheck ok"

.PHONY: build go_build go_build_docker go_run preflight test bench dev \
	docker_s3d docker_compose_up docker_compose_down docker docker_clean docker_test \
	run clean format check-format vet security-check
