GO_PROJECT_NAME := s3d

# Where to install Go tools
GOBIN ?= $(shell go env GOBIN)
ifeq ($(GOBIN),)
  GOBIN := $(shell go env GOPATH)/bin
endif

GOVULNCHECK := $(GOBIN)/govulncheck

# Install govulncheck only if the binary is missing / out of date
$(GOVULNCHECK):
	go install golang.org/x/vuln/cmd/govulncheck@latest

GOSECCHECK := $(GOBIN)/gosec

# Install gosec only if the binary is missing / out of date
$(GOSECCHECK):
	go install github.com/securego/gosec/v2/cmd/gosec@latest

GOSTATICCHECK := $(GOBIN)/staticcheck

# Install govulncheck only if the binary is missing / out of date
$(GOSTATICCHECK):
	go install honnef.co/go/tools/cmd/staticcheck@latest

build:
	$(MAKE) go_build

# GO commands
go_build:
	@echo "\n....Building $(GO_PROJECT_NAME)"
	go build -ldflags "-s -w" -o ./bin/s3d cmd/s3d/main.go

# Build multi-arch for docker, TODO add ARM
go_build_docker:
	@echo "\n....Building $(GO_PROJECT_NAME)"
	GOOS=linux GOARCH=amd64 go build -ldflags "-s -w" --ldflags '-extldflags "-static"' -o ./bin/linux/s3d cmd/s3d/main.go

	GOOS=darwin GOARCH=$(GOARCH) go build -ldflags "-s -w" -o ./bin/darwin/s3d cmd/s3d/main.go

go_run:
	@echo "\n....Running $(GO_PROJECT_NAME)...."
	$(GOPATH)/bin/$(GO_PROJECT_NAME)

test: $(GOVULNCHECK)
	@echo "\n....Running tests for $(GO_PROJECT_NAME)...."
	LOG_IGNORE=1 go test -v ./...
	$(GOVULNCHECK) ./...

bench:
	@echo "\n....Running benchmarks for $(GO_PROJECT_NAME)...."
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
#docker volume ls -f dangling=true
#yes | docker volume prune

docker_test: docker docker_compose_up test docker_compose_down docker_clean

run:
	$(MAKE) go_build
	$(MAKE) go_run

clean:
	rm ./bin/s3d

security: $(GOVULNCHECK) $(GOSECCHECK) $(GOSTATICCHECK)
	@echo "\n....Running security checks for $(GO_PROJECT_NAME)...."

	$(GOVULNCHECK) ./... > tests/govulncheck-report.txt || true
	@echo "Govulncheck report saved to tests/govulncheck-report.txt"

	$(GOSECCHECK) ./... > tests/gosec-report.txt || true
	@echo "Gosec report saved to tests/gosec-report.txt"

	# default config + disable dep warning since we are using aws sdk v1
	$(GOSTATICCHECK) -checks="all,-ST1000,-ST1003,-ST1016,-ST1020,-ST1021,-ST1022,-SA1019,-SA9005" ./...  > tests/staticcheck-report.txt || true
	@echo "Staticcheck report saved to tests/staticcheck-report.txt"

	go vet ./... 2>&1 | tee tests/govet-report.txt || true
	@echo "Go vet report saved to tests/govet-report.txt"

.PHONY: go_build go_run build run test docker security
