.PHONY: dev build generate install image release profile bench test clean

CGO_ENABLED=0
VERSION=$(shell git describe --abbrev=0 --tags)
COMMIT=$(shell git rev-parse --short HEAD)

all: dev

dev: build
	@./bitcask --version
	@./bitcaskd --version

build: clean generate
	@go build \
		-tags "netgo static_build" -installsuffix netgo \
		-ldflags "-w -X $(shell go list)/internal.Version=$(VERSION) -X $(shell go list)/internal.Commit=$(COMMIT)" \
		./cmd/bitcask/...
	@go build \
		-tags "netgo static_build" -installsuffix netgo \
		-ldflags "-w -X $(shell go list)/internal.Version=$(VERSION) -X $(shell go list)/internal.Commit=$(COMMIT)" \
		./cmd/bitcaskd/...

generate:
	@go generate $(shell go list)/...

install: build
	@go install ./cmd/bitcask/...
	@go install ./cmd/bitcaskd/...

image:
	@docker build -t prologic/bitcask .

release:
	@./tools/release.sh

profile: build
	@go test -cpuprofile cpu.prof -memprofile mem.prof -v -bench .

bench: build
	@go test -v -benchmem -bench=. .

test: build
	@go test -v \
		-cover -coverprofile=coverage.txt -covermode=atomic \
		-coverpkg=$(shell go list) \
		-race \
		.

clean:
	@git clean -f -d -X
