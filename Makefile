.PHONY: help test test-race test-coverage test-no-cgo test-with-cgo lint format clean bench examples example-basic example-basic-badger example-worker

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

test: ## Run tests (default: with SQLite if CGO enabled)
	go test -tags sqlite ./...

test-no-cgo: ## Run tests without CGO (BadgerDB only)
	CGO_ENABLED=0 go test -tags "" ./...

test-with-cgo: ## Run tests with CGO (both backends)
	go test -tags sqlite ./...

test-race: ## Run tests with race detector (requires CGO)
	go test -tags sqlite -race ./...

test-coverage: ## Run tests with coverage report (default: with SQLite)
	go test -tags sqlite -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

test-coverage-no-cgo: ## Run tests with coverage (no CGO, BadgerDB only)
	CGO_ENABLED=0 go test -tags "" -coverprofile=coverage-no-cgo.out ./...
	go tool cover -html=coverage-no-cgo.out -o coverage-no-cgo.html
	@echo "Coverage report generated: coverage-no-cgo.html"

test-verbose: ## Run tests with verbose output
	go test -tags sqlite -v ./...

lint: ## Run linters
	golangci-lint run

format: ## Format code
	go fmt ./...
	goimports -w .

bench: ## Run benchmarks (requires CGO for SQLite benchmarks)
	go test -tags sqlite -bench=. -benchmem ./...

bench-no-cgo: ## Run benchmarks without CGO (BadgerDB only)
	CGO_ENABLED=0 go test -tags "" -bench=. -benchmem ./...

clean: ## Clean build artifacts
	rm -f coverage.out coverage.html coverage-no-cgo.out coverage-no-cgo.html
	rm -rf tmp
	find . -name "*.test" -delete
	find . -name "test_*.db" -delete
	find . -name "test_*.db-shm" -delete
	find . -name "test_*.db-wal" -delete
	find . -name "example*.db" -delete
	find . -name "example-data" -type d -exec rm -rf {} + 2>/dev/null || true

check: format lint test ## Run format, lint, and test

ci: test-race test-coverage lint ## Run all CI checks (with CGO)

ci-no-cgo: test-no-cgo test-coverage-no-cgo lint ## Run CI checks without CGO

# Example builds
examples: example-basic example-basic-badger example-worker ## Build all examples

example-basic: ## Build basic example (requires CGO and sqlite tag)
	@mkdir -p tmp
	@echo "Building examples/basic..."
	go build -tags sqlite -o tmp/basic ./examples/basic
	@echo "Built: tmp/basic"

example-basic-badger: ## Build basic-badger example (no CGO required)
	@mkdir -p tmp
	@echo "Building examples/basic-badger..."
	CGO_ENABLED=0 go build -tags "" -o tmp/basic-badger ./examples/basic-badger
	@echo "Built: tmp/basic-badger"

example-worker: ## Build worker example (requires CGO and sqlite tag)
	@mkdir -p tmp
	@echo "Building examples/worker..."
	go build -tags sqlite -o tmp/worker ./examples/worker
	@echo "Built: tmp/worker"

