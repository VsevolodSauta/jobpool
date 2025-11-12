# JobPool

[![Go Reference](https://pkg.go.dev/badge/github.com/VsevolodSauta/jobpool.svg)](https://pkg.go.dev/github.com/VsevolodSauta/jobpool)
[![Go Report Card](https://goreportcard.com/badge/github.com/VsevolodSauta/jobpool)](https://goreportcard.com/report/github.com/VsevolodSauta/jobpool)

A reusable job queue library with support for multiple storage backends (SQLite, BadgerDB) and distributed worker management.

## Features

- Multiple backend support (SQLite, BadgerDB)
- Job assignee tracking
- Automatic job return-to-pending on worker disconnect
- Batch job operations
- Tag-based job filtering and statistics
- Retry mechanism with configurable retry counts
- Automatic cleanup of expired completed jobs

## Backends

### SQLite (Optional, requires CGO)
- Persistent storage using SQLite database
- ACID transactions
- Suitable for single-server deployments
- **Requires CGO** and the `sqlite` build tag
- To use: Build with `-tags sqlite` or import with `go build -tags sqlite`
- To exclude: Build without the tag (library will work with BadgerDB only)

### BadgerDB (Default, no CGO)
- High-performance key-value store
- Embedded database
- Suitable for high-throughput scenarios
- **No CGO required** - pure Go implementation
- Always available, no build tags needed

## Usage

## Installation

### Stable Version

```bash
go get github.com/VsevolodSauta/jobpool@v0.1.0
```

### Latest Version

```bash
go get github.com/VsevolodSauta/jobpool@latest
```

### In Your go.mod

```go
require (
    github.com/VsevolodSauta/jobpool v0.1.0
)
```

### Building Without CGO (SQLite Optional)

The library can be built **without CGO** by excluding the SQLite backend:

```bash
# Build without SQLite (no CGO required)
CGO_ENABLED=0 go build -tags "" ./...

# Build with SQLite (CGO required)
go build -tags sqlite ./...
```

**Note**: 
- By default, the library includes SQLite support (requires CGO)
- To build without CGO, exclude the `sqlite` build tag
- BadgerDB backend is always available and does not require CGO
- After the initial release, the git tag `v0.1.0` must be created and pushed for `go get` to work with version pinning

#### Build Tag Details

**Without CGO (BadgerDB Only):**
- Use `CGO_ENABLED=0 go build -tags "" ./...`
- Only BadgerDB backend is available
- Pure Go binary, no CGO dependencies

**With CGO (Both Backends):**
- Use `go build -tags sqlite ./...`
- Both BadgerDB and SQLite backends available
- Requires SQLite development libraries

**Testing:**
```bash
# Test without SQLite
CGO_ENABLED=0 go test -tags "" ./...

# Test with SQLite
go test -tags sqlite ./...
```

**Troubleshooting:**
- **"undefined: NewSQLiteBackend"**: Build with `-tags sqlite` or use BadgerDB backend
- **"CGO_ENABLED=0 but CGO is required"**: Build with `-tags ""` to exclude SQLite, or enable CGO
- **"sqlite3 driver not found"**: Ensure CGO is enabled and SQLite development libraries are installed

## Quick Start

### With BadgerDB (No CGO)

```go
package main

import (
    "context"
    "time"
    "github.com/VsevolodSauta/jobpool"
)

func main() {
    // Create BadgerDB backend (no CGO required)
    backend, _ := jobpool.NewBadgerBackend("./jobs-data")
    queue := jobpool.NewPoolQueue(backend)
    defer queue.Close()

    // Enqueue a job
    job := &jobpool.Job{
        ID:            "job-1",
        Status:        jobpool.JobStatusPending,
        JobType:       "my_task",
        JobDefinition: []byte(`{"data": "example"}`),
        Tags:          []string{"tag1"},
        CreatedAt:     time.Now(),
    }
    queue.EnqueueJob(context.Background(), job)

    // Dequeue and process
    jobs, _ := queue.DequeueJobs(context.Background(), "worker-1", 1)
    // ... process jobs ...
}
```

### With SQLite (Requires CGO and sqlite build tag)

```go
package main

import (
    "context"
    "time"
    "github.com/VsevolodSauta/jobpool"
)

func main() {
    // Create SQLite backend (requires CGO and -tags sqlite)
    backend, _ := jobpool.NewSQLiteBackend("./jobs.db")
    queue := jobpool.NewPoolQueue(backend)
    defer queue.Close()

    // ... same as above ...
}
```

## Examples

See the `examples/` directory for complete examples:
- `examples/basic/` - Basic usage example with SQLite (requires `-tags sqlite`)
- `examples/basic-badger/` - BadgerDB-only example (no CGO required, no build tags)
- `examples/worker/` - Worker implementation example with SQLite (requires `-tags sqlite`)

**Note**: Examples using SQLite require building with `-tags sqlite`. For CGO-free builds, use `examples/basic-badger/`.

## Documentation

For complete API documentation, see [pkg.go.dev](https://pkg.go.dev/github.com/VsevolodSauta/jobpool) (generated automatically from godoc comments).

The README above provides quick start examples and usage patterns. For detailed API reference, consult the godoc comments in the source code or visit the package documentation on pkg.go.dev.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, testing, and contribution guidelines.

### Running Tests

```bash
# Run all tests
make test

# Run tests with race detector
make test-race

# Run tests with coverage
make test-coverage

# Run benchmarks
make bench

# Format code
make format

# Run linter
make lint
```

## Requirements

- Go 1.21 or later

## Version

Current version: **v0.1.0**

See [CHANGELOG.md](CHANGELOG.md) for version history.

## License

See [LICENSE](LICENSE) file for details.

