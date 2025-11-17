# JobPool

[![Go Reference](https://pkg.go.dev/badge/github.com/VsevolodSauta/jobpool.svg)](https://pkg.go.dev/github.com/VsevolodSauta/jobpool)
[![Go Report Card](https://goreportcard.com/badge/github.com/VsevolodSauta/jobpool)](https://goreportcard.com/report/github.com/VsevolodSauta/jobpool)

A reusable job queue library with support for multiple storage backends (SQLite, BadgerDB) and distributed worker management.

## Features

- Multiple backend support (SQLite, BadgerDB)
- **Atomic job lifecycle operations** - all state transitions are atomic (status + side effects in single operation)
- Push-based job streaming with automatic assignment
- Job assignee tracking
- Worker disconnect handling with automatic retry
- Batch job operations
- Tag-based job filtering and statistics
- Automatic retry mechanism with retry count tracking
- Automatic cleanup of expired completed jobs
- Comprehensive cancellation support with timeout handling

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

## Installation

### Stable Version

```bash
go get github.com/VsevolodSauta/jobpool@v2.0.0-rc.1
```

### Latest Version

```bash
go get github.com/VsevolodSauta/jobpool@latest
```

### In Your go.mod

```go
require (
    github.com/VsevolodSauta/jobpool v2.0.0-rc.1
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
        Status:        jobpool.JobStatusInitialPending,
        JobType:       "my_task",
        JobDefinition: []byte(`{"data": "example"}`),
        Tags:          []string{"tag1"},
        CreatedAt:     time.Now(),
    }
    queue.EnqueueJob(context.Background(), job)

    // Stream jobs (push-based model)
    jobCh := make(chan []*jobpool.Job, 1)
    go func() {
        defer close(jobCh)
        queue.StreamJobs(context.Background(), "worker-1", nil, 1, jobCh)
    }()

    // Process jobs from channel
    for jobs := range jobCh {
        for _, job := range jobs {
            // Process job...
            // Complete job atomically
            queue.CompleteJob(context.Background(), job.ID, []byte("result"))
        }
    }
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

    // Enqueue a job
    job := &jobpool.Job{
        ID:            "job-1",
        Status:        jobpool.JobStatusInitialPending,
        JobType:       "my_task",
        JobDefinition: []byte(`{"data": "example"}`),
        Tags:          []string{"tag1"},
        CreatedAt:     time.Now(),
    }
    queue.EnqueueJob(context.Background(), job)

    // Stream jobs (push-based model)
    jobCh := make(chan []*jobpool.Job, 1)
    go func() {
        defer close(jobCh)
        queue.StreamJobs(context.Background(), "worker-1", nil, 1, jobCh)
    }()

    // Process jobs from channel
    for jobs := range jobCh {
        for _, job := range jobs {
            // Process job...
            // Complete job atomically
            queue.CompleteJob(context.Background(), job.ID, []byte("result"))
        }
    }
}
```

## Key API Methods

### Atomic Job Lifecycle Operations

All job state transitions are atomic (status + side effects in a single operation):

- **`CompleteJob(jobID, result)`** - Atomically transitions job to COMPLETED with result
- **`FailJob(jobID, errorMsg)`** - Atomically transitions job to FAILED_RETRY → INITIAL_PENDING with retry increment
- **`StopJob(jobID, errorMsg)`** - Atomically transitions job to STOPPED (cancellation)
- **`StopJobWithRetry(jobID, errorMsg)`** - Atomically stops job with retry increment (for CANCELLING jobs that fail)
- **`MarkJobUnknownStopped(jobID, errorMsg)`** - Atomically marks job as UNKNOWN_STOPPED (worker unresponsive)

All methods handle status transitions, timestamp updates, assignee clearing, and retry count increments atomically to prevent race conditions.

### Job Management

- **`EnqueueJob(job)`** / **`EnqueueJobs(jobs)`** - Submit jobs to the queue
- **`StreamJobs(assigneeID, tags, limit, channel)`** - Push-based job streaming (jobs automatically assigned)
- **`CancelJobs(tags, jobIDs)`** - Cancel jobs by tags or IDs
- **`GetJob(jobID)`** - Retrieve job details
- **`GetJobStats(tags)`** - Get statistics for jobs matching tags

See the [API documentation](https://pkg.go.dev/github.com/VsevolodSauta/jobpool) for complete method reference.

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

Current version: **v2.0.0-rc.1**

**Note**: Version 2.0.0 introduces additional breaking changes (status renames, lifecycle semantics). See [CHANGELOG.md](CHANGELOG.md) for migration guidance and version history.

## License

See [LICENSE](LICENSE) file for details.

