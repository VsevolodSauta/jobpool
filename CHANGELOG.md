# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2025-11-13

### Added
- Initial release of JobPool library
- Thread-safe queue interface with concurrent operation support
- SQLite backend implementation (optional, requires CGO and `sqlite` build tag)
  - ACID transactions with persistent storage
  - Foreign key constraints and cascading deletes
  - Optimized indexes for status, tags, assignee, and timestamps
- BadgerDB backend implementation (default, no CGO required)
  - High-performance key-value store
  - Pure Go implementation with efficient prefix-based indexing
- Job enqueue operations: single job (`EnqueueJob`) and batch (`EnqueueJobs`)
- Job dequeue operations with assignee tracking for distributed workers
- Tag-based job filtering with AND logic (jobs must have ALL specified tags)
- Priority-based dequeuing (oldest first, considering retry timestamps)
- Comprehensive job status lifecycle: `pending`, `running`, `completed`, `failed`, `stopped`, `unscheduled`, `unknown_retry`, `cancelling`, `unknown_stopped`
- Status transition validation
- Job status updates with result and error message support
- Job retrieval by ID with full job details
- Job assignee tracking with assignment timestamp
- Automatic job assignment on dequeue
- Worker disconnect handling: `ReturnJobsToPending` and `MarkJobsAsUnknown`
- Service restart recovery: `ResetRunningJobs` for handling jobs in progress during crashes
- Tag-based job statistics: `GetJobStats` with counts for total, pending, running, completed, and failed jobs
- Retry mechanism: `IncrementRetryCount` with automatic retry scheduling
- Retry timestamp tracking (`LastRetryAt`)
- Single job cancellation: `CancelJob` with status transitions (pending → unscheduled, running → cancelling)
- Batch job cancellation: `CancelJobs` by tags and/or job IDs
- Cancellation state management with valid transition validation
- Automatic cleanup of expired completed jobs: `CleanupExpiredJobs` with configurable TTL
- Worker implementation: `Worker` struct for background job processing with automatic dequeuing
- Automatic status updates and cleanup loop
- Graceful shutdown support (`Stop()` method)
- Environment-based configuration: `LoadConfig` with support for `JOBPOOL_TTL`, `JOBPOOL_CLEANUP_INTERVAL`, and `JOBPOOL_BATCH_SIZE`
- Configuration supports integer days or duration strings (e.g., "24h", "7d", "1h30m")
- Comprehensive test coverage with Ginkgo/Gomega test framework
- Benchmark tests for performance measurement
- Makefile for common development tasks
- golangci-lint configuration and code quality checks

[0.1.0]: https://github.com/VsevolodSauta/jobpool/releases/tag/v0.1.0
