# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0-rc.1] - 2025-11-17

### Breaking
- Renamed lifecycle statuses to clarify semantics:
  - `JobStatusPending` → `JobStatusInitialPending`
  - `JobStatusFailed` → `JobStatusFailedRetry`
- Jobs that fail no longer return to `INITIAL_PENDING`; they remain in `FAILED_RETRY` while still eligible for scheduling.
- Renamed `Job.CompletedAt` to `Job.FinalizedAt` and updated all lifecycle methods to populate the field consistently.
- Backend lifecycle methods (`CompleteJob`, `FailJob`, `StopJob`, `StopJobWithRetry`) now return `map[string]int` describing freed capacity per assignee instead of string slices.
- Assignee metadata (`assignee_id`, `assigned_at`) is preserved for historical tracking even after completion/failure, requiring downstream consumers to stop assuming those fields are cleared.
- `JobStats` structure was updated to report `StoppedJobs` separately and to treat `FAILED_RETRY`/`UNKNOWN_RETRY` as part of failed counts.

### Added
- Subscription-based `StreamJobs` implementation with explicit per-worker capacity tracking and tag-aware routing.
- Worker subscription registry with automatic rebalancing when jobs complete/fail, improving fairness across tag filters.
- `queue.md` architecture guide describing the push-based assignment model and worker expectations.
- Comprehensive Ginkgo BDD suite (`queue_bdd_test.go`, `stream_bdd_test.go`) that exercises lifecycle flows across backends.

### Changed
- Queue internals were rewritten to focus on push-based streaming rather than repeated dequeue polling, reducing duplicate work under high contention.
- Capacity notifications now reflect the actual number of freed slots per worker instead of assuming a single job per completion.
- Examples were updated to use the new status names and clarify how to enqueue `INITIAL_PENDING` jobs.

### Removed
- Deprecated `queue_test.go`, `queue_bench_test.go`, and backend-specific tests replaced by the new BDD harness.

## [1.2.0] - 2025-01-XX

### Added
- InMemoryBackend implementation for in-memory job storage
  - Thread-safe in-memory backend suitable for testing and development
  - Implements full Backend interface with atomic operations
  - No external dependencies (pure Go, no CGO required)
  - Comprehensive test coverage using BackendTestSuite
- Comprehensive test coverage for cancellation flow edge cases:
  - Test for CANCELLING → STOPPED transition via JobsResultMessage with success=false (verifies retry increment is applied)
  - Test for cancellation timeout scenario (CANCELLING → UNKNOWN_STOPPED when worker doesn't acknowledge)
  - Tests for worker response rules validation (workers cannot send both JobsResultMessage and JobCancelledMessage for same job)
  - Test for job finishing before cancellation is processed (JobsResultMessage first, then JobCancelledMessage)
  - Test for UNKNOWN_STOPPED → COMPLETED transition

### Fixed
- Fixed test for FAILED_RETRY → STOPPED transition to actually test the correct state transition
  - Previously tested INITIAL_PENDING → UNSCHEDULED instead of FAILED_RETRY → STOPPED
  - Now correctly uses backend.UpdateJobStatus to set job to FAILED_RETRY state before cancellation
- Removed duplicate/misleading test case that incorrectly suggested CANCELLING → COMPLETED was an invalid transition

## [1.1.0] - 2025-11-15

### Added
- Forceful job deletion: `DeleteJobs` method for cleanup operations
  - Validates that all jobs are in final states (COMPLETED, UNSCHEDULED, STOPPED, UNKNOWN_STOPPED) before deletion
  - Supports deletion by tags (AND logic) and/or job IDs (union of both sets)
  - Returns error if any job is not in a final state, ensuring data integrity
  - Available in both `Backend` interface and `Queue` interface
  - Implemented in both SQLiteBackend and BadgerBackend
- Comprehensive BDD-style tests using Ginkgo/Gomega:
  - `delete_test.go`: Tests for forceful job deletion with final state validation

## [1.0.0] - 2025-11-15

### Changed
- **BREAKING**: Redesigned Backend and Queue interfaces for atomic operations and reduced error probability
  - All job lifecycle transitions are now atomic (status + side effects in single operation)
  - Removed error-prone methods: `IncrementRetryCount`, `MarkJobsAsUnknown`, `ReturnJobsToPending`
  - Replaced with atomic methods: `CompleteJob`, `FailJob`, `StopJob`, `StopJobWithRetry`, `MarkJobUnknownStopped`
  - `FailJob` now atomically handles: status transition (RUNNING → FAILED_RETRY → INITIAL_PENDING), retry count increment, timestamp updates, and assignee clearing
  - `StopJobWithRetry` atomically handles cancellation with retry increment for CANCELLING jobs that fail
  - `MarkJobUnknownStopped` replaces `MarkJobsAsUnknown` for individual jobs
  - `MarkWorkerUnresponsive` replaces `MarkJobsAsUnknown` for batch worker disconnect handling
- **BREAKING**: Updated `ResetRunningJobs` behavior
  - RUNNING jobs → UNKNOWN_RETRY (should be retried)
  - CANCELLING jobs → UNKNOWN_STOPPED (cancellation failed, should not be retried)
- **BREAKING**: `AcknowledgeCancellation` behavior refined
  - Now works in conjunction with atomic methods (`StopJob`, `MarkJobUnknownStopped`)
  - Equivalent to calling atomic methods directly, but provided for backward compatibility
- Enhanced method documentation with comprehensive client-focused descriptions
  - All methods now document: purpose, atomic operations, state transitions, parameters, returns, and when to use vs other methods
  - Added "JOB LIFECYCLE OVERVIEW" section to proto file for complete lifecycle documentation

### Added
- Atomic job completion: `CompleteJob` supports transitions from RUNNING, CANCELLING, UNKNOWN_RETRY, and UNKNOWN_STOPPED states
- Atomic job failure: `FailJob` atomically transitions RUNNING/UNKNOWN_RETRY → FAILED_RETRY → INITIAL_PENDING with retry increment
- Atomic job stopping: `StopJob` atomically transitions RUNNING/CANCELLING/UNKNOWN_RETRY → STOPPED
- Atomic cancellation with retry: `StopJobWithRetry` atomically transitions CANCELLING → STOPPED with retry increment
- Atomic unknown stopped: `MarkJobUnknownStopped` atomically transitions CANCELLING/UNKNOWN_RETRY/RUNNING → UNKNOWN_STOPPED
- Generic status update: `UpdateJobStatus` for edge cases not covered by atomic methods (marked as "use sparingly")
- Comprehensive BDD-style tests using Ginkgo/Gomega:
  - `atomic_methods_test.go`: Tests for all atomic lifecycle methods
  - `cancel_test.go`: Comprehensive cancellation flow tests
  - `cleanup_test.go`: Tests for expired job cleanup
- Test helper method: `SetJobFinalizedAtForTesting` in SQLiteBackend for testing cleanup scenarios

### Removed
- **BREAKING**: `IncrementRetryCount` - replaced by atomic `FailJob` and `StopJobWithRetry` methods
- **BREAKING**: `MarkJobsAsUnknown` - replaced by `MarkWorkerUnresponsive` (batch) and `MarkJobUnknownStopped` (individual)
- **BREAKING**: `ReturnJobsToPending` - replaced by `MarkWorkerUnresponsive` which transitions RUNNING → UNKNOWN_RETRY

### Fixed
- Eliminated race conditions from non-atomic status updates
- Fixed potential inconsistencies from separate retry count increments
- Improved error handling for worker disconnect scenarios
- Corrected `ResetRunningJobs` to properly handle CANCELLING jobs

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

[2.0.0-rc.1]: https://github.com/VsevolodSauta/jobpool/releases/tag/v2.0.0-rc.1
[1.0.0]: https://github.com/VsevolodSauta/jobpool/releases/tag/v1.0.0
[0.1.0]: https://github.com/VsevolodSauta/jobpool/releases/tag/v0.1.0
