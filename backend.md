# Backend API Specification

## Introduction

The Backend interface provides a storage abstraction for job queue operations. The Queue interface delegates all persistent storage and atomic state transition operations to a Backend implementation. All Backend operations must be thread-safe and support concurrent access from multiple goroutines.

The Backend is responsible for:
- Persistent storage of job state and metadata
- Atomic state transitions with transactional consistency
- Efficient querying and filtering by tags and status
- Job assignment with race condition prevention
- Capacity tracking through assignee ID management

### Core Concepts

- **Assignee**: A worker identifier (assigneeID) that can process jobs. The Backend tracks which jobs are assigned to which assignees for capacity management.
- **Capacity Tracking**: When jobs transition from RUNNING to terminal states (or become eligible again), the Backend returns `freedAssigneeIDs` - a map indicating which assignees had capacity freed and how many jobs were freed. This allows the Queue to manage worker capacity efficiently.
- **Tags**: String labels attached to jobs for filtering. Tag matching uses AND logic: a job matches a filter if the job contains ALL of the specified tags. Empty tags means no filtering (all jobs match).
- **Job States**: Jobs transition through states (INITIAL_PENDING, RUNNING, COMPLETED, FAILED_RETRY, STOPPED, etc.). Only INITIAL_PENDING, FAILED_RETRY, and UNKNOWN_RETRY states are eligible for scheduling via DequeueJobs.
- **Eligibility**: Jobs in INITIAL_PENDING, FAILED_RETRY, or UNKNOWN_RETRY states can be assigned to workers. Jobs in other states are not eligible for scheduling.
- **AssigneeID Preservation**: The Backend preserves `AssigneeID` and `AssignedAt` fields for historical tracking even after job completion or failure. These fields are never cleared. To check if a job is currently assigned, check that `Status == RUNNING` rather than checking if `AssigneeID` is empty.

## Core Types

The Backend uses the same core types as the Queue interface. See `queue.md` for detailed definitions of:
- `Job`: Job structure with all fields
- `JobStatus`: Job state enumeration
- `JobStats`: Statistics structure for job queries

### freedAssigneeIDs Return Value

Several Backend methods return `map[string]int` called `freedAssigneeIDs`. This map indicates which assignees had capacity freed and how many jobs were freed:

- **Key**: Assignee ID (worker identifier)
- **Value**: Count of jobs freed for that assignee
- **Empty map**: Indicates no capacity was freed (job was not assigned to any assignee)
- **Typical usage**: Most methods return a map with 0 or 1 entry (one job freed for one assignee)
- **Multiplicity**: If multiple jobs are freed for the same assignee in a single operation, the count reflects the multiplicity

The Queue uses this information to:
- Free capacity for subscriptions matching the assigneeID
- Notify workers about newly available capacity
- Track which subscriptions need capacity updates

## Interface Definition

The Backend interface defines all methods required for job storage and state management:

```go
type Backend interface {
    // Job submission
    EnqueueJob(ctx context.Context, job *Job) (string, error)
    EnqueueJobs(ctx context.Context, jobs []*Job) ([]string, error)

    // Job assignment (atomic: INITIAL_PENDING/FAILED_RETRY/UNKNOWN_RETRY → RUNNING)
    DequeueJobs(ctx context.Context, assigneeID string, tags []string, limit int) ([]*Job, error)

    // Job lifecycle
    CompleteJob(ctx context.Context, jobID string, result []byte) (map[string]int, error)
    FailJob(ctx context.Context, jobID string, errorMsg string) (map[string]int, error)
    StopJob(ctx context.Context, jobID string, errorMsg string) (map[string]int, error)
    StopJobWithRetry(ctx context.Context, jobID string, errorMsg string) (map[string]int, error)
    MarkJobUnknownStopped(ctx context.Context, jobID string, errorMsg string) (map[string]int, error)
    UpdateJobStatus(ctx context.Context, jobID string, status JobStatus, result []byte, errorMsg string) (map[string]int, error)

    // Cancellation
    CancelJobs(ctx context.Context, tags []string, jobIDs []string) ([]string, []string, error)
    AcknowledgeCancellation(ctx context.Context, jobID string, wasExecuting bool) error

    // Worker management
    MarkWorkerUnresponsive(ctx context.Context, assigneeID string) error

    // Query operations
    GetJob(ctx context.Context, jobID string) (*Job, error)
    GetJobStats(ctx context.Context, tags []string) (*JobStats, error)

    // Maintenance
    ResetRunningJobs(ctx context.Context) error
    CleanupExpiredJobs(ctx context.Context, ttl time.Duration) error
    DeleteJobs(ctx context.Context, tags []string, jobIDs []string) error

    Close() error
}
```

## Behavioral Contracts

The following sections specify the behavior of each Backend method using preconditions, postconditions, and observable effects. Methods are organized by functional category.

### Job Submission

#### EnqueueJob

**Signature**: `EnqueueJob(ctx context.Context, job *Job) (string, error)`

**Preconditions**:
- `ctx` is a valid context
- `job` is non-nil
- `job.ID` is non-empty
- `job.Status` is `INITIAL_PENDING`
- `job.ID` does not already exist in storage

**Postconditions**:
- If successful:
  - Returns `job.ID` and `nil` error
  - Job is stored with all fields preserved
  - Job status is `INITIAL_PENDING`
  - Job is immediately available for DequeueJobs
- If unsuccessful:
  - Returns empty string and non-nil error
  - No partial job record exists

**Observable Effects**:
- Job becomes available for DequeueJobs calls with matching tag filters
- Job appears in GetJobStats queries matching its tags
- Job can be retrieved via GetJob immediately after creation

**Backend Expectations**:
- Must create job record with unique ID and store all job fields
- Must set initial status to `INITIAL_PENDING`
- Operation must be atomic (all-or-nothing)
- Must return error if job ID already exists
- Must support concurrent job creation
- Must store all tags associated with the job

**Error Conditions**:
- Returns error if job ID already exists (duplicate detection)
- Returns error if database/storage operation fails
- Returns error if context is cancelled
- Returns error if job validation fails

#### EnqueueJobs

**Signature**: `EnqueueJobs(ctx context.Context, jobs []*Job) ([]string, error)`

**Preconditions**:
- `ctx` is a valid context
- `jobs` is a slice (may be empty)
- All jobs in slice are non-nil
- All job IDs are non-empty and unique within the batch
- All job statuses are `INITIAL_PENDING`
- No job ID in batch already exists in storage

**Postconditions**:
- If successful:
  - Returns slice of job IDs in same order as input jobs and `nil` error
  - All jobs are stored with `INITIAL_PENDING` status
  - All jobs are immediately available for DequeueJobs
- If unsuccessful:
  - Returns partial job IDs (for jobs successfully enqueued) and non-nil error
  - No partial batch creation should occur (atomic operation)

**Observable Effects**:
- All successfully enqueued jobs become available for matching DequeueJobs
- All jobs appear in stats and queries

**Backend Expectations**:
- Must create all jobs atomically (all-or-nothing)
- Must ensure each job has unique ID (no duplicates within batch or with existing jobs)
- Should be more efficient than multiple `EnqueueJob` calls
- Must return error if any job fails validation
- Must use batch insert operations when possible

**Error Conditions**:
- Returns error if any job fails validation (e.g., duplicate ID)
- Returns error if database/storage operation fails
- Returns error if context is cancelled
- Returns error with details about which job(s) failed

### Job Assignment

#### DequeueJobs

**Signature**: `DequeueJobs(ctx context.Context, assigneeID string, tags []string, limit int) ([]*Job, error)`

**Parameters**:
- `assigneeID`: Worker identifier to assign jobs to
- `tags`: Filter jobs by tags using AND logic (jobs must have ALL provided tags). Empty slice means no filtering.
- `limit`: Maximum number of jobs to dequeue and assign

**Preconditions**:
- `ctx` is a valid context
- `assigneeID` is non-empty
- `limit > 0`
- `tags` is a slice (may be empty for no filtering)

**Postconditions**:
- Returns slice of jobs (may be empty if no eligible jobs available)
- Jobs in returned slice have:
  - `Status = RUNNING`
  - `AssigneeID = assigneeID`
  - `AssignedAt` set to current time
  - `StartedAt` set to current time if not already set
- Jobs are filtered by tags (AND logic: all provided tags must be in job tags)
- Empty tags means accept all jobs
- Jobs are selected in ascending order by `COALESCE(LastRetryAt, CreatedAt)` (oldest first) as a selection criterion
- The order of jobs in returned slice is not guaranteed to match this ordering
- Total returned jobs never exceeds `limit`
- Operation is atomic: jobs are selected and assigned in a single transaction

**Observable Effects**:
- Selected jobs transition from eligible states (INITIAL_PENDING, FAILED_RETRY, UNKNOWN_RETRY) to RUNNING
- Jobs are assigned to `assigneeID` and cannot be assigned to another worker
- Jobs become unavailable for other DequeueJobs calls until they transition to eligible states again

**Backend Expectations**:
- Must select jobs eligible for scheduling: `INITIAL_PENDING`, `FAILED_RETRY`, `UNKNOWN_RETRY`
- Must filter by tags using AND logic (job must have ALL provided tags; empty tags means no filtering)
- Must atomically assign selected jobs to `assigneeID` (set `AssigneeID`, `AssignedAt`, transition to `RUNNING`)
- Must prevent same job from being assigned to multiple workers concurrently
- Must return up to `limit` jobs per call
- Must select jobs in ascending order by `COALESCE(LastRetryAt, CreatedAt)` (oldest first)
- Must use database-level locking or optimistic concurrency control to prevent race conditions
- Must support efficient querying by status and tags for real-time responsiveness

**Error Conditions**:
- Returns error if database/storage operation fails
- Returns error if context is cancelled
- Returns empty slice (not error) if no eligible jobs available

**Implementation Notes**:
- Should use `SELECT ... FOR UPDATE` or similar locking mechanism to prevent double-assignment
- Should use transactions to ensure atomicity
- Should use efficient indexes on status and tags for performance
- May return fewer jobs than `limit` if fewer eligible jobs are available

### Job Lifecycle

#### CompleteJob

**Signature**: `CompleteJob(ctx context.Context, jobID string, result []byte) (map[string]int, error)`

**Preconditions**:
- `ctx` is a valid context
- `jobID` is non-empty
- Job exists in storage
- Job is in a valid state for completion: `RUNNING`, `CANCELLING`, `UNKNOWN_RETRY`, or `UNKNOWN_STOPPED`

**Postconditions**:
- If job exists and in valid state:
  - Job status becomes `COMPLETED`
  - `Result` field set to provided result
  - `FinalizedAt` timestamp set to current time
  - `StartedAt` timestamp set to current time if not already set
  - `AssigneeID` and `AssignedAt` are preserved (not cleared) for historical tracking
  - Returns `freedAssigneeIDs` map:
    - Empty map if job was not assigned to an assignee
    - Contains `assignee_id` with count=1 if job was assigned
- If job not found or invalid state:
  - Returns error

**Observable Effects**:
- Job will never be eligible for scheduling again
- Capacity is freed for the assignee (if job was assigned)

**Valid State Transitions**:
- `RUNNING → COMPLETED`: Normal completion flow
- `CANCELLING → COMPLETED`: Job completed before cancellation took effect
- `UNKNOWN_RETRY → COMPLETED`: Job completed after worker reconnection
- `UNKNOWN_STOPPED → COMPLETED`: Job completed despite previous cancellation issues

**Backend Expectations**:
- Must transition job to `COMPLETED` state atomically
- Must store result bytes and set `FinalizedAt` timestamp
- Must verify job exists and is in valid state before transition
- Must preserve `AssigneeID` and `AssignedAt` for historical tracking
- Must return error if job is in invalid state (terminal states like `COMPLETED`, `STOPPED`, `UNSCHEDULED`)
- Must return `freedAssigneeIDs` based on whether job was assigned

**Error Conditions**:
- Returns error if job doesn't exist
- Returns error if job is in invalid state (terminal states)
- Returns error if database/storage operation fails
- Returns error if context is cancelled

#### FailJob

**Signature**: `FailJob(ctx context.Context, jobID string, errorMsg string) (map[string]int, error)`

**Preconditions**:
- `ctx` is a valid context
- `jobID` is non-empty
- `errorMsg` is non-empty
- Job exists in storage
- Job is in a valid state for failure: `RUNNING` or `UNKNOWN_RETRY`

**Postconditions**:
- If job exists and in valid state:
  - Job status becomes `FAILED_RETRY`
  - `ErrorMessage` field set to provided error message
  - `RetryCount` incremented by 1
  - `LastRetryAt` timestamp set to current time
  - `AssigneeID` and `AssignedAt` are preserved (not cleared) for historical tracking
  - Returns `freedAssigneeIDs` map:
    - Empty map if job was not assigned to an assignee
    - Contains `assignee_id` with count=1 if job was assigned
- If job not found or invalid state:
  - Returns error

**Observable Effects**:
- Job becomes eligible for scheduling again (but remains in FAILED_RETRY state)
- Capacity is freed for the assignee (if job was assigned)

**Valid State Transitions**:
- `RUNNING → FAILED_RETRY`: Normal failure flow (job eligible for scheduling)
- `UNKNOWN_RETRY → FAILED_RETRY`: Job failed after worker reconnection (job eligible for scheduling)

**Important**: Jobs never return to INITIAL_PENDING state once they leave it. FAILED_RETRY jobs are eligible for scheduling but remain in FAILED_RETRY state.

**Backend Expectations**:
- Must transition job to `FAILED_RETRY` state atomically
- Must increment `RetryCount` by 1 and set `LastRetryAt` timestamp atomically with state transition
- Must store error message
- Must verify job exists and is in valid state before transition
- Must preserve `AssigneeID` and `AssignedAt` for historical tracking
- Must return error if job is in invalid state (terminal states)
- Must return `freedAssigneeIDs` based on whether job was assigned

**Error Conditions**:
- Returns error if job doesn't exist
- Returns error if job is in invalid state (terminal states)
- Returns error if `errorMsg` is empty
- Returns error if database/storage operation fails
- Returns error if context is cancelled

#### StopJob

**Signature**: `StopJob(ctx context.Context, jobID string, errorMsg string) (map[string]int, error)`

**Preconditions**:
- `ctx` is a valid context
- `jobID` is non-empty
- `errorMsg` is optional (may be empty string, unlike FailJob which requires non-empty errorMsg)
- Job exists in storage
- Job is in a valid state for stopping: `RUNNING`, `CANCELLING`, or `UNKNOWN_RETRY`

**Postconditions**:
- If job exists and in valid state:
  - Job status becomes `STOPPED`
  - `ErrorMessage` field set (may be empty if errorMsg was empty)
  - `FinalizedAt` timestamp set to current time (if not already set)
  - `AssigneeID` and `AssignedAt` are preserved (not cleared) for historical tracking
  - Returns `freedAssigneeIDs` map:
    - Empty map if job was not assigned to an assignee
    - Contains `assignee_id` with count=1 if job was assigned
- If job not found or invalid state:
  - Returns error

**Observable Effects**:
- Job no longer eligible for scheduling (terminal state)
- Capacity is freed for the assignee (if job was assigned)

**Valid State Transitions**:
- `RUNNING → STOPPED`: Job cancelled before completion
- `CANCELLING → STOPPED`: Job cancellation acknowledged by worker (normal cancellation flow)
- `UNKNOWN_RETRY → STOPPED`: Job cancelled after worker reconnection

**Backend Expectations**:
- Must transition job to `STOPPED` state atomically
- Must store error message (may be empty) and set `FinalizedAt` timestamp
- Must verify job exists and is in valid state before transition
- Must preserve `AssigneeID` and `AssignedAt` for historical tracking
- Must return error if job is in invalid state (already terminal)
- Must return `freedAssigneeIDs` based on whether job was assigned

**Error Conditions**:
- Returns error if job doesn't exist
- Returns error if job is in invalid state (already terminal)
- Returns error if database/storage operation fails
- Returns error if context is cancelled

#### StopJobWithRetry

**Signature**: `StopJobWithRetry(ctx context.Context, jobID string, errorMsg string) (map[string]int, error)`

**Preconditions**:
- `ctx` is a valid context
- `jobID` is non-empty
- Job exists in storage
- Job is in `CANCELLING` state

**Postconditions**:
- If job exists and in `CANCELLING` state:
  - Job status becomes `STOPPED`
  - `ErrorMessage` field set
  - `RetryCount` incremented by 1
  - `LastRetryAt` timestamp set to current time
  - `FinalizedAt` timestamp set to current time (if not already set)
  - `AssigneeID` and `AssignedAt` are preserved (not cleared) for historical tracking
  - Returns `freedAssigneeIDs` map:
    - Empty map if job was not assigned to an assignee
    - Contains `assignee_id` with count=1 if job was assigned
- If job not found or not in `CANCELLING` state:
  - Returns error

**Observable Effects**:
- Job no longer eligible for scheduling (terminal state)
- Capacity is freed for the assignee (if job was assigned)

**Valid State Transitions**:
- `CANCELLING → STOPPED` (with retry increment): Job failed while being cancelled (applies FAILED_RETRY state effects)

**Backend Expectations**:
- Must transition job from `CANCELLING` to `STOPPED` state atomically
- Must increment `RetryCount` by 1 and set `LastRetryAt` timestamp (applies retry increment from transitory FAILED state)
- Must store error message and set `FinalizedAt` timestamp
- Must verify job is in `CANCELLING` state before transition
- Must preserve `AssigneeID` and `AssignedAt` for historical tracking
- Must return error if job is not in `CANCELLING` state
- Must return `freedAssigneeIDs` based on whether job was assigned

**Error Conditions**:
- Returns error if job doesn't exist
- Returns error if job is not in `CANCELLING` state
- Returns error if database/storage operation fails
- Returns error if context is cancelled

#### MarkJobUnknownStopped

**Signature**: `MarkJobUnknownStopped(ctx context.Context, jobID string, errorMsg string) (map[string]int, error)`

**Preconditions**:
- `ctx` is a valid context
- `jobID` is non-empty
- Job exists in storage

**Postconditions**:
- If job exists:
  - Job status becomes `UNKNOWN_STOPPED`
  - `ErrorMessage` field set
  - `FinalizedAt` timestamp set to current time (if not already set)
  - `AssigneeID` and `AssignedAt` are preserved (not cleared) for historical tracking
  - Returns `freedAssigneeIDs` map:
    - Empty map if job was not assigned to an assignee
    - Contains `assignee_id` with count=1 if job was assigned
- If job not found:
  - Returns error

**Observable Effects**:
- Job no longer eligible for scheduling (terminal state)
- Capacity is freed for the assignee (if job was assigned)

**Valid State Transitions**:
- `CANCELLING → UNKNOWN_STOPPED`: Cancellation timeout or job unknown to worker
- `UNKNOWN_RETRY → UNKNOWN_STOPPED`: Job unknown to worker during cancellation
- `RUNNING → UNKNOWN_STOPPED`: Worker not connected (fallback case, indicates service bug)

**Backend Expectations**:
- Must transition job to `UNKNOWN_STOPPED` state atomically
- Must store error message and set `FinalizedAt` timestamp
- Must verify job exists before transition
- Must preserve `AssigneeID` and `AssignedAt` for historical tracking
- Must return error if job doesn't exist
- Must return `freedAssigneeIDs` based on whether job was assigned

**Error Conditions**:
- Returns error if job doesn't exist
- Returns error if database/storage operation fails
- Returns error if context is cancelled

#### UpdateJobStatus

**Signature**: `UpdateJobStatus(ctx context.Context, jobID string, status JobStatus, result []byte, errorMsg string) (map[string]int, error)`

**Preconditions**:
- `ctx` is a valid context
- `jobID` is non-empty
- Job exists in storage
- State transition is valid (per state machine rules)

**Postconditions**:
- If job exists and transition is valid:
  - Job status updated to specified status
  - `Result` field updated if provided (non-nil)
  - `ErrorMessage` field updated if provided (non-empty)
  - Timestamps updated based on status:
    - `StartedAt` set if transitioning to `RUNNING` and not already set
    - `FinalizedAt` set if transitioning to terminal state and not already set
  - `AssigneeID` and `AssignedAt` are preserved (not cleared) for historical tracking
  - Returns `freedAssigneeIDs` map:
    - Empty map if job was not assigned or not transitioning from RUNNING to terminal state
    - Contains `assignee_id` with count=1 if job was assigned and transitioning to terminal state (COMPLETED, STOPPED, UNKNOWN_STOPPED)
- If job not found or invalid transition:
  - Returns error

**Observable Effects**:
- Job state changes according to transition
- Capacity may be freed if transitioning from RUNNING to terminal state

**Backend Expectations**:
- Must update job status atomically
- Must validate that transition is allowed (per state machine rules)
- Must update result and error message if provided
- Must update timestamps based on status transitions
- Must preserve `AssigneeID` and `AssignedAt` for historical tracking
- Must return error if state transition is invalid
- Must return `freedAssigneeIDs` based on whether job was assigned and transitioning to terminal state
- Should be used sparingly - prefer atomic methods (CompleteJob, FailJob, etc.) for common transitions

**Error Conditions**:
- Returns error if job doesn't exist
- Returns error if state transition is invalid
- Returns error if database/storage operation fails
- Returns error if context is cancelled

**Usage Notes**:
- Use this method only for edge cases not covered by atomic methods
- Ensure related operations (e.g., retry increment) are handled separately if needed
- Prefer CompleteJob, FailJob, StopJob, etc. for common transitions

### Cancellation

#### CancelJobs

**Signature**: `CancelJobs(ctx context.Context, tags []string, jobIDs []string) ([]string, []string, error)`

**Preconditions**:
- `ctx` is a valid context
- `tags` is a slice (may be empty)
- `jobIDs` is a slice (may be empty)
- At least one of `tags` or `jobIDs` is non-empty

**Postconditions**:
- Jobs matching tags (AND logic) or jobIDs are processed
- State transitions applied based on current job state:
  - `INITIAL_PENDING → UNSCHEDULED`
  - `RUNNING → CANCELLING`
  - `FAILED_RETRY → STOPPED`
  - `UNKNOWN_RETRY → STOPPED`
- Jobs already in `CANCELLING` state are treated as no-op (no state change, included in `cancelledJobIDs` if they match the filter)
- Returns:
  - `cancelledJobIDs`: Jobs successfully cancelled (state changed or already in CANCELLING)
  - `unknownJobIDs`: Jobs not found or already in terminal states
  - Error if operation fails

**Observable Effects**:
- Jobs transition to terminal or `CANCELLING` states
- Jobs in `CANCELLING` state await acknowledgment

**Backend Expectations**:
- Must find jobs matching tags (AND logic) or jobIDs (union of both sets)
- Must apply state transitions based on current job state atomically
- Must return `cancelledJobIDs` (successfully cancelled) and `unknownJobIDs` (not found or terminal)
- Must handle terminal jobs gracefully (include in `unknownJobIDs`, don't fail operation)
- Must support efficient querying by tags and job IDs
- Must use batch operations for performance
- All cancellations should be atomic (transaction)

**Error Conditions**:
- Returns error if database/storage operation fails
- Returns error if context is cancelled
- Should not return error for terminal jobs (just include in `unknownJobIDs`)

#### AcknowledgeCancellation

**Signature**: `AcknowledgeCancellation(ctx context.Context, jobID string, wasExecuting bool) error`

**Preconditions**:
- `ctx` is a valid context
- `jobID` is non-empty
- Job exists in storage
- Job is in `CANCELLING` state

**Postconditions**:
- If `wasExecuting = true`:
  - Job status becomes `STOPPED`
  - `FinalizedAt` timestamp set to current time (if not already set)
- If `wasExecuting = false`:
  - Job status becomes `UNKNOWN_STOPPED`
  - `FinalizedAt` timestamp set to current time (if not already set)
- `AssigneeID` and `AssignedAt` are preserved (not cleared) for historical tracking
- If job not found or not in `CANCELLING` state:
  - Returns error

**Observable Effects**:
- Job transitions to terminal state
- Capacity is freed for the assignee (if job was assigned and wasExecuting=true)

**Valid State Transitions**:
- `CANCELLING → STOPPED` (if `wasExecuting=true`): Cancellation acknowledged by worker
- `CANCELLING → UNKNOWN_STOPPED` (if `wasExecuting=false`): Job unknown to worker or already finished

**Backend Expectations**:
- Must transition job from `CANCELLING` to `STOPPED` (if `wasExecuting=true`) or `UNKNOWN_STOPPED` (if `wasExecuting=false`) atomically
- Must verify job is in `CANCELLING` state before transition
- Must preserve `AssigneeID` and `AssignedAt` for historical tracking
- Must return error if job is not in `CANCELLING` state

**Error Conditions**:
- Returns error if job doesn't exist
- Returns error if job is not in `CANCELLING` state
- Returns error if database/storage operation fails
- Returns error if context is cancelled

### Worker Management

#### MarkWorkerUnresponsive

**Signature**: `MarkWorkerUnresponsive(ctx context.Context, assigneeID string) error`

**Preconditions**:
- `ctx` is a valid context
- `assigneeID` is non-empty

**Postconditions**:
- All jobs with `AssigneeID = assigneeID` transition:
  - `RUNNING → UNKNOWN_RETRY`
  - `CANCELLING → UNKNOWN_STOPPED`
- `AssigneeID` and `AssignedAt` are preserved (not cleared) for all affected jobs
- If operation fails:
  - Returns error
- If worker has no assigned jobs:
  - Operation is no-op (not an error)

**Observable Effects**:
- Jobs become eligible (`UNKNOWN_RETRY`) or terminal (`UNKNOWN_STOPPED`)
- Capacity is freed for the worker's subscriptions (when jobs transition from RUNNING to UNKNOWN_RETRY)

**Backend Expectations**:
- Must find all jobs with `AssigneeID = assigneeID`
- Must transition jobs atomically based on current state: `RUNNING → UNKNOWN_RETRY`, `CANCELLING → UNKNOWN_STOPPED`
- Must preserve `AssigneeID` and `AssignedAt` for all affected jobs (not cleared)
- Must handle case where worker has no assigned jobs (no-op, not an error)
- Must support efficient querying by `AssigneeID` and batch updates
- All transitions should be atomic (transaction)

**Error Conditions**:
- Returns error if database/storage operation fails
- Returns error if context is cancelled
- Should not return error if worker has no assigned jobs (no-op is valid)

### Query Operations

#### GetJob

**Signature**: `GetJob(ctx context.Context, jobID string) (*Job, error)`

**Preconditions**:
- `ctx` is a valid context
- `jobID` is non-empty

**Postconditions**:
- Returns job if exists, error if not found
- Job returned has all fields populated, including tags
- No observable side effects (read-only operation)

**Backend Expectations**:
- Must return complete job record with all fields populated
- Must return current state (read committed or stronger isolation)
- Must return error if job doesn't exist
- Must support efficient lookup by job ID
- Must include all tags associated with the job

**Error Conditions**:
- Returns error if job doesn't exist
- Returns error if database/storage operation fails
- Returns error if context is cancelled

#### GetJobStats

**Signature**: `GetJobStats(ctx context.Context, tags []string) (*JobStats, error)`

**Preconditions**:
- `ctx` is a valid context
- `tags` is a slice (may be empty)

**Postconditions**:
- Returns statistics for jobs matching ALL provided tags (AND logic)
- Empty tags returns stats for all jobs (no filtering, consistent with tag matching semantics)
- No observable side effects (read-only operation)

**Backend Expectations**:
- Must filter jobs by tags using AND logic (empty tags means no filtering)
- Must calculate statistics efficiently (use aggregated queries when possible)
- Must return consistent results (read committed or stronger isolation)
- Must support efficient querying by tags for performance
- Statistics include:
  - `TotalJobs`: Total number of jobs matching tags
  - `PendingJobs`: Number of jobs in `INITIAL_PENDING` state
  - `RunningJobs`: Number of jobs in `RUNNING` state
  - `CompletedJobs`: Number of jobs in `COMPLETED` state
  - `StoppedJobs`: Number of jobs in final states, except `COMPLETED`
  - `FailedJobs`: Number of failed jobs, including `UNKNOWN_RETRY`
  - `TotalRetries`: Total retry count across all jobs

**Error Conditions**:
- Returns error if database/storage operation fails
- Returns error if context is cancelled

### Maintenance

#### CleanupExpiredJobs

**Signature**: `CleanupExpiredJobs(ctx context.Context, ttl time.Duration) error`

**Preconditions**:
- `ctx` is a valid context
- `ttl > 0`

**Postconditions**:
- Deletes `COMPLETED` jobs older than `ttl`
- Returns error if operation fails

**Observable Effects**:
- Jobs removed from system
- Statistics updated

**Backend Expectations**:
- Must find `COMPLETED` jobs older than `ttl`
- Must delete matching jobs permanently
- Must support efficient time-based queries
- Should use batch delete operations when possible

**Error Conditions**:
- Returns error if `ttl <= 0`
- Returns error if database/storage operation fails
- Returns error if context is cancelled

#### ResetRunningJobs

**Signature**: `ResetRunningJobs(ctx context.Context) error`

**Preconditions**:
- `ctx` is a valid context

**Postconditions**:
- All `RUNNING` jobs transition to `UNKNOWN_RETRY`
- `AssigneeID` and `AssignedAt` are preserved (not cleared) for all affected jobs
- Returns error if operation fails
- If no running jobs exist:
  - Operation is no-op (not an error)

**Observable Effects**:
- Jobs become eligible for scheduling
- Capacity is freed for workers (jobs are unassigned)

**Backend Expectations**:
- Must find all jobs in `RUNNING` state
- Must transition all `RUNNING` jobs to `UNKNOWN_RETRY` atomically
- Must preserve `AssigneeID` and `AssignedAt` for all affected jobs (not cleared)
- Must handle case where no running jobs exist (no-op, not an error)
- Must support efficient querying by status and batch updates
- All transitions should be atomic (transaction)

**Error Conditions**:
- Returns error if database/storage operation fails
- Returns error if context is cancelled
- Should not return error if no running jobs exist (no-op is valid)

#### DeleteJobs

**Signature**: `DeleteJobs(ctx context.Context, tags []string, jobIDs []string) error`

**Preconditions**:
- `ctx` is a valid context
- `tags` is a slice (may be empty)
- `jobIDs` is a slice (may be empty)
- At least one of `tags` or `jobIDs` is non-empty

**Postconditions**:
- The method validates that all jobs matching tags (AND logic) or jobIDs are in final states (`COMPLETED`, `UNSCHEDULED`, `STOPPED`, `UNKNOWN_STOPPED`)
- If all matching jobs are in final states:
  - All matching jobs are deleted
  - Returns `nil` error
- If any matching job is not in final state:
  - No jobs are deleted (atomic operation)
  - Returns error
- If operation fails:
  - Returns error

**Observable Effects**:
- Jobs removed from system
- Statistics updated

**Backend Expectations**:
- Must find jobs matching tags (AND logic) or jobIDs (union of both sets)
- Must validate all matching jobs are in final states (`COMPLETED`, `UNSCHEDULED`, `STOPPED`, `UNKNOWN_STOPPED`)
- Must delete all matching jobs atomically (all-or-nothing)
- Must return error if any matching job is not in final state
- Must support efficient querying by tags and job IDs
- Should use batch delete operations when possible

**Error Conditions**:
- Returns error if any matching job is not in final state
- Returns error if database/storage operation fails
- Returns error if context is cancelled

### Resource Management

#### Close

**Signature**: `Close() error`

**Preconditions**:
- None

**Postconditions**:
- Backend closed
- All resources released
- Backend unusable after close (subsequent method calls have undefined behavior)
- Returns error if operation fails

**Observable Effects**:
- Backend becomes unusable
- Resources are cleaned up

**Backend Expectations**:
- Must close backend storage connection/resources
- Must handle cleanup gracefully
- Must return error if cleanup fails

**Error Conditions**:
- Returns error if cleanup fails

## Behavioral Invariants

The following invariants are maintained by all Backend implementations:

1. **Thread Safety**: All methods are safe for concurrent calls from multiple goroutines.

2. **Atomicity**: All state transitions and multi-field updates are atomic. Either all changes succeed or none do.

3. **State Transitions**: All state transitions follow well-defined rules. Jobs never return to `INITIAL_PENDING` once they leave it. `FAILED_RETRY` jobs remain in `FAILED_RETRY` state but are eligible for scheduling.

4. **Tag Matching**: AND logic - all provided tags must be present in job tags. Empty tags means accept all jobs. Tags are case-sensitive and order-independent.

5. **Eligibility**: Only jobs in `INITIAL_PENDING`, `FAILED_RETRY`, or `UNKNOWN_RETRY` states are eligible for scheduling via DequeueJobs. Jobs in other states are not assigned to workers until they change their state to eligible one.

6. **Assignment Semantics**: When jobs are dequeued via DequeueJobs, they are immediately assigned (status becomes RUNNING, assigneeID set). The Backend prevents reassignment of jobs in RUNNING state - a job in RUNNING state cannot be assigned to a different worker. Multiple workers cannot work on the same job.

7. **AssigneeID Preservation**: `AssigneeID` and `AssignedAt` are always preserved for historical tracking, even after job completion or failure. These fields are never cleared. To check if a job is currently assigned, check that `Status == RUNNING` rather than checking if `AssigneeID` is empty.

8. **freedAssigneeIDs Semantics**: Methods that free capacity return `freedAssigneeIDs` map indicating which assignees had capacity freed. The map is empty if no capacity was freed, or contains assignee IDs with counts if capacity was freed. This information is used by Queue for capacity management.

9. **Ordering**: Jobs are selected for assignment in ascending order by `COALESCE(LastRetryAt, CreatedAt)` (oldest first) as a selection criterion. The order of jobs in returned slices is not guaranteed to match this ordering.

## Observable State Changes

Clients can observe state changes through:

1. **Job State Transitions**: Via `GetJob` - observe job status, assignee, timestamps, retry count
2. **Capacity Changes**: Via `freedAssigneeIDs` return values - observe which assignees had capacity freed
3. **Statistics Changes**: Via `GetJobStats` - observe counts of jobs in various states matching tag filters
4. **Assignment State**: Via `GetJob` - observe which jobs are assigned to which assignees and when

## Error Conditions

1. **Context Cancellation**: All methods respect `ctx.Done()`. Methods that block return `ctx.Err()` when context is cancelled.

2. **Invalid States**: Methods return errors for invalid state transitions (e.g., completing a job that's already completed).

3. **Not Found**: Methods return errors when jobs don't exist.

4. **Backend Errors**: Errors from storage operations are propagated to callers.

5. **Invalid Parameters**: Methods return errors for invalid parameters (e.g., empty jobID, negative limit, nil job, invalid state transitions).

6. **Validation Failures**: Methods return errors when validation fails (e.g., duplicate job IDs, jobs not in final states for deletion).

## Integration Patterns

### Queue Usage of Backend

The Queue uses Backend methods in the following patterns:

1. **Job Submission**: Queue calls `EnqueueJob` or `EnqueueJobs` to create jobs, then notifies workers.

2. **Job Assignment**: Queue calls `DequeueJobs` to atomically assign jobs to workers. The Queue manages capacity tracking and worker notifications.

3. **Job Completion**: Queue calls `CompleteJob`, `FailJob`, `StopJob`, etc., then uses `freedAssigneeIDs` to free capacity for subscriptions and notify workers.

4. **Capacity Management**: Queue uses `freedAssigneeIDs` returned by lifecycle methods to update subscription capacity and notify workers about newly available capacity.

5. **Worker Management**: Queue calls `MarkWorkerUnresponsive` when workers disconnect, which transitions their jobs to eligible or terminal states.

### Implementation Guidelines

**Transaction Support**:
- All methods that modify state must support transactions
- Use database transactions for atomicity
- Ensure ACID properties (Atomicity, Consistency, Isolation, Durability)

**Indexing Requirements**:
- Maintain indexes on:
  - Job ID (primary key/index for GetJob, updates)
  - Status (for filtering by status in DequeueJobs, GetJobStats, ResetRunningJobs)
  - Tags (for tag-based filtering in DequeueJobs, GetJobStats, CancelJobs, DeleteJobs)
  - AssigneeID (for worker-specific queries in MarkWorkerUnresponsive)
  - CreatedAt/LastRetryAt (for ordering in DequeueJobs)

**Concurrency Control**:
- Use optimistic locking (version numbers or timestamps) to detect conflicts
- Use pessimistic locking (SELECT ... FOR UPDATE) for critical sections like DequeueJobs
- Prevent race conditions in DequeueJobs (prevent double-assignment)
- Use consistent lock ordering to prevent deadlocks

**Performance Expectations**:
- `GetJob`: O(log n) or better with proper indexing
- `DequeueJobs`: Real-time responsiveness (< 100ms for job assignment)
- `GetJobStats`: Efficient aggregation (< 500ms for large datasets)
- Batch operations should be more efficient than individual operations
- Support 10+ concurrent workers without degradation

**Error Handling**:
- All methods must handle errors consistently
- Return appropriate errors for context cancellation, not found, invalid state, storage failures
- Never return partial results on error (atomic operations)

## Testing Requirements

Backend implementations should be tested for:

1. **State Transition Correctness**: All valid transitions work, invalid transitions fail
2. **Atomicity**: Operations are atomic (no partial updates)
3. **Concurrency**: Multiple workers can operate concurrently without conflicts
4. **Performance**: Meets performance expectations under load
5. **Error Handling**: All error cases are handled correctly
6. **Consistency**: Data remains consistent after operations
7. **Durability**: Changes are persisted correctly
8. **freedAssigneeIDs Accuracy**: Return values correctly reflect capacity freed
9. **Tag Filtering**: AND logic works correctly, empty tags means no filtering
10. **Ordering**: Jobs are selected in correct order (oldest first)

