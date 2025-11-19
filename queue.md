# Queue API Specification

## Introduction

The Queue interface provides a thread-safe job queue with push-based job assignment, capacity management, and tag-based filtering. All operations are thread-safe and can be called concurrently from multiple goroutines.

The Queue interface delegates storage operations to a Backend implementation while providing additional functionality for worker notification, capacity management, and automatic job pushing.

### Core Concepts

- **Assignee**: A worker identifier (assigneeID) that can process jobs.
- **Capacity**: The maximum number of jobs that can be assigned to a single StreamJobs call simultaneously (maxAssignedJobs). Each StreamJobs call has its own independent capacity.
- **Tags**: String labels attached to jobs for filtering. Tag matching uses AND logic: a job matches a worker's tag filter if the job contains ALL of the worker's tags.
- **Job States**: Jobs transition through states (INITIAL_PENDING, RUNNING, COMPLETED, FAILED_RETRY, STOPPED, etc.). Only INITIAL_PENDING, FAILED_RETRY, and UNKNOWN_RETRY states are eligible for scheduling.
- **Eligibility**: Jobs in INITIAL_PENDING, FAILED_RETRY, or UNKNOWN_RETRY states can be assigned to workers. Jobs in other states are not eligible for scheduling.
- **Worker Notification**: When workers are "notified", it means that active StreamJobs calls (which are blocking waiting for jobs) are signaled to wake up and attempt to dequeue eligible jobs. Notification is observable: after notification, workers with matching tag filters and available capacity will receive jobs via their StreamJobs channel. All subscriptions (StreamJobs invocations) having free capacity and matching tags of at least one incoming job are notified, but notification is sent only once for each subscription. Notifications are mostly non-blocking (buffered) and at-most-once per event (deduplicated for same tag combinations in EnqueueJobs).

## Core Types

Before examining the interface, it is important to understand the core data types used throughout the Queue API.

### Job

A Job represents a unit of work in the queue. All fields are relevant for Queue behavior:

- `ID`: Unique job identifier
- `Status`: Current job state (JobStatus)
- `JobType`: Job type identifier
- `JobDefinition`: Serialized job definition (protobuf or JSON)
- `Tags`: String labels for filtering (empty slice means no tags)
- `CreatedAt`: Timestamp when the job was created
- `StartedAt`: Timestamp when job started processing (nil if not started). Set on the first transition to RUNNING state.
- `FinalizedAt`: Timestamp when job completed (nil if not completed). Always set when job transits to a final status (COMPLETED, STOPPED, UNKNOWN_STOPPED, UNSCHEDULED).
- `ErrorMessage`: Error message if job failed
- `Result`: Serialized result (if completed)
- `RetryCount`: Number of times job has been retried
- `LastRetryAt`: Timestamp of last retry (nil if never retried)
- `AssigneeID`: ID of the last worker that was assigned to this job. This field stores the most recent assignment and is preserved even after job completion/failure for historical tracking. To check if a job is currently assigned, check that `Status == RUNNING` rather than checking if `AssigneeID` is empty.
- `AssignedAt`: Timestamp of the last assignment of this job to a worker. This field stores the most recent assignment timestamp and is preserved even after job completion/failure for historical tracking. To check if a job is currently assigned, check that `Status == RUNNING` rather than checking if `AssignedAt` is nil.

### JobStatus

Job states and their semantics:

- `INITIAL_PENDING`: Job waiting to be processed (eligible for scheduling)
- `RUNNING`: Job currently being processed (not eligible)
- `COMPLETED`: Job completed successfully (terminal, not eligible)
- `FAILED_RETRY`: Job failed during processing (eligible for scheduling)
- `STOPPED`: Job was cancelled while running (terminal, not eligible)
- `UNSCHEDULED`: Job was cancelled before assignment (terminal, not eligible)
- `UNKNOWN_RETRY`: Job was assigned to unresponsive worker (eligible for scheduling)
- `CANCELLING`: Job cancellation requested (transitional, not eligible)
- `UNKNOWN_STOPPED`: Job cancellation failed or unknown to worker (terminal, not eligible)

**Important**: Once a job leaves INITIAL_PENDING state, it never returns to INITIAL_PENDING. FAILED_RETRY jobs remain in FAILED_RETRY state but are eligible for scheduling.

**UNKNOWN_* States Semantics**: UNKNOWN_* states (UNKNOWN_RETRY, UNKNOWN_STOPPED) are called "unknown" because the Queue does not know their actual state. These states may potentially transition to any other state, as the actual state is uncertain. 

### JobStats

Statistics for jobs matching tag filters:

- `Tags`: Tags used for query
- `TotalJobs`: Total number of jobs matching tags
- `PendingJobs`: Number of pending jobs
- `RunningJobs`: Number of running jobs
- `CompletedJobs`: Number of completed jobs
- `StoppedJobs`: Number of jobs in final states, except COMPLETED
- `FailedJobs`: Number of failed jobs, including UNKNOWN_RETRY
- `TotalRetries`: Total retry count across all jobs

### Tag Matching

Tag matching uses AND logic:
- A job matches a worker's tag filter if the job contains ALL tags specified by the worker
- Empty worker tags (nil or empty slice) means "accept all jobs"
- Tags are case-sensitive and order-independent
- Job tags may contain additional tags beyond those required by the worker

**Examples**:
- Worker tags: `["tag1", "tag2"]` matches job tags: `["tag1", "tag2"]` ✓
- Worker tags: `["tag1", "tag2"]` matches job tags: `["tag1", "tag2", "tag3"]` ✓
- Worker tags: `["tag1", "tag2"]` does not match job tags: `["tag1"]` ✗
- Worker tags: `[]` matches any job tags ✓

## Interface Definition

The Queue interface defines all methods available for job queue operations:

```go
type Queue interface {
    // Job submission
    EnqueueJob(ctx context.Context, job *Job) (string, error)
    EnqueueJobs(ctx context.Context, jobs []*Job) ([]string, error)

    // Job streaming (push-based)
    StreamJobs(ctx context.Context, assigneeID string, tags []string, maxAssignedJobs int, ch chan<- []*Job) error

    // Job lifecycle
    CompleteJob(ctx context.Context, jobID string, result []byte) error
    FailJob(ctx context.Context, jobID string, errorMsg string) error
    StopJob(ctx context.Context, jobID string, errorMsg string) error
    StopJobWithRetry(ctx context.Context, jobID string, errorMsg string) error
    MarkJobUnknownStopped(ctx context.Context, jobID string, errorMsg string) error

    // Cancellation
    CancelJobs(ctx context.Context, tags []string, jobIDs []string) ([]string, []string, error)
    AcknowledgeCancellation(ctx context.Context, jobID string, wasExecuting bool) error

    // Worker management
    MarkWorkerUnresponsive(ctx context.Context, assigneeID string) error

    // Query operations
    GetJob(ctx context.Context, jobID string) (*Job, error)
    GetJobStats(ctx context.Context, tags []string) (*JobStats, error)

    // Maintenance
    CleanupExpiredJobs(ctx context.Context, ttl time.Duration) error
    ResetRunningJobs(ctx context.Context) error
    DeleteJobs(ctx context.Context, tags []string, jobIDs []string) error

    Close() error
}
```

## Behavioral Contracts

The following sections specify the behavior of each Queue method using preconditions, postconditions, and observable effects. Methods are organized by functional category.

### Job Submission

#### EnqueueJob

**Signature**: `EnqueueJob(ctx context.Context, job *Job) (string, error)`

**Preconditions**:
- `ctx` is a valid context
- `job` is non-nil
- `job.Status` is `INITIAL_PENDING`
- `job.ID` is non-empty

**Postconditions**:
- If successful:
  - Returns `job.ID` and `nil` error
  - Job is stored with `INITIAL_PENDING` status
  - Workers with matching tags are notified (observable via StreamJobs receiving jobs)
- If unsuccessful:
  - Returns empty string and non-nil error

**Observable Effects**:
- Job becomes available for StreamJobs calls with matching tag filters
- Workers may receive the job via their StreamJobs channel

**Backend Expectations**:
- Must create job record with unique ID and store all job fields
- Must set initial status to `INITIAL_PENDING`
- Operation must be atomic (all-or-nothing)
- Must return error if job ID already exists
- Must support concurrent job creation

#### EnqueueJobs

**Signature**: `EnqueueJobs(ctx context.Context, jobs []*Job) ([]string, error)`

**Preconditions**:
- `ctx` is a valid context
- `jobs` is a slice (may be empty)

**Postconditions**:
- If successful:
  - Returns slice of job IDs in same order as input jobs and `nil` error
  - All jobs are stored with `INITIAL_PENDING` status
  - Workers notified per unique tag combination (deduplicated)
- If unsuccessful:
  - Returns partial job IDs (for jobs successfully enqueued) and non-nil error

**Observable Effects**:
- All successfully enqueued jobs become available for matching StreamJobs
- Workers may receive jobs via their StreamJobs channels

**Backend Expectations**:
- Must create all jobs atomically (all-or-nothing)
- Must ensure each job has unique ID (no duplicates within batch or with existing jobs)
- Should be more efficient than multiple `EnqueueJob` calls
- Must return error if any job fails validation

### Job Streaming

#### StreamJobs

**Signature**: `StreamJobs(ctx context.Context, assigneeID string, tags []string, maxAssignedJobs int, ch chan<- []*Job) error`

**Parameters**:
- `maxAssignedJobs`: Maximum number of jobs that can be assigned to this StreamJobs call at any time (capacity limit). Each StreamJobs call has its own independent capacity. The Queue attempts to push as many eligible jobs as possible in a single batch while keeping the number of assigned jobs within the limit.

**Preconditions**:
- `ctx` is a valid context
- `assigneeID` is non-empty
- `maxAssignedJobs > 0`
- `ch` is non-nil and not closed

**Postconditions**:
- Method blocks until one of:
  - Context is cancelled
  - Queue is closed
  - An error occurs
- Jobs sent to channel have:
  - `Status = RUNNING`
  - `AssigneeID = assigneeID`
  - `AssignedAt` set to current time
- Jobs are filtered by tags (AND logic: all worker tags must be in job tags)
- Empty tags means accept all jobs
- Jobs are selected in ascending order by `COALESCE(LastRetryAt, CreatedAt)` (oldest first) as a selection criterion
- The order of jobs in slices sent to the channel is not guaranteed to match this ordering
- Total assigned jobs for this StreamJobs call never exceeds `maxAssignedJobs`
- **Channel ownership**: The Queue closes the channel when StreamJobs returns (on context cancellation, queue close, or error). The caller must NOT close the channel. Closing is idempotent (safe if already closed).

**Observable Effects**:
- Initial dequeue: If capacity available when StreamJobs starts, eligible jobs are immediately dequeued and sent
- Post-registration notification: After the initial dequeue, if capacity is still available, the subscription is notified to trigger another dequeue attempt. This ensures that if there are many pending jobs (e.g., after service restart when ResetRunningJobs was called before workers connected), all eligible jobs will be discovered and processed. This is especially important when jobs are already pending in the queue when StreamJobs is first called
- Capacity tracking: Capacity initialized to `maxAssignedJobs` when StreamJobs is called
- Capacity decrement: When jobs are dequeued and assigned, capacity decreases by number of jobs assigned (capacity is decremented on assignment, not on channel send)
- Capacity increment: When jobs are freed (via CompleteJob, FailJob, etc.), capacity increases (up to maxAssignedJobs)
- Job pushing: When both capacity is available and eligible jobs exist, jobs are dequeued, assigned, and pushed directly to channel. This occurs when: (1) capacity becomes available and eligible jobs exist, or (2) eligible jobs become available and capacity exists
- Channel full behavior: If channel is full when jobs are pushed, the jobs are still assigned (status becomes RUNNING, capacity decremented) but delivery to the channel may be delayed. Jobs assigned but not yet delivered remain in RUNNING state and are not eligible for re-assignment

**Termination**:
StreamJobs blocks until one of the following occurs:
- **Context cancellation**: Returns `ctx.Err()` when the context is cancelled (normal termination)
- **Queue closure**: Returns `nil` when the queue itself is closed
- **Error**: Returns a non-nil error if an operation fails (error termination)
- **Channel is closed by Queue** when StreamJobs returns (regardless of return reason)
- **Job cleanup on termination**: When StreamJobs terminates, any jobs that were assigned (status RUNNING) but not yet delivered to the channel are transitioned to `FAILED_RETRY` state with context cancellation error message. This ensures jobs are not stuck in RUNNING state when the StreamJobs call ends. The Queue maintains internal state mapping to track which jobs belong to which StreamJobs call (implementation detail).

**Usage Notes**:
- Must be called in a goroutine (method blocks)
- Each StreamJobs call has its own independent capacity (maxAssignedJobs)
- Each StreamJobs call has its own tag filter; jobs are filtered by the tags of the specific StreamJobs call
- Channel must be consumed by caller (not blocking the channel)
- **Caller must NOT close the channel** - the Queue closes it when StreamJobs returns
- If channel is full, jobs are still assigned and will be received on subsequent iterations

**Backend Expectations**:
- Must select jobs eligible for scheduling: `INITIAL_PENDING`, `FAILED_RETRY`, `UNKNOWN_RETRY`
- Must filter by tags using AND logic (job must have ALL provided tags; empty tags means no filtering)
- Must atomically assign selected jobs to `assigneeID` (set `AssigneeID`, `AssignedAt`, transition to `RUNNING`)
- Must prevent same job from being assigned to multiple workers concurrently
- Must return up to `maxAssignedJobs` jobs per batch
- Must block when no eligible jobs available (until jobs become available or context cancelled)
- Must wake up when new jobs are enqueued or jobs transition to eligible states
- Must support efficient querying by status and tags for real-time responsiveness

### Job Lifecycle

#### CompleteJob

**Signature**: `CompleteJob(ctx context.Context, jobID string, result []byte) error`

**Preconditions**:
- `ctx` is a valid context
- `jobID` is non-empty

**Postconditions**:
- If job exists and in valid state:
  - Job status becomes `COMPLETED`
  - `Result` field set to provided result
  - `FinalizedAt` timestamp set
  - `AssigneeID` and `AssignedAt` are preserved (not cleared) for historical tracking. To check if a job is currently assigned, check that `Status == RUNNING` rather than checking if `AssigneeID` is empty.
  - Capacity freed for the StreamJobs call that had this job assigned (observable via StreamJobs receiving new jobs)
  - Workers with matching tags notified
- If job not found or invalid state:
  - Returns error

**Observable Effects**:
- Job will never ever be eligible for scheduling
- Capacity increases for the StreamJobs call that had this job assigned
- Workers may receive new jobs via StreamJobs

**Valid State Transitions**:
- `RUNNING → COMPLETED`
- `CANCELLING → COMPLETED`
- `UNKNOWN_RETRY → COMPLETED` (if job is in UNKNOWN_RETRY state, CompleteJob transitions it to COMPLETED)
- `UNKNOWN_STOPPED → COMPLETED` (UNKNOWN_* states may transition to any state as their actual state is unknown)

**Backend Expectations**:
- Must transition job to `COMPLETED` state atomically
- Must store result bytes and set `FinalizedAt` timestamp
- Must verify job exists and is in valid state before transition
- Must preserve `AssigneeID` and `AssignedAt` for historical tracking
- Must return error if job is in invalid state (terminal states)

#### FailJob

**Signature**: `FailJob(ctx context.Context, jobID string, errorMsg string) error`

**Preconditions**:
- `ctx` is a valid context
- `jobID` is non-empty
- `errorMsg` is non-empty

**Postconditions**:
- If job exists and in valid state:
  - Job status becomes `FAILED_RETRY`
  - `ErrorMessage` field set
  - `RetryCount` incremented by 1
  - `LastRetryAt` timestamp set
  - `AssigneeID` and `AssignedAt` are preserved (not cleared) for historical tracking. To check if a job is currently assigned, check that `Status == RUNNING` rather than checking if `AssigneeID` is empty.
  - Capacity freed for the StreamJobs call that had this job assigned
  - Workers with matching tags notified
- If job not found or invalid state:
  - Returns error

**Observable Effects**:
- Job becomes eligible for scheduling again (but remains in FAILED_RETRY state)
- Capacity increases for the StreamJobs call that had this job assigned
- Workers notified of eligible job

**Valid State Transitions**:
- `RUNNING → FAILED_RETRY`
- `UNKNOWN_RETRY → FAILED_RETRY`

**Note**: CompleteJob and FailJob do not carry assigneeID parameter, so the Queue does not know (and does not care) which worker actually called these methods. If a job is in UNKNOWN_RETRY state and CompleteJob or FailJob is called, the job transitions to COMPLETED or FAILED_RETRY respectively.

**Backend Expectations**:
- Must transition job to `FAILED_RETRY` state atomically
- Must increment `RetryCount` by 1 and set `LastRetryAt` timestamp
- Must store error message and set `CompletedAt` timestamp
- Must verify job exists and is in valid state before transition
- Must preserve `AssigneeID` and `AssignedAt` for historical tracking
- Must return error if job is in invalid state (terminal states)

#### StopJob

**Signature**: `StopJob(ctx context.Context, jobID string, errorMsg string) error`

**Preconditions**:
- `ctx` is a valid context
- `jobID` is non-empty
- `errorMsg` is optional (may be empty string, unlike FailJob which requires non-empty errorMsg)

**Postconditions**:
- If job exists and in valid state:
  - Job status becomes `STOPPED`
  - `ErrorMessage` field set (may be empty if errorMsg was empty)
  - `FinalizedAt` timestamp set
  - `AssigneeID` and `AssignedAt` are preserved (not cleared) for historical tracking. To check if a job is currently assigned, check that `Status == RUNNING` rather than checking if `AssigneeID` is empty.
  - Capacity freed for the StreamJobs call that had this job assigned
- If job not found or invalid state:
  - Returns error

**Observable Effects**:
- Job no longer eligible for scheduling
- Capacity increases for the StreamJobs call that had this job assigned

**Valid State Transitions**:
- `RUNNING → STOPPED`
- `CANCELLING → STOPPED`
- `UNKNOWN_RETRY → STOPPED`

**Backend Expectations**:
- Must transition job to `STOPPED` state atomically
- Must store error message (may be empty) and set `FinalizedAt` timestamp
- Must verify job exists and is in valid state before transition
- Must preserve `AssigneeID` and `AssignedAt` for historical tracking
- Must return error if job is in invalid state (already terminal)

#### StopJobWithRetry

**Signature**: `StopJobWithRetry(ctx context.Context, jobID string, errorMsg string) error`

**Preconditions**:
- `ctx` is a valid context
- `jobID` is non-empty
- Job is in `CANCELLING` state

**Postconditions**:
- If job exists and in `CANCELLING` state:
  - Job status becomes `STOPPED`
  - `ErrorMessage` field set
  - `RetryCount` incremented by 1
  - `LastRetryAt` timestamp set
  - `FinalizedAt` timestamp set
  - `AssigneeID` and `AssignedAt` are preserved (not cleared) for historical tracking. To check if a job is currently assigned, check that `Status == RUNNING` rather than checking if `AssigneeID` is empty.
  - Capacity freed for the StreamJobs call that had this job assigned
- If job not found or not in `CANCELLING` state:
  - Returns error

**Observable Effects**:
- Job no longer eligible for scheduling
- Capacity increases for the StreamJobs call that had this job assigned

**Valid State Transitions**:
- `CANCELLING → STOPPED` (with retry increment)

**Backend Expectations**:
- Must transition job from `CANCELLING` to `STOPPED` state atomically
- Must increment `RetryCount` by 1 and set `LastRetryAt` timestamp (applies retry increment from transitory FAILED state)
- Must store error message and set `FinalizedAt` timestamp
- Must verify job is in `CANCELLING` state before transition
- Must preserve `AssigneeID` and `AssignedAt` for historical tracking
- Must return error if job is not in `CANCELLING` state

#### MarkJobUnknownStopped

**Signature**: `MarkJobUnknownStopped(ctx context.Context, jobID string, errorMsg string) error`

**Preconditions**:
- `ctx` is a valid context
- `jobID` is non-empty

**Postconditions**:
- If job exists:
  - Job status becomes `UNKNOWN_STOPPED`
  - `ErrorMessage` field set
  - `FinalizedAt` timestamp set
  - `AssigneeID` and `AssignedAt` are preserved (not cleared) for historical tracking. To check if a job is currently assigned, check that `Status == RUNNING` rather than checking if `AssigneeID` is empty.
  - Capacity freed for the StreamJobs call that had this job assigned
- If job not found:
  - Returns error

**Observable Effects**:
- Job no longer eligible for scheduling
- Capacity increases for the StreamJobs call that had this job assigned

**Valid State Transitions**:
- `CANCELLING → UNKNOWN_STOPPED`
- `UNKNOWN_RETRY → UNKNOWN_STOPPED`
- `RUNNING → UNKNOWN_STOPPED`

**Backend Expectations**:
- Must transition job to `UNKNOWN_STOPPED` state atomically
- Must store error message and set `FinalizedAt` timestamp
- Must verify job exists before transition
- Must preserve `AssigneeID` and `AssignedAt` for historical tracking
- Must return error if job doesn't exist

### Cancellation

#### CancelJobs

**Signature**: `CancelJobs(ctx context.Context, tags []string, jobIDs []string) ([]string, []string, error)`

**Preconditions**:
- `ctx` is a valid context

**Postconditions**:
- Jobs matching tags (AND logic) or jobIDs are processed
- State transitions:
  - `INITIAL_PENDING → UNSCHEDULED`
  - `RUNNING → CANCELLING`
  - `FAILED_RETRY → STOPPED`
  - `UNKNOWN_RETRY → STOPPED`
- Jobs already in `CANCELLING` state are treated as no-op (no state change, included in `cancelledJobIDs` if they match the filter)
- Returns:
  - `cancelledJobIDs`: Jobs successfully cancelled
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

#### AcknowledgeCancellation

**Signature**: `AcknowledgeCancellation(ctx context.Context, jobID string, wasExecuting bool) error`

**Preconditions**:
- `ctx` is a valid context
- `jobID` is non-empty
- Job is in `CANCELLING` state

**Postconditions**:
- If `wasExecuting = true`:
  - Job status becomes `STOPPED`
  - `FinalizedAt` timestamp set to current time (if not already set)
  - Capacity is freed for the StreamJobs call that had this job assigned (job was actively executing when cancelled)
- If `wasExecuting = false`:
  - Job status becomes `UNKNOWN_STOPPED`
  - `FinalizedAt` timestamp set to current time (if not already set)
  - Capacity is not freed (job was not executing, so no capacity was held)
- `AssigneeID` and `AssignedAt` are preserved (not cleared) for historical tracking
- If job not found or not in `CANCELLING` state:
  - Returns error

**Observable Effects**:
- Job transitions to terminal state
- Capacity increases for the StreamJobs call that had this job assigned only if `wasExecuting=true` (job was actively executing when cancelled)

**Backend Expectations**:
- Must transition job from `CANCELLING` to `STOPPED` (if `wasExecuting=true`) or `UNKNOWN_STOPPED` (if `wasExecuting=false`) atomically
- Must verify job is in `CANCELLING` state before transition
- Must preserve `AssigneeID` and `AssignedAt` for historical tracking
- Must return error if job is not in `CANCELLING` state

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
- `AssigneeID` and `AssignedAt` are preserved (not cleared) for all affected jobs. To check if a job is currently assigned, check that `Status == RUNNING` rather than checking if `AssigneeID` is empty.
- Capacity is freed for the StreamJobs call(s) that had these jobs assigned (when jobs transition from RUNNING to UNKNOWN_RETRY)
- All waiting workers notified (jobs may have become eligible)
- If operation fails:
  - Returns error

**Observable Effects**:
- Jobs become eligible (`UNKNOWN_RETRY`) or terminal (`UNKNOWN_STOPPED`)
- Workers notified of newly eligible jobs

**Backend Expectations**:
- Must find all jobs with `AssigneeID = assigneeID`
- Must transition jobs atomically based on current state: `RUNNING → UNKNOWN_RETRY`, `CANCELLING → UNKNOWN_STOPPED`
- Must preserve `AssigneeID` and `AssignedAt` for all affected jobs (not cleared)
- Must handle case where worker has no assigned jobs (no-op, not an error)
- Must support efficient querying by `AssigneeID` and batch updates

### Query Operations

#### GetJob

**Signature**: `GetJob(ctx context.Context, jobID string) (*Job, error)`

**Preconditions**:
- `ctx` is a valid context
- `jobID` is non-empty

**Postconditions**:
- Returns job if exists, error if not found
- No observable side effects (read-only operation)

**Backend Expectations**:
- Must return complete job record with all fields populated
- Must return current state (read committed or stronger isolation)
- Must return error if job doesn't exist
- Must support efficient lookup by job ID

#### GetJobStats

**Signature**: `GetJobStats(ctx context.Context, tags []string) (*JobStats, error)`

**Preconditions**:
- `ctx` is a valid context

**Postconditions**:
- Returns statistics for jobs matching ALL provided tags (AND logic)
- Empty tags returns stats for all jobs (no filtering, consistent with tag matching semantics)
- No observable side effects (read-only operation)

**Backend Expectations**:
- Must filter jobs by tags using AND logic (empty tags means no filtering)
- Must calculate statistics efficiently (use aggregated queries when possible)
- Must return consistent results (read committed or stronger isolation)
- Must support efficient querying by tags for performance

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

#### ResetRunningJobs

**Signature**: `ResetRunningJobs(ctx context.Context) error`

**Preconditions**:
- `ctx` is a valid context

**Postconditions**:
- All `RUNNING` jobs transition to `UNKNOWN_RETRY` (eligible for retry)
- All `CANCELLING` jobs transition to `UNKNOWN_STOPPED` (terminal state, cancellation was in progress)
- `AssigneeID` and `AssignedAt` are preserved (not cleared) for all affected jobs. To check if a job is currently assigned, check that `Status == RUNNING` rather than checking if `AssigneeID` is empty.
- Returns error if operation fails
- If no running or cancelling jobs exist:
  - Operation is no-op (not an error)

**Observable Effects**:
- RUNNING jobs become eligible for scheduling (transition to UNKNOWN_RETRY)
- CANCELLING jobs become terminal (transition to UNKNOWN_STOPPED)
- Capacity is freed for workers (RUNNING jobs are unassigned and become eligible)
- All active workers with matching tags are notified about newly eligible jobs
- **Note**: If ResetRunningJobs is called before workers connect (e.g., during service startup), notifications are sent but no subscriptions exist yet. In this case, when workers later connect and call StreamJobs, the post-registration notification mechanism ensures they will discover and process the newly eligible jobs
- Workers may receive jobs via StreamJobs

**Backend Expectations**:
- Must find all jobs in `RUNNING` state and transition them to `UNKNOWN_RETRY`
- Must find all jobs in `CANCELLING` state and transition them to `UNKNOWN_STOPPED`
- Must transition all jobs atomically (single transaction)
- Must preserve `AssigneeID` and `AssignedAt` for all affected jobs (not cleared)
- Must handle case where no running or cancelling jobs exist (no-op, not an error)
- Must support efficient querying by status and batch updates

#### DeleteJobs

**Signature**: `DeleteJobs(ctx context.Context, tags []string, jobIDs []string) error`

**Preconditions**:
- `ctx` is a valid context

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

### Resource Management

#### Close

**Signature**: `Close() error`

**Preconditions**:
- None

**Postconditions**:
- All active StreamJobs notified (channels closed)
- All StreamJobs calls terminate
- Backend closed
- Queue unusable after close (subsequent method calls have undefined behavior)
- Returns error if operation fails

**Observable Effects**:
- All StreamJobs terminate
- Channels closed
- Queue becomes unusable

**Backend Expectations**:
- Must close backend storage connection/resources
- Must handle cleanup gracefully
- Must return error if cleanup fails

## Behavioral Invariants

The following invariants are maintained by all Queue implementations:

1. **Thread Safety**: All methods are safe for concurrent calls from multiple goroutines.

2. **Capacity Limits**: Total assigned jobs per StreamJobs call never exceeds `maxAssignedJobs` specified in that call. Each StreamJobs call has its own independent capacity.

3. **Ordering**: Jobs are selected for assignment in ascending order by `COALESCE(LastRetryAt, CreatedAt)` (oldest first) as a selection criterion. The order of jobs in slices sent to the channel is not guaranteed to match this ordering.

4. **Tag Matching**: AND logic - all worker tags must be present in job tags. Empty worker tags means accept all jobs. Each StreamJobs call has its own tag filter.

5. **Eligibility**: Only jobs in `INITIAL_PENDING`, `FAILED_RETRY`, or `UNKNOWN_RETRY` states are eligible for scheduling. Jobs in other states are not assigned to workers until they change their state to eligible one.

6. **State Transitions**: All state transitions are atomic and follow well-defined rules. Jobs never return to `INITIAL_PENDING` once they leave it. `FAILED_RETRY` jobs remain in `FAILED_RETRY` state but are eligible for scheduling.

7. **Notification**: Notifications are mostly non-blocking (buffered) and at-most-once per event (deduplicated for same tag combinations in EnqueueJobs). All subscriptions (StreamJobs invocations) having free capacity and matching tags of at least one incoming job are notified, but notification is sent only once for each subscription. As a result, at least one subscription will dequeue some of the incoming jobs.

8. **Assignment Semantics**: When jobs are dequeued, they are immediately assigned (status becomes RUNNING, assigneeID set). This occurs even if the channel send fails or is delayed. The Queue prevents reassignment of jobs in RUNNING state - a job in RUNNING state cannot be assigned to a different worker. Multiple workers cannot work on the same job. CompleteJob and FailJob do not carry assigneeID parameter, so the Queue does not know (and does not care) which worker actually called these methods.

9. **Capacity Semantics**: Capacity is decremented when jobs are assigned (dequeued), not when jobs are sent to channel. Capacity is incremented when jobs are freed (CompleteJob, FailJob, etc.), up to the maximum.

## Observable State Changes

Clients can observe state changes through:

1. **Job State Transitions**: Via `GetJob` - observe job status, assignee, timestamps, retry count
2. **Capacity Changes**: Via `StreamJobs` - observe jobs being received when capacity becomes available. Capacity is observable indirectly: when jobs are received, capacity was available; when no jobs are received despite eligible jobs existing, capacity is exhausted.
3. **Notification Delivery**: Via `StreamJobs` - observe jobs being received after events (enqueue, fail, complete, etc.). Notifications cause StreamJobs to wake up and attempt dequeue.
4. **Statistics Changes**: Via `GetJobStats` - observe counts of jobs in various states matching tag filters
5. **Assignment State**: Via `GetJob` - observe which jobs are assigned to which assignees and when

## Error Conditions

1. **Context Cancellation**: All methods respect `ctx.Done()`. Methods that block (e.g., StreamJobs) return `ctx.Err()` when context is cancelled.

2. **Invalid States**: Methods return errors for invalid state transitions (e.g., completing a job that's already completed).

3. **Not Found**: Methods return errors when jobs don't exist.

4. **Backend Errors**: Errors from Backend operations are propagated to callers.

5. **Invalid Parameters**: Methods return errors for invalid parameters (e.g., empty jobID, negative maxAssignedJobs, nil job, closed channel).

6. **Queue Closed**: Methods called after `Close()` have undefined behavior. StreamJobs calls started before `Close()` will terminate gracefully.

## Integration Patterns

### StreamJobs Usage

StreamJobs must be called in a goroutine as it blocks:

```go
go func() {
    err := queue.StreamJobs(ctx, "worker-1", []string{"tag1"}, 10, jobChan)
    if err != nil {
        // Handle error
    }
}()
```

### Worker Pattern

Workers use StreamJobs for push-based job assignment:

1. Call StreamJobs with assigneeID, tags, and maxAssignedJobs
2. Receive jobs from channel
3. Process jobs
4. Call CompleteJob or FailJob when done
5. Capacity automatically freed, new jobs pushed

### Capacity Management

- Set `maxAssignedJobs` based on worker capacity
- Observe capacity indirectly: when jobs are received, capacity was available; when no jobs are received despite eligible jobs existing, capacity is exhausted
- Capacity is decremented when jobs are assigned (dequeued), not when sent to channel
- Capacity is incremented when jobs are freed (CompleteJob, FailJob, StopJob, etc.)
- Each StreamJobs call has its own independent capacity
- Capacity automatically managed by Queue

### Error Handling

- Always check errors from Queue methods
- Respect context cancellation
- Handle channel closure in StreamJobs consumers (channel is closed by Queue when StreamJobs returns)
- Do NOT close the channel passed to StreamJobs - Queue owns and closes it
- Use context with timeout for operations that may block

