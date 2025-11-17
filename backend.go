package jobpool

import (
	"context"
	"time"
)

// Backend represents the interface for job pool storage backends.
// Implementations must be thread-safe and support concurrent operations.
type Backend interface {
	// Job lifecycle
	// EnqueueJob enqueues a single job
	EnqueueJob(ctx context.Context, job *Job) (string, error)

	// EnqueueJobs enqueues multiple jobs in a batch
	EnqueueJobs(ctx context.Context, jobs []*Job) ([]string, error)

	// Job assignment (atomic: INITIAL_PENDING/FAILED_RETRY/UNKNOWN_RETRY → RUNNING)
	// DequeueJobs dequeues pending jobs up to the limit and assigns them to the given assigneeID
	// tags: Filter jobs by tags using AND logic (jobs must have ALL provided tags). Empty slice means no filtering.
	DequeueJobs(ctx context.Context, assigneeID string, tags []string, limit int) ([]*Job, error)

	// Job completion (atomic: RUNNING/CANCELLING/UNKNOWN_RETRY/UNKNOWN_STOPPED → COMPLETED)
	// CompleteJob atomically transitions a job to COMPLETED with the given result.
	//
	// Purpose: Use this method when a job has finished successfully, regardless of its current state.
	// This is the primary method for marking jobs as completed.
	//
	// Atomic operations:
	// - Updates job status to COMPLETED
	// - Sets result field with provided result data
	// - Sets finalized_at timestamp to current time
	// - Sets started_at timestamp if not already set
	// - Preserves assignee_id and assigned_at for historical tracking (not cleared)
	//
	// State transitions:
	// - RUNNING → COMPLETED: Normal completion flow
	// - CANCELLING → COMPLETED: Job completed before cancellation took effect
	// - UNKNOWN_RETRY → COMPLETED: Job completed after worker reconnection
	// - UNKNOWN_STOPPED → COMPLETED: Job completed despite previous cancellation issues
	//
	// Parameters:
	// - jobID: The unique identifier of the job to complete
	// - result: Serialized result data (protobuf or JSON) to store with the job
	//
	// Returns:
	// - freedAssigneeIDs: Map of assignee IDs to count of jobs freed (typically 0 or 1 entry)
	//   - Empty map if job was not assigned to an assignee
	//   - Contains assignee_id with count if job was assigned (assignee capacity is now available)
	//   - If multiple jobs freed for same assignee, count reflects the multiplicity
	// - error if job not found, job is in invalid state, or database operation fails
	//
	// When to use vs other methods:
	// - Use CompleteJob for all successful job completions
	// - Do not use UpdateJobStatus for completion - use CompleteJob instead
	CompleteJob(ctx context.Context, jobID string, result []byte) (map[string]int, error)

	// Job failure (atomic: RUNNING/UNKNOWN_RETRY → FAILED_RETRY with retry increment)
	// FailJob atomically transitions a job to FAILED_RETRY, incrementing the retry count.
	//
	// Purpose: Use this method when a job fails during execution and should be retried.
	// The job will be automatically rescheduled for execution after this call.
	//
	// Atomic operations:
	// - Updates job status to FAILED_RETRY (job becomes eligible for scheduling)
	// - Sets error_message field with provided error message
	// - Increments retry_count by 1
	// - Sets last_retry_at timestamp to current time
	// - Preserves assignee_id and assigned_at for historical tracking (not cleared)
	//
	// State transitions:
	// - RUNNING → FAILED_RETRY: Normal failure flow (job eligible for scheduling)
	// - UNKNOWN_RETRY → FAILED_RETRY: Job failed after worker reconnection (job eligible for scheduling)
	//
	// Important: Jobs never return to INITIAL_PENDING state once they leave it. FAILED_RETRY jobs are eligible
	// for scheduling but remain in FAILED_RETRY state.
	//
	// Parameters:
	// - jobID: The unique identifier of the job that failed
	// - errorMsg: Human-readable error message describing the failure
	//
	// Returns:
	// - freedAssigneeIDs: Map of assignee IDs to count of jobs freed (typically 0 or 1 entry)
	//   - Empty map if job was not assigned to an assignee
	//   - Contains assignee_id with count if job was assigned (assignee capacity is now available)
	//   - If multiple jobs freed for same assignee, count reflects the multiplicity
	// - error if job not found, job is in invalid state, or database operation fails
	//
	// When to use vs other methods:
	// - Use FailJob when job should be retried (normal failure case)
	// - Use StopJob or StopJobWithRetry when job should not be retried (cancellation case)
	// - Do not use UpdateJobStatus for failures - use FailJob instead
	FailJob(ctx context.Context, jobID string, errorMsg string) (map[string]int, error)

	// Job cancellation - stop without retry (atomic: RUNNING/CANCELLING/UNKNOWN_RETRY → STOPPED)
	// StopJob atomically transitions a job to STOPPED with an error message.
	//
	// Purpose: Use this method when a job is cancelled and should not be retried.
	// This is the primary method for stopping jobs that were acknowledged by workers.
	//
	// Atomic operations:
	// - Updates job status to STOPPED (terminal state)
	// - Sets error_message field with provided error message
	// - Sets finalized_at timestamp to current time (if not already set)
	// - Preserves assignee_id and assigned_at for historical tracking (not cleared)
	//
	// State transitions:
	// - RUNNING → STOPPED: Job cancelled before completion
	// - CANCELLING → STOPPED: Job cancellation acknowledged by worker (normal cancellation flow)
	// - UNKNOWN_RETRY → STOPPED: Job cancelled after worker reconnection
	//
	// Parameters:
	// - jobID: The unique identifier of the job to stop
	// - errorMsg: Human-readable message explaining why the job was stopped
	//
	// Returns:
	// - freedAssigneeIDs: Map of assignee IDs to count of jobs freed (typically 0 or 1 entry)
	//   - Empty map if job was not assigned to an assignee
	//   - Contains assignee_id with count if job was assigned (assignee capacity is now available)
	//   - If multiple jobs freed for same assignee, count reflects the multiplicity
	// - error if job not found, job is in invalid state, or database operation fails
	//
	// When to use vs other methods:
	// - Use StopJob for normal cancellation acknowledgment (no retry increment needed)
	// - Use StopJobWithRetry when cancellation happens after job failure (retry increment needed)
	// - Use MarkJobUnknownStopped when worker is unresponsive or job is unknown to worker
	StopJob(ctx context.Context, jobID string, errorMsg string) (map[string]int, error)

	// Job cancellation with retry increment (atomic: CANCELLING → STOPPED with retry increment)
	// StopJobWithRetry atomically transitions a job from CANCELLING to STOPPED, applying all effects
	// from the transitory FAILED_RETRY state (retry increment + error message).
	//
	// Purpose: Use this method when a job in CANCELLING state fails (success=false in result).
	// This ensures the retry count is incremented atomically, matching the behavior of FailJob.
	//
	// Atomic operations:
	// - Updates job status to STOPPED (terminal state)
	// - Sets error_message field with provided error message
	// - Increments retry_count by 1 (applies FAILED_RETRY state effect)
	// - Sets last_retry_at timestamp to current time
	// - Sets finalized_at timestamp to current time (if not already set)
	// - Preserves assignee_id and assigned_at for historical tracking (not cleared)
	//
	// State transitions:
	// - CANCELLING → STOPPED: Job failed while being cancelled (applies FAILED_RETRY state effects)
	//
	// Parameters:
	// - jobID: The unique identifier of the job to stop
	// - errorMsg: Human-readable error message describing the failure
	//
	// Returns:
	// - freedWorkerIDs: Map of worker IDs to count of jobs freed (typically 0 or 1 entry)
	//   - Empty map if job was not assigned to a worker
	//   - Contains assignee_id with count if job was assigned (worker capacity is now available)
	//   - If multiple jobs freed for same assignee, count reflects the multiplicity
	// - error if job not found, job is not in CANCELLING state, or database operation fails
	//
	// When to use vs other methods:
	// - Use StopJobWithRetry when CANCELLING job fails (success=false) - applies retry increment
	// - Use StopJob when CANCELLING job is acknowledged without failure - no retry increment
	// - Do not use UpdateJobStatus + IncrementRetryCount separately - use StopJobWithRetry instead
	StopJobWithRetry(ctx context.Context, jobID string, errorMsg string) (map[string]int, error)

	// Mark job as unknown stopped (atomic: CANCELLING/UNKNOWN_RETRY/RUNNING → UNKNOWN_STOPPED)
	// MarkJobUnknownStopped atomically transitions a job to UNKNOWN_STOPPED with an error message.
	//
	// Purpose: Use this method when a job cancellation fails due to worker unresponsiveness
	// or when a job is unknown to the worker. Jobs in UNKNOWN_STOPPED state should not be retried.
	//
	// Atomic operations:
	// - Updates job status to UNKNOWN_STOPPED (terminal state)
	// - Sets error_message field with provided error message
	// - Sets finalized_at timestamp to current time (if not already set)
	// - Preserves assignee_id and assigned_at for historical tracking (not cleared)
	//
	// State transitions:
	// - CANCELLING → UNKNOWN_STOPPED: Cancellation timeout or job unknown to worker
	// - UNKNOWN_RETRY → UNKNOWN_STOPPED: Job unknown to worker during cancellation
	// - RUNNING → UNKNOWN_STOPPED: Worker not connected (fallback case, indicates service bug)
	//
	// Parameters:
	// - jobID: The unique identifier of the job to mark as unknown stopped
	// - errorMsg: Human-readable message explaining why the job was marked as unknown stopped
	//
	// Returns:
	// - freedWorkerIDs: Map of worker IDs to count of jobs freed (typically 0 or 1 entry)
	//   - Empty map if job was not assigned to a worker
	//   - Contains assignee_id with count if job was assigned (worker capacity is now available)
	//   - If multiple jobs freed for same assignee, count reflects the multiplicity
	// - error if job not found or database operation fails
	//
	// When to use vs other methods:
	// - Use MarkJobUnknownStopped for cancellation timeouts and unknown job scenarios
	// - Use StopJob for normal cancellation acknowledgment
	// - Use MarkWorkerUnresponsive to mark all jobs for a worker (batch operation)
	MarkJobUnknownStopped(ctx context.Context, jobID string, errorMsg string) (map[string]int, error)

	// Generic job status update (use sparingly - prefer atomic methods)
	// UpdateJobStatus updates a job's status, result, and error message.
	//
	// Purpose: Use this method only for edge cases not covered by atomic methods (CompleteJob,
	// FailJob, StopJob, StopJobWithRetry, MarkJobUnknownStopped). This method provides maximum
	// flexibility but requires the caller to ensure atomicity of related operations.
	//
	// Atomic operations:
	// - Updates job status to the specified status
	// - Sets result field if provided (non-nil)
	// - Sets error_message field if provided (non-empty)
	// - Updates timestamps based on status (started_at, finalized_at)
	// - Preserves assignee_id and assigned_at for historical tracking (not cleared)
	//
	// State transitions:
	// - Supports any valid state transition
	// - Common edge cases: UNKNOWN_STOPPED → STOPPED, various → COMPLETED from non-standard states
	//
	// Parameters:
	// - jobID: The unique identifier of the job to update
	// - status: The new status for the job
	// - result: Optional serialized result data (nil if not applicable)
	// - errorMsg: Optional error message (empty string if not applicable)
	//
	// Returns:
	// - freedAssigneeIDs: Map of assignee IDs to count of jobs freed (typically 0 or 1 entry)
	//   - Empty map if job was not assigned to an assignee or not transitioning from RUNNING to terminal state
	//   - Contains assignee_id with count if job was assigned and transitioning to terminal state (COMPLETED, STOPPED, UNKNOWN_STOPPED)
	//   - If multiple jobs freed for same assignee, count reflects the multiplicity
	// - error if job not found, invalid status transition, or database operation fails
	//
	// When to use vs other methods:
	// - Prefer atomic methods (CompleteJob, FailJob, StopJob, etc.) for common transitions
	// - Use UpdateJobStatus only for edge cases not covered by atomic methods
	// - Ensure related operations (e.g., retry increment) are handled separately if needed
	UpdateJobStatus(ctx context.Context, jobID string, status JobStatus, result []byte, errorMsg string) (map[string]int, error)

	// Cancellation operations
	// CancelJobs cancels jobs by tags and/or job IDs (batch cancellation)
	// tags: Filter jobs by tags using AND logic (jobs must have ALL provided tags)
	// jobIDs: Specific job IDs to cancel
	// Both tags and jobIDs are processed (union of both sets)
	// Returns: (cancelledJobIDs []string, unknownJobIDs []string, error)
	// - cancelledJobIDs: Jobs that were successfully cancelled
	// - unknownJobIDs: Jobs that were not found or already completed
	// State transitions:
	// - INITIAL_PENDING → UNSCHEDULED
	// - RUNNING → CANCELLING
	// - FAILED_RETRY → STOPPED
	// - UNKNOWN_RETRY → STOPPED
	CancelJobs(ctx context.Context, tags []string, jobIDs []string) ([]string, []string, error)

	// AcknowledgeCancellation handles cancellation acknowledgment from worker
	// wasExecuting: true if job was executing when cancellation was processed, false if job was unknown/finished
	// State transitions:
	// - CANCELLING → STOPPED (wasExecuting=true)
	// - CANCELLING → UNKNOWN_STOPPED (wasExecuting=false)
	AcknowledgeCancellation(ctx context.Context, jobID string, wasExecuting bool) error

	// Worker disconnect handling (atomic: RUNNING → UNKNOWN_RETRY, CANCELLING → UNKNOWN_STOPPED)
	// MarkWorkerUnresponsive marks all jobs assigned to the given assigneeID as unresponsive
	// This is called when a worker becomes unresponsive
	// State transitions:
	// - RUNNING → UNKNOWN_RETRY
	// - CANCELLING → UNKNOWN_STOPPED
	MarkWorkerUnresponsive(ctx context.Context, assigneeID string) error

	// Query operations
	// GetJob retrieves a job by ID
	GetJob(ctx context.Context, jobID string) (*Job, error)

	// GetJobStats gets statistics for jobs matching ALL provided tags (AND logic)
	GetJobStats(ctx context.Context, tags []string) (*JobStats, error)

	// Maintenance
	// ResetRunningJobs marks all running jobs as unknown (for service restart)
	// This is called when the service restarts and there are jobs that were in progress
	ResetRunningJobs(ctx context.Context) error

	// CleanupExpiredJobs deletes completed jobs older than TTL
	CleanupExpiredJobs(ctx context.Context, ttl time.Duration) error

	// DeleteJobs forcefully deletes jobs by tags and/or job IDs
	// This method validates that all jobs are in final states (COMPLETED, UNSCHEDULED, STOPPED, UNKNOWN_STOPPED)
	// before deletion. If any job is not in a final state, an error is returned.
	// tags: Filter jobs by tags using AND logic (jobs must have ALL provided tags)
	// jobIDs: Specific job IDs to delete
	// Both tags and jobIDs are processed (union of both sets)
	// Returns error if any job is not in a final state or if deletion fails
	DeleteJobs(ctx context.Context, tags []string, jobIDs []string) error

	// Close closes the backend connection
	Close() error
}
