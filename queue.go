package jobpool

import (
	"context"
	"time"
)

// Queue represents the job queue interface.
// All queue operations are thread-safe and can be called concurrently.
type Queue interface {
	EnqueueJob(ctx context.Context, job *Job) (string, error)
	EnqueueJobs(ctx context.Context, jobs []*Job) ([]string, error)
	DequeueJobs(ctx context.Context, assigneeID string, tags []string, limit int) ([]*Job, error)
	UpdateJobStatus(ctx context.Context, jobID string, status JobStatus, result []byte, errorMsg string) error
	GetJobStats(ctx context.Context, tags []string) (*JobStats, error)
	ResetRunningJobs(ctx context.Context) error
	ReturnJobsToPending(ctx context.Context, assigneeID string) error
	IncrementRetryCount(ctx context.Context, jobID string) error
	CleanupExpiredJobs(ctx context.Context, ttl time.Duration) error
	GetJob(ctx context.Context, jobID string) (*Job, error)
	CancelJob(ctx context.Context, jobID string) error
	CancelJobs(ctx context.Context, tags []string, jobIDs []string) ([]string, []string, error)
	MarkJobsAsUnknown(ctx context.Context, assigneeID string) error
	Close() error
}

// PoolQueue implements the Queue interface using a Backend.
type PoolQueue struct {
	backend Backend
}

// NewPoolQueue creates a new queue with the given backend.
// The backend must not be nil.
func NewPoolQueue(backend Backend) *PoolQueue {
	return &PoolQueue{
		backend: backend,
	}
}

// EnqueueJob enqueues a single job into the queue.
// Returns the job ID on success, or an error if the operation fails.
func (q *PoolQueue) EnqueueJob(ctx context.Context, job *Job) (string, error) {
	return q.backend.EnqueueJob(ctx, job)
}

// EnqueueJobs enqueues multiple jobs in a batch operation.
// This is more efficient than calling EnqueueJob multiple times.
// Returns a slice of job IDs in the same order as the input jobs, or an error if the operation fails.
func (q *PoolQueue) EnqueueJobs(ctx context.Context, jobs []*Job) ([]string, error) {
	return q.backend.EnqueueJobs(ctx, jobs)
}

// DequeueJobs dequeues pending jobs up to the specified limit and assigns them to the given assigneeID.
// Jobs are selected in order of priority (oldest first, considering retry timestamps).
// The jobs are automatically marked as "running" and assigned to the specified assigneeID.
// tags: Filter jobs by tags using AND logic (jobs must have ALL provided tags). Empty slice means no filtering.
// Returns a slice of jobs (may be empty if no pending jobs are available), or an error if the operation fails.
func (q *PoolQueue) DequeueJobs(ctx context.Context, assigneeID string, tags []string, limit int) ([]*Job, error) {
	return q.backend.DequeueJobs(ctx, assigneeID, tags, limit)
}

// UpdateJobStatus updates a job's status, result, and error message.
// This is typically called after a job has been processed.
//   - status: The new status (e.g., JobStatusCompleted, JobStatusFailed)
//   - result: Serialized result data (can be nil)
//   - errorMsg: Error message if the job failed (empty string if successful)
func (q *PoolQueue) UpdateJobStatus(ctx context.Context, jobID string, status JobStatus, result []byte, errorMsg string) error {
	return q.backend.UpdateJobStatus(ctx, jobID, status, result, errorMsg)
}

// GetJobStats gets statistics for jobs matching ALL provided tags (AND logic)
func (q *PoolQueue) GetJobStats(ctx context.Context, tags []string) (*JobStats, error) {
	return q.backend.GetJobStats(ctx, tags)
}

// ResetRunningJobs marks all running jobs as unknown state.
// This is called when the service restarts and there are jobs that were in progress.
// Jobs marked as unknown indicate they were assigned to workers that are no longer available.
func (q *PoolQueue) ResetRunningJobs(ctx context.Context) error {
	return q.backend.ResetRunningJobs(ctx)
}

// ReturnJobsToPending returns all running jobs assigned to the given assigneeID back to pending state.
// This is typically called when a worker disconnects or crashes, allowing
// the jobs to be reassigned to other workers.
func (q *PoolQueue) ReturnJobsToPending(ctx context.Context, assigneeID string) error {
	return q.backend.ReturnJobsToPending(ctx, assigneeID)
}

// IncrementRetryCount increments the retry count for a job and sets it back to pending state.
// This is called when a job fails and needs to be retried.
// The job will be available for dequeuing again after this operation.
func (q *PoolQueue) IncrementRetryCount(ctx context.Context, jobID string) error {
	return q.backend.IncrementRetryCount(ctx, jobID)
}

// CleanupExpiredJobs deletes completed jobs that are older than the specified TTL.
// This helps prevent the database from growing indefinitely.
// Only completed jobs are deleted; pending and running jobs are never deleted.
func (q *PoolQueue) CleanupExpiredJobs(ctx context.Context, ttl time.Duration) error {
	return q.backend.CleanupExpiredJobs(ctx, ttl)
}

// GetJob retrieves a job by ID.
// Returns the job if found, or an error if the job doesn't exist.
func (q *PoolQueue) GetJob(ctx context.Context, jobID string) (*Job, error) {
	return q.backend.GetJob(ctx, jobID)
}

// CancelJob cancels a job by ID.
// For pending jobs: marks as "unscheduled"
// For running jobs: marks as "stopped"
// Returns error if job doesn't exist or is already in terminal state.
func (q *PoolQueue) CancelJob(ctx context.Context, jobID string) error {
	return q.backend.CancelJob(ctx, jobID)
}

// CancelJobs cancels jobs by tags and/or job IDs (batch cancellation).
// tags: Filter jobs by tags using AND logic (jobs must have ALL provided tags)
// jobIDs: Specific job IDs to cancel
// Both tags and jobIDs are processed (union of both sets)
// Returns: (cancelledJobIDs []string, unknownJobIDs []string, error)
// - cancelledJobIDs: Jobs that were successfully cancelled
// - unknownJobIDs: Jobs that were not found or already completed
func (q *PoolQueue) CancelJobs(ctx context.Context, tags []string, jobIDs []string) ([]string, []string, error) {
	return q.backend.CancelJobs(ctx, tags, jobIDs)
}

// MarkJobsAsUnknown marks all running jobs assigned to the given assigneeID as unknown.
// This is called when a worker becomes unresponsive.
func (q *PoolQueue) MarkJobsAsUnknown(ctx context.Context, assigneeID string) error {
	return q.backend.MarkJobsAsUnknown(ctx, assigneeID)
}

// Close closes the backend connection and releases any resources.
// This should be called when the queue is no longer needed.
// After closing, the queue should not be used.
func (q *PoolQueue) Close() error {
	return q.backend.Close()
}
