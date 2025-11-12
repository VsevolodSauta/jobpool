package jobpool

import (
	"context"
	"time"
)

// Backend represents the interface for job pool storage backends.
// Implementations must be thread-safe and support concurrent operations.
type Backend interface {
	// EnqueueJob enqueues a single job
	EnqueueJob(ctx context.Context, job *Job) (string, error)

	// EnqueueJobs enqueues multiple jobs in a batch
	EnqueueJobs(ctx context.Context, jobs []*Job) ([]string, error)

	// DequeueJobs dequeues pending jobs up to the limit and assigns them to the given assigneeID
	// tags: Filter jobs by tags using AND logic (jobs must have ALL provided tags). Empty slice means no filtering.
	DequeueJobs(ctx context.Context, assigneeID string, tags []string, limit int) ([]*Job, error)

	// UpdateJobStatus updates a job's status
	UpdateJobStatus(ctx context.Context, jobID string, status JobStatus, result []byte, errorMsg string) error

	// GetJobStats gets statistics for jobs matching ALL provided tags (AND logic)
	GetJobStats(ctx context.Context, tags []string) (*JobStats, error)

	// ResetRunningJobs marks all running jobs as unknown (for service restart)
	// This is called when the service restarts and there are jobs that were in progress
	ResetRunningJobs(ctx context.Context) error

	// ReturnJobsToPending returns all jobs assigned to the given assigneeID back to pending
	ReturnJobsToPending(ctx context.Context, assigneeID string) error

	// IncrementRetryCount increments the retry count for a job and sets it back to pending
	IncrementRetryCount(ctx context.Context, jobID string) error

	// CleanupExpiredJobs deletes completed jobs older than TTL
	CleanupExpiredJobs(ctx context.Context, ttl time.Duration) error

	// GetJob retrieves a job by ID
	GetJob(ctx context.Context, jobID string) (*Job, error)

	// CancelJob cancels a job by ID
	// For pending jobs: marks as "unscheduled"
	// For running jobs: marks as "stopped"
	// Returns error if job doesn't exist or is already in terminal state
	CancelJob(ctx context.Context, jobID string) error

	// CancelJobs cancels jobs by tags and/or job IDs (batch cancellation)
	// tags: Filter jobs by tags using AND logic (jobs must have ALL provided tags)
	// jobIDs: Specific job IDs to cancel
	// Both tags and jobIDs are processed (union of both sets)
	// Returns: (cancelledJobIDs []string, unknownJobIDs []string, error)
	// - cancelledJobIDs: Jobs that were successfully cancelled
	// - unknownJobIDs: Jobs that were not found or already completed
	CancelJobs(ctx context.Context, tags []string, jobIDs []string) ([]string, []string, error)

	// MarkJobsAsUnknown marks all running jobs assigned to the given assigneeID as unknown
	// This is called when a worker becomes unresponsive
	MarkJobsAsUnknown(ctx context.Context, assigneeID string) error

	// Close closes the backend connection
	Close() error
}
