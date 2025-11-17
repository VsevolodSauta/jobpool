// Package jobpool provides a reusable job queue library with support for multiple
// storage backends (SQLite, BadgerDB) and distributed worker management.
//
// The library supports:
//   - Multiple backend implementations (SQLite, BadgerDB)
//   - Job assignee tracking for distributed workers
//   - Automatic job return-to-pending on worker disconnect
//   - Batch job operations
//   - Tag-based job filtering and statistics
//   - Retry mechanism with configurable retry counts
//   - Automatic cleanup of expired completed jobs
//
// Example usage:
//
//	backend, _ := jobpool.NewSQLiteBackend("./jobs.db")
//	queue := jobpool.NewPoolQueue(backend)
//	defer queue.Close()
//
//	job := &jobpool.Job{
//	    ID:            "job-1",
//	    Status:        jobpool.JobStatusInitialPending,
//	    JobType:       "my_task",
//	    JobDefinition: []byte(`{"data": "example"}`),
//	    Tags:          []string{"tag1"},
//	    CreatedAt:     time.Now(),
//	}
//	queue.EnqueueJob(ctx, job)
package jobpool

import (
	"time"
)

// JobStatus represents the status of a job in the queue.
type JobStatus string

const (
	// JobStatusInitialPending indicates the job is waiting to be processed.
	JobStatusInitialPending JobStatus = "initial_pending"
	// JobStatusRunning indicates the job is currently being processed.
	JobStatusRunning JobStatus = "running"
	// JobStatusCompleted indicates the job completed successfully.
	JobStatusCompleted JobStatus = "completed"
	// JobStatusFailedRetry indicates the job failed during processing.
	JobStatusFailedRetry JobStatus = "failed_retry"
	// JobStatusStopped indicates the job was running and got cancelled.
	JobStatusStopped JobStatus = "stopped"
	// JobStatusUnscheduled indicates the job was pending and got cancelled.
	JobStatusUnscheduled JobStatus = "unscheduled"
	// JobStatusUnknownRetry indicates the job was assigned to an unresponsive worker (should be retried).
	JobStatusUnknownRetry JobStatus = "unknown_retry"
	// JobStatusCancelling indicates the job cancellation has been requested (transitional state).
	// From this state, the job can transition to:
	// - COMPLETED: JobResultMessage with success=true
	// - FAILED_RETRY → STOPPED: JobResultMessage with success=false, then JobCancelledMessage with success=false
	// - STOPPED: JobCancelledMessage with success=true
	// - UNKNOWN_STOPPED: JobCancelledMessage with success=false or timeout
	JobStatusCancelling JobStatus = "cancelling"
	// JobStatusUnknownStopped indicates the job cancellation failed or job unknown to worker (should not be retried).
	JobStatusUnknownStopped JobStatus = "unknown_stopped"
)

// Job represents a generic job in the queue.
type Job struct {
	ID            string     // Unique job identifier
	Status        JobStatus  // Current job status
	JobType       string     // Job type identifier
	JobDefinition []byte     // Serialized job definition (protobuf or JSON)
	Tags          []string   // Tags for filtering and statistics
	CreatedAt     time.Time  // When the job was created
	StartedAt     *time.Time // When the job started processing (nil if not started)
	FinalizedAt   *time.Time // When the job completed (nil if not completed)
	ErrorMessage  string     // Error message if job failed
	Result        []byte     // Serialized result (if completed)
	RetryCount    int        // Number of times job has been retried
	LastRetryAt   *time.Time // When the job was last retried (nil if never retried)
	AssigneeID    string     // ID of the worker assigned to this job
	AssignedAt    *time.Time // When the job was assigned to a worker
}

// JobStats represents statistics for jobs matching certain tags.
// Tags use AND logic - jobs must have ALL specified tags to be included.
type JobStats struct {
	Tags          []string // Tags used for query
	TotalJobs     int32    // Total number of jobs
	PendingJobs   int32    // Number of pending jobs
	RunningJobs   int32    // Number of running jobs
	CompletedJobs int32    // Number of completed jobs
	StoppedJobs   int32    // Number of jobs in final states, except COMPLETED
	FailedJobs    int32    // Number of failed jobs, including UNKNOWN_RETRY
	TotalRetries  int32    // Total number of retries across all jobs
}
