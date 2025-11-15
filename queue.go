package jobpool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"
)

// Queue represents the job queue interface.
// All queue operations are thread-safe and can be called concurrently.
type Queue interface {
	// Job submission
	EnqueueJob(ctx context.Context, job *Job) (string, error)
	EnqueueJobs(ctx context.Context, jobs []*Job) ([]string, error)

	// Job streaming (push-based)
	// StreamJobs streams eligible jobs (PENDING, FAILED, UNKNOWN_RETRY) matching the tag filter to the provided channel.
	// This method blocks until jobs become available or the context is cancelled.
	// Jobs are automatically assigned to the specified assigneeID (marked as RUNNING) via DequeueJobs.
	// tags: Filter jobs by tags using AND logic (jobs must have ALL provided tags). Empty slice means no filtering.
	// limit: Maximum number of jobs per batch sent to the channel.
	// The channel is closed when the context is cancelled, the queue is closed, or an error occurs.
	// This method should be called in a goroutine as it blocks.
	StreamJobs(ctx context.Context, assigneeID string, tags []string, limit int, ch chan<- []*Job) error

	// Job lifecycle (delegates to Backend)
	// CompleteJob atomically transitions a job to COMPLETED with the given result.
	//
	// When to use Queue vs Backend:
	// - Use Queue.CompleteJob when you want the standard behavior (delegates to Backend)
	// - Queue does not notify workers for COMPLETED jobs (terminal state, no scheduling needed)
	// - Use Backend.CompleteJob directly only if you need to bypass Queue's notification system
	//
	// Delegation behavior:
	// - Delegates to Backend.CompleteJob
	// - No worker notification (COMPLETED is a terminal state)
	//
	// State transitions: RUNNING/CANCELLING/UNKNOWN_RETRY/UNKNOWN_STOPPED → COMPLETED
	CompleteJob(ctx context.Context, jobID string, result []byte) error

	// FailJob atomically transitions a job to FAILED, then to PENDING, incrementing the retry count.
	//
	// When to use Queue vs Backend:
	// - Use Queue.FailJob when you want worker notification after job becomes eligible (PENDING state)
	// - Queue notifies waiting workers after the job transitions to PENDING
	// - Use Backend.FailJob directly only if you need to bypass Queue's notification system
	//
	// Delegation behavior:
	// - Delegates to Backend.FailJob
	// - Notifies waiting workers after job transitions to PENDING (job becomes eligible for scheduling)
	//
	// Worker notification:
	// - Workers are notified when job becomes PENDING (eligible for scheduling)
	// - Notification uses job tags to match workers with appropriate tag filters
	//
	// State transitions: RUNNING/UNKNOWN_RETRY → FAILED → PENDING
	FailJob(ctx context.Context, jobID string, errorMsg string) error

	// StopJob atomically transitions a job to STOPPED with an error message.
	//
	// When to use Queue vs Backend:
	// - Use Queue.StopJob when you want the standard behavior (delegates to Backend)
	// - Queue does not notify workers for STOPPED jobs (terminal state, no scheduling needed)
	// - Use Backend.StopJob directly only if you need to bypass Queue's notification system
	//
	// Delegation behavior:
	// - Delegates to Backend.StopJob
	// - No worker notification (STOPPED is a terminal state)
	//
	// State transitions: RUNNING/CANCELLING/UNKNOWN_RETRY → STOPPED
	StopJob(ctx context.Context, jobID string, errorMsg string) error

	// StopJobWithRetry atomically transitions a job from CANCELLING to STOPPED with retry increment.
	//
	// When to use Queue vs Backend:
	// - Use Queue.StopJobWithRetry when you want the standard behavior (delegates to Backend)
	// - Queue does not notify workers for STOPPED jobs (terminal state, no scheduling needed)
	// - Use Backend.StopJobWithRetry directly only if you need to bypass Queue's notification system
	//
	// Delegation behavior:
	// - Delegates to Backend.StopJobWithRetry
	// - No worker notification (STOPPED is a terminal state, job was already in retry flow)
	//
	// State transitions: CANCELLING → STOPPED (with retry increment)
	StopJobWithRetry(ctx context.Context, jobID string, errorMsg string) error

	// MarkJobUnknownStopped atomically transitions a job to UNKNOWN_STOPPED with an error message.
	//
	// When to use Queue vs Backend:
	// - Use Queue.MarkJobUnknownStopped when you want the standard behavior (delegates to Backend)
	// - Queue does not notify workers for UNKNOWN_STOPPED jobs (terminal state, no scheduling needed)
	// - Use Backend.MarkJobUnknownStopped directly only if you need to bypass Queue's notification system
	//
	// Delegation behavior:
	// - Delegates to Backend.MarkJobUnknownStopped
	// - No worker notification (UNKNOWN_STOPPED is a terminal state)
	//
	// State transitions: CANCELLING/UNKNOWN_RETRY/RUNNING → UNKNOWN_STOPPED
	MarkJobUnknownStopped(ctx context.Context, jobID string, errorMsg string) error

	// UpdateJobStatus updates a job's status, result, and error message (use sparingly).
	//
	// When to use Queue vs Backend:
	// - Use Queue.UpdateJobStatus when you want conditional worker notification
	// - Queue notifies waiting workers if job becomes eligible (PENDING, FAILED, UNKNOWN_RETRY)
	// - Use Backend.UpdateJobStatus directly only if you need to bypass Queue's notification system
	//
	// Delegation behavior:
	// - Delegates to Backend.UpdateJobStatus
	// - Conditionally notifies workers if job becomes eligible for scheduling
	//
	// Worker notification:
	// - Workers are notified only if job transitions to PENDING, FAILED, or UNKNOWN_RETRY
	// - Notification uses job tags to match workers with appropriate tag filters
	// - No notification for terminal states (COMPLETED, STOPPED, UNSCHEDULED, UNKNOWN_STOPPED)
	//
	// Client usage patterns:
	// - Prefer atomic methods (CompleteJob, FailJob, StopJob, etc.) for common transitions
	// - Use UpdateJobStatus only for edge cases not covered by atomic methods
	// - Ensure related operations (e.g., retry increment) are handled separately if needed
	UpdateJobStatus(ctx context.Context, jobID string, status JobStatus, result []byte, errorMsg string) error

	// Cancellation (delegates to Backend)
	CancelJobs(ctx context.Context, tags []string, jobIDs []string) ([]string, []string, error)
	AcknowledgeCancellation(ctx context.Context, jobID string, wasExecuting bool) error

	// Worker management (delegates to Backend)
	MarkWorkerUnresponsive(ctx context.Context, assigneeID string) error

	// Query operations (delegates to Backend)
	GetJob(ctx context.Context, jobID string) (*Job, error)
	GetJobStats(ctx context.Context, tags []string) (*JobStats, error)

	// Maintenance (delegates to Backend)
	CleanupExpiredJobs(ctx context.Context, ttl time.Duration) error
	ResetRunningJobs(ctx context.Context) error

	// DeleteJobs forcefully deletes jobs by tags and/or job IDs
	// This method validates that all jobs are in final states (COMPLETED, UNSCHEDULED, STOPPED, UNKNOWN_STOPPED)
	// before deletion. If any job is not in a final state, an error is returned.
	// tags: Filter jobs by tags using AND logic (jobs must have ALL provided tags)
	// jobIDs: Specific job IDs to delete
	// Both tags and jobIDs are processed (union of both sets)
	// Returns error if any job is not in a final state or if deletion fails
	DeleteJobs(ctx context.Context, tags []string, jobIDs []string) error

	Close() error
}

// streamWaiter represents a waiting StreamJobs call
type streamWaiter struct {
	assigneeID string
	tags       []string
	notifyCh   chan struct{}
}

// PoolQueue implements the Queue interface using a Backend.
type PoolQueue struct {
	backend       Backend
	streamWaiters map[string]*streamWaiter // key: worker identifier (assigneeID + tags hash)
	streamMu      sync.RWMutex             // protects streamWaiters
}

// NewPoolQueue creates a new queue with the given backend.
// The backend must not be nil.
func NewPoolQueue(backend Backend) *PoolQueue {
	return &PoolQueue{
		backend:       backend,
		streamWaiters: make(map[string]*streamWaiter),
	}
}

// EnqueueJob enqueues a single job into the queue.
// Returns the job ID on success, or an error if the operation fails.
func (q *PoolQueue) EnqueueJob(ctx context.Context, job *Job) (string, error) {
	jobID, err := q.backend.EnqueueJob(ctx, job)
	if err == nil {
		// Notify waiting workers that a new eligible job is available
		q.notifyWaitingWorkers(job.Tags)
	}
	return jobID, err
}

// EnqueueJobs enqueues multiple jobs in a batch operation.
// This is more efficient than calling EnqueueJob multiple times.
// Returns a slice of job IDs in the same order as the input jobs, or an error if the operation fails.
func (q *PoolQueue) EnqueueJobs(ctx context.Context, jobs []*Job) ([]string, error) {
	jobIDs, err := q.backend.EnqueueJobs(ctx, jobs)
	if err == nil {
		// Notify waiting workers for each job's tags
		// Use a set to avoid duplicate notifications for same tag combinations
		notifiedTags := make(map[string]bool)
		for _, job := range jobs {
			tagKey := q.tagsKey(job.Tags)
			if !notifiedTags[tagKey] {
				q.notifyWaitingWorkers(job.Tags)
				notifiedTags[tagKey] = true
			}
		}
	}
	return jobIDs, err
}

// dequeueJobs is an internal method that dequeues pending jobs up to the specified limit and assigns them to the given assigneeID.
// Jobs are selected in order of priority (oldest first, considering retry timestamps).
// The jobs are automatically marked as "running" and assigned to the specified assigneeID.
// tags: Filter jobs by tags using AND logic (jobs must have ALL provided tags). Empty slice means no filtering.
// Returns a slice of jobs (may be empty if no pending jobs are available), or an error if the operation fails.
func (q *PoolQueue) dequeueJobs(ctx context.Context, assigneeID string, tags []string, limit int) ([]*Job, error) {
	return q.backend.DequeueJobs(ctx, assigneeID, tags, limit)
}

// StreamJobs streams eligible jobs (PENDING, FAILED, UNKNOWN_RETRY) matching the tag filter to the provided channel.
// This method blocks until jobs become available or the context is cancelled.
// Jobs are automatically assigned to the specified assigneeID (marked as RUNNING) via DequeueJobs.
// tags: Filter jobs by tags using AND logic (jobs must have ALL provided tags). Empty slice means no filtering.
// limit: Maximum number of jobs per batch sent to the channel.
// The channel is closed when the context is cancelled, the queue is closed, or an error occurs.
// This method should be called in a goroutine as it blocks.
func (q *PoolQueue) StreamJobs(ctx context.Context, assigneeID string, tags []string, limit int, ch chan<- []*Job) error {
	// Generate worker identifier
	workerID := q.workerIdentifier(assigneeID, tags)

	// Create notification channel
	notifyCh := make(chan struct{}, 1)

	// Register waiter
	q.streamMu.Lock()
	q.streamWaiters[workerID] = &streamWaiter{
		assigneeID: assigneeID,
		tags:       tags,
		notifyCh:   notifyCh,
	}
	q.streamMu.Unlock()

	// Cleanup on exit
	defer func() {
		q.streamMu.Lock()
		delete(q.streamWaiters, workerID)
		close(notifyCh)
		q.streamMu.Unlock()
		close(ch)
	}()

	// Try initial dequeue in case jobs are already available
	jobs, err := q.dequeueJobs(ctx, assigneeID, tags, limit)
	if err != nil {
		return err
	}
	if len(jobs) > 0 {
		select {
		case ch <- jobs:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Main loop: wait for notifications and dequeue jobs
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notifyCh:
			// Jobs may be available, try to dequeue
			jobs, err := q.dequeueJobs(ctx, assigneeID, tags, limit)
			if err != nil {
				return err
			}
			if len(jobs) > 0 {
				select {
				case ch <- jobs:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}
}

// workerIdentifier generates a unique identifier for a worker based on assigneeID and tags
func (q *PoolQueue) workerIdentifier(assigneeID string, tags []string) string {
	// Create a hash of tags for uniqueness
	h := sha256.New()
	h.Write([]byte(assigneeID))
	for _, tag := range tags {
		h.Write([]byte(tag))
	}
	hash := hex.EncodeToString(h.Sum(nil))
	return assigneeID + ":" + hash[:16] // Use first 16 chars of hash
}

// tagsKey generates a key for a set of tags (for deduplication)
func (q *PoolQueue) tagsKey(tags []string) string {
	h := sha256.New()
	for _, tag := range tags {
		h.Write([]byte(tag))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// tagsMatch checks if job tags match worker tags using AND logic
// Worker tags must ALL be present in job tags
func tagsMatch(jobTags []string, workerTags []string) bool {
	// Empty worker tags means "accept all jobs"
	if len(workerTags) == 0 {
		return true
	}

	// Create a map of job tags for quick lookup
	jobTagMap := make(map[string]bool, len(jobTags))
	for _, tag := range jobTags {
		jobTagMap[tag] = true
	}

	// All worker tags must be in job tags
	for _, workerTag := range workerTags {
		if !jobTagMap[workerTag] {
			return false
		}
	}

	return true
}

// notifyWaitingWorkers notifies waiting StreamJobs workers when eligible jobs appear
func (q *PoolQueue) notifyWaitingWorkers(jobTags []string) {
	q.streamMu.RLock()
	waiters := make([]*streamWaiter, 0, len(q.streamWaiters))
	for _, waiter := range q.streamWaiters {
		waiters = append(waiters, waiter)
	}
	q.streamMu.RUnlock()

	// Notify matching workers
	for _, waiter := range waiters {
		if tagsMatch(jobTags, waiter.tags) {
			// Non-blocking send
			select {
			case waiter.notifyCh <- struct{}{}:
			default:
				// Channel already has a notification, skip
			}
		}
	}
}

// notifyWorkerByAssigneeID notifies StreamJobs workers with the given assigneeID
// This is used when a job completes to wake up the worker's StreamJobs so it can check for more jobs
func (q *PoolQueue) notifyWorkerByAssigneeID(assigneeID string) {
	q.streamMu.RLock()
	waiters := make([]*streamWaiter, 0, len(q.streamWaiters))
	for _, waiter := range q.streamWaiters {
		waiters = append(waiters, waiter)
	}
	q.streamMu.RUnlock()

	// Notify all workers with matching assigneeID (regardless of tags)
	// This ensures that when a job completes, the worker's StreamJobs wakes up
	// to check if there are more pending jobs to assign
	for _, waiter := range waiters {
		if waiter.assigneeID == assigneeID {
			// Non-blocking send
			select {
			case waiter.notifyCh <- struct{}{}:
			default:
				// Channel already has a notification, skip
			}
		}
	}
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

// CompleteJob atomically transitions a job to COMPLETED with the given result.
// Notifies the worker that completed the job so StreamJobs can check for more jobs.
// Also notifies workers waiting for jobs with matching tags (in case there are pending jobs).
func (q *PoolQueue) CompleteJob(ctx context.Context, jobID string, result []byte) error {
	// Get job before completing to retrieve assigneeID and tags for notification
	job, err := q.backend.GetJob(ctx, jobID)
	if err != nil {
		// If we can't get job, still try to complete but skip notification
		return q.backend.CompleteJob(ctx, jobID, result)
	}

	// Complete the job
	err = q.backend.CompleteJob(ctx, jobID, result)
	if err != nil {
		return err
	}

	// Notify the worker that completed this job (by assigneeID) so StreamJobs can check for more jobs
	// This is critical: when a job completes, the worker's capacity is freed, so StreamJobs should
	// wake up and check if there are more pending jobs to assign to this worker.
	if job.AssigneeID != "" {
		q.notifyWorkerByAssigneeID(job.AssigneeID)
	}

	// Also notify workers waiting for jobs with matching tags (in case there are pending jobs)
	// This ensures that if there are other workers waiting for jobs with these tags, they get notified too
	if len(job.Tags) > 0 {
		q.notifyWaitingWorkers(job.Tags)
	}

	return nil
}

// FailJob atomically transitions a job to FAILED, then to PENDING, incrementing the retry count.
// Notifies waiting workers after job becomes eligible (PENDING state).
func (q *PoolQueue) FailJob(ctx context.Context, jobID string, errorMsg string) error {
	// Get job to retrieve tags for notification
	job, err := q.backend.GetJob(ctx, jobID)
	if err != nil {
		return q.backend.FailJob(ctx, jobID, errorMsg)
	}

	err = q.backend.FailJob(ctx, jobID, errorMsg)
	if err == nil {
		// Job is now in PENDING state (after FAILED -> IncrementRetryCount -> PENDING)
		// Notify waiting workers
		q.notifyWaitingWorkers(job.Tags)
	}
	return err
}

// StopJob atomically transitions a job to STOPPED with an error message.
// Notifies the worker that stopped the job so StreamJobs can check for more jobs.
func (q *PoolQueue) StopJob(ctx context.Context, jobID string, errorMsg string) error {
	// Get job before stopping to retrieve assigneeID for notification
	job, err := q.backend.GetJob(ctx, jobID)
	if err != nil {
		// If we can't get job, still try to stop but skip notification
		return q.backend.StopJob(ctx, jobID, errorMsg)
	}

	err = q.backend.StopJob(ctx, jobID, errorMsg)
	if err != nil {
		return err
	}

	// Notify the worker that stopped this job (by assigneeID) so StreamJobs can check for more jobs
	// This is critical: when a job stops, the worker's capacity is freed, so StreamJobs should
	// wake up and check if there are more pending jobs to assign to this worker.
	if job.AssigneeID != "" {
		q.notifyWorkerByAssigneeID(job.AssigneeID)
	}

	return nil
}

// StopJobWithRetry atomically transitions a job from CANCELLING to STOPPED with retry increment.
// Notifies the worker that stopped the job so StreamJobs can check for more jobs.
func (q *PoolQueue) StopJobWithRetry(ctx context.Context, jobID string, errorMsg string) error {
	// Get job before stopping to retrieve assigneeID for notification
	job, err := q.backend.GetJob(ctx, jobID)
	if err != nil {
		// If we can't get job, still try to stop but skip notification
		return q.backend.StopJobWithRetry(ctx, jobID, errorMsg)
	}

	err = q.backend.StopJobWithRetry(ctx, jobID, errorMsg)
	if err != nil {
		return err
	}

	// Notify the worker that stopped this job (by assigneeID) so StreamJobs can check for more jobs
	// This is critical: when a job stops, the worker's capacity is freed, so StreamJobs should
	// wake up and check if there are more pending jobs to assign to this worker.
	if job.AssigneeID != "" {
		q.notifyWorkerByAssigneeID(job.AssigneeID)
	}

	return nil
}

// MarkJobUnknownStopped atomically transitions a job to UNKNOWN_STOPPED with an error message.
// Notifies the worker that had this job assigned so StreamJobs can check for more jobs.
func (q *PoolQueue) MarkJobUnknownStopped(ctx context.Context, jobID string, errorMsg string) error {
	// Get job before marking as unknown stopped to retrieve assigneeID for notification
	job, err := q.backend.GetJob(ctx, jobID)
	if err != nil {
		// If we can't get job, still try to mark but skip notification
		return q.backend.MarkJobUnknownStopped(ctx, jobID, errorMsg)
	}

	err = q.backend.MarkJobUnknownStopped(ctx, jobID, errorMsg)
	if err != nil {
		return err
	}

	// Notify the worker that had this job assigned (by assigneeID) so StreamJobs can check for more jobs
	// This is critical: when a job is marked as unknown stopped, the worker's capacity is freed,
	// so StreamJobs should wake up and check if there are more pending jobs to assign to this worker.
	if job.AssigneeID != "" {
		q.notifyWorkerByAssigneeID(job.AssigneeID)
	}

	return nil
}

// UpdateJobStatus updates a job's status, result, and error message.
// Conditionally notifies workers if job becomes eligible for scheduling or transitions to terminal state.
func (q *PoolQueue) UpdateJobStatus(ctx context.Context, jobID string, status JobStatus, result []byte, errorMsg string) error {
	// Get job before updating to retrieve assigneeID and tags for notification
	job, err := q.backend.GetJob(ctx, jobID)
	if err != nil {
		// If we can't get job, still try to update but skip notification
		return q.backend.UpdateJobStatus(ctx, jobID, status, result, errorMsg)
	}

	// Update the job
	err = q.backend.UpdateJobStatus(ctx, jobID, status, result, errorMsg)
	if err != nil {
		return err
	}

	// Check if we need to notify workers
	// Case 1: Job becomes eligible for scheduling (PENDING, FAILED, UNKNOWN_RETRY)
	if status == JobStatusPending || status == JobStatusFailed || status == JobStatusUnknownRetry {
		// Notify workers waiting for jobs with matching tags
		if len(job.Tags) > 0 {
			q.notifyWaitingWorkers(job.Tags)
		}
	}

	// Case 2: Job transitions to terminal state (COMPLETED, STOPPED, UNKNOWN_STOPPED)
	// Notify the worker that had this job assigned so StreamJobs can check for more jobs
	if status == JobStatusCompleted || status == JobStatusStopped || status == JobStatusUnknownStopped {
		if job.AssigneeID != "" {
			q.notifyWorkerByAssigneeID(job.AssigneeID)
		}
	}

	return nil
}

// CleanupExpiredJobs deletes completed jobs that are older than the specified TTL.
// This helps prevent the database from growing indefinitely.
// Only completed jobs are deleted; pending and running jobs are never deleted.
func (q *PoolQueue) CleanupExpiredJobs(ctx context.Context, ttl time.Duration) error {
	return q.backend.CleanupExpiredJobs(ctx, ttl)
}

// DeleteJobs forcefully deletes jobs by tags and/or job IDs
// This method validates that all jobs are in final states (COMPLETED, UNSCHEDULED, STOPPED, UNKNOWN_STOPPED)
// before deletion. If any job is not in a final state, an error is returned.
func (q *PoolQueue) DeleteJobs(ctx context.Context, tags []string, jobIDs []string) error {
	return q.backend.DeleteJobs(ctx, tags, jobIDs)
}

// GetJob retrieves a job by ID.
// Returns the job if found, or an error if the job doesn't exist.
func (q *PoolQueue) GetJob(ctx context.Context, jobID string) (*Job, error) {
	return q.backend.GetJob(ctx, jobID)
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

// AcknowledgeCancellation handles cancellation acknowledgment from worker
func (q *PoolQueue) AcknowledgeCancellation(ctx context.Context, jobID string, wasExecuting bool) error {
	return q.backend.AcknowledgeCancellation(ctx, jobID, wasExecuting)
}

// MarkWorkerUnresponsive marks all jobs assigned to the given assigneeID as unresponsive.
// This is called when a worker becomes unresponsive.
func (q *PoolQueue) MarkWorkerUnresponsive(ctx context.Context, assigneeID string) error {
	return q.backend.MarkWorkerUnresponsive(ctx, assigneeID)
}

// Close closes the backend connection and releases any resources.
// This should be called when the queue is no longer needed.
// After closing, the queue should not be used.
func (q *PoolQueue) Close() error {
	return q.backend.Close()
}
