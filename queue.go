package jobpool

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// Queue provides a thread-safe job queue with push-based job assignment,
// capacity management, and tag-based filtering.
type Queue interface {
	// Job submission
	EnqueueJob(ctx context.Context, job *Job) (string, error)
	EnqueueJobs(ctx context.Context, jobs []*Job) ([]string, error)

	// Job streaming (push-based)
	StreamJobs(ctx context.Context, assigneeID string, tags []string, maxAssignedJobs int, ch chan<- []*Job) error

	// Job lifecycle
	CompleteJobs(ctx context.Context, jobID2result map[string][]byte) error
	FailJobs(ctx context.Context, jobID2errorMsg map[string]string) error
	StopJobs(ctx context.Context, jobID2errorMsg map[string]string) error
	StopJobsWithRetry(ctx context.Context, jobID2errorMsg map[string]string) error
	MarkJobsUnknownStopped(ctx context.Context, jobID2errorMsg map[string]string) error
	UpdateJobStatus(ctx context.Context, jobID string, status JobStatus, result []byte, errorMsg string) error

	// Cancellation
	CancelJobs(ctx context.Context, tags []string, jobIDs []string) ([]string, []string, error)
	AcknowledgeCancellation(ctx context.Context, jobIDs2wasExecuting map[string]bool) error

	// Worker management
	MarkWorkerUnresponsive(ctx context.Context, assigneeID string) error

	// Query operations
	GetJob(ctx context.Context, jobID string) (*Job, error)
	GetJobs(ctx context.Context, jobIDs []string) ([]*Job, error)
	GetJobStats(ctx context.Context, tags []string) (*JobStats, error)

	// Maintenance
	CleanupExpiredJobs(ctx context.Context, ttl time.Duration) error
	ResetRunningJobs(ctx context.Context) error
	DeleteJobs(ctx context.Context, tags []string, jobIDs []string) error

	Close() error
}

// subscription represents an active StreamJobs call
type subscription struct {
	id              uint64 // unique subscription ID
	assigneeID      string
	tags            []string
	maxCapacity     int
	currentCapacity int
	ch              chan<- []*Job
	assignedJobs    map[string]bool // job IDs assigned to this subscription
	notifyCh        chan struct{}   // notification channel for this subscription
	done            chan struct{}   // closed when StreamJobs exits
	dequeuing       bool            // true if tryDequeueForSubscription is currently running
	chClosed        bool            // true if channel is closed (to prevent sending to closed channel)
	mu              sync.Mutex
}

// PoolQueue implements the Queue interface using a Backend for storage.
type PoolQueue struct {
	backend       Backend
	logger        *slog.Logger
	mu            sync.RWMutex
	subscriptions map[uint64]*subscription // active StreamJobs calls
	nextSubID     uint64
	closed        bool
	closeCh       chan struct{} // closed when queue is closed
}

// NewPoolQueue creates a new PoolQueue with the given backend.
func NewPoolQueue(backend Backend, logger *slog.Logger) Queue {
	return &PoolQueue{
		backend:       backend,
		logger:        logger,
		subscriptions: make(map[uint64]*subscription),
		closeCh:       make(chan struct{}),
	}
}

// EnqueueJob enqueues a single job and notifies waiting workers.
func (q *PoolQueue) EnqueueJob(ctx context.Context, job *Job) (string, error) {
	if job == nil {
		q.logger.Debug("EnqueueJob: error - job is nil")
		return "", fmt.Errorf("job is nil")
	}
	q.logger.Debug("EnqueueJob", "jobID", job.ID, "jobType", job.JobType, "status", job.Status, "tags", job.Tags)
	if job.ID == "" {
		q.logger.Debug("EnqueueJob: error - job ID is empty")
		return "", fmt.Errorf("job ID is empty")
	}
	if job.Status != JobStatusInitialPending {
		q.logger.Debug("EnqueueJob: error - invalid status", "status", job.Status, "expected", "INITIAL_PENDING")
		return "", fmt.Errorf("job status must be INITIAL_PENDING, got %s", job.Status)
	}

	jobID, err := q.backend.EnqueueJob(ctx, job)
	if err != nil {
		q.logger.Debug("EnqueueJob: backend.EnqueueJob error", "jobID", job.ID, "error", err)
		return "", err
	}
	q.logger.Debug("EnqueueJob: backend.EnqueueJob returned", "jobID", jobID)

	// Notify workers with matching tags
	q.logger.Debug("EnqueueJob: notifying workers", "tags", job.Tags)
	q.notifyWorkers(job.Tags)

	return jobID, nil
}

// EnqueueJobs enqueues multiple jobs and notifies workers (deduplicated by tag combination).
func (q *PoolQueue) EnqueueJobs(ctx context.Context, jobs []*Job) ([]string, error) {
	q.logger.Debug("EnqueueJobs", "count", len(jobs))
	if len(jobs) == 0 {
		q.logger.Debug("EnqueueJobs: empty job list, returning")
		return []string{}, nil
	}

	// Validate all jobs first (before accessing fields)
	for _, job := range jobs {
		if job == nil {
			q.logger.Debug("EnqueueJobs: error - job is nil")
			return nil, fmt.Errorf("job is nil")
		}
	}

	// Collect job IDs and tags for logging (after validation)
	jobIDs := make([]string, len(jobs))
	allTags := make(map[string]bool)
	for i, job := range jobs {
		jobIDs[i] = job.ID
		for _, tag := range job.Tags {
			allTags[tag] = true
		}
	}
	q.logger.Debug("EnqueueJobs", "jobIDs", jobIDs, "uniqueTags", allTags)

	// Validate job fields
	for _, job := range jobs {
		if job.ID == "" {
			q.logger.Debug("EnqueueJobs: error - job ID is empty")
			return nil, fmt.Errorf("job ID is empty")
		}
		if job.Status != JobStatusInitialPending {
			q.logger.Debug("EnqueueJobs: error - invalid status", "status", job.Status, "jobID", job.ID, "expected", "INITIAL_PENDING")
			return nil, fmt.Errorf("job %s status must be INITIAL_PENDING, got %s", job.ID, job.Status)
		}
	}

	jobIDs, err := q.backend.EnqueueJobs(ctx, jobs)
	if err != nil {
		q.logger.Debug("EnqueueJobs: backend.EnqueueJobs error", "error", err)
		return jobIDs, err
	}
	q.logger.Debug("EnqueueJobs: backend.EnqueueJobs returned", "jobIDs", jobIDs)

	// Deduplicate tag combinations and notify
	tagCombos := make(map[string]bool)
	notifiedTagCombos := 0
	for _, job := range jobs {
		comboKey := tagComboKey(job.Tags)
		if !tagCombos[comboKey] {
			tagCombos[comboKey] = true
			notifiedTagCombos++
			q.logger.Debug("EnqueueJobs: notifying workers for tag combo", "tags", job.Tags)
			q.notifyWorkers(job.Tags)
		}
	}
	q.logger.Debug("EnqueueJobs: notified unique tag combinations", "count", notifiedTagCombos)

	return jobIDs, nil
}

// tagComboKey creates a unique key for a tag combination (sorted for consistency).
func tagComboKey(tags []string) string {
	if len(tags) == 0 {
		return ""
	}
	sorted := make([]string, len(tags))
	copy(sorted, tags)
	sort.Strings(sorted)
	key := ""
	for _, tag := range sorted {
		key += tag + ","
	}
	return key
}

// StreamJobs provides push-based job assignment with capacity management.
func (q *PoolQueue) StreamJobs(ctx context.Context, assigneeID string, tags []string, maxAssignedJobs int, ch chan<- []*Job) error {
	q.logger.Debug("StreamJobs: starting", "assigneeID", assigneeID, "tags", tags, "maxAssignedJobs", maxAssignedJobs)
	if assigneeID == "" {
		q.logger.Debug("StreamJobs: error - assigneeID is empty")
		return fmt.Errorf("assigneeID is empty")
	}
	if maxAssignedJobs <= 0 {
		q.logger.Debug("StreamJobs: error - maxAssignedJobs must be > 0", "maxAssignedJobs", maxAssignedJobs)
		return fmt.Errorf("maxAssignedJobs must be > 0, got %d", maxAssignedJobs)
	}
	if ch == nil {
		q.logger.Debug("StreamJobs: error - channel is nil")
		return fmt.Errorf("channel is nil")
	}

	// Register subscription
	q.logger.Debug("StreamJobs: registering subscription", "assigneeID", assigneeID, "tags", tags, "maxAssignedJobs", maxAssignedJobs)
	sub := q.registerSubscription(assigneeID, tags, maxAssignedJobs, ch)
	defer func() {
		q.logger.Debug("StreamJobs: unregistering subscription", "assigneeID", assigneeID, "subID", sub.id)
		close(sub.done) // Signal that StreamJobs has exited
		q.unregisterSubscription(sub.id)
	}()

	// Initial dequeue if capacity available
	q.logger.Debug("StreamJobs: performing initial dequeue", "assigneeID", assigneeID, "subID", sub.id, "tags", tags, "maxCapacity", maxAssignedJobs)
	q.tryDequeueForSubscription(ctx, sub)

	// After registering a new subscription, notify it about any pending jobs
	// This is especially important after service restart when jobs may already be pending
	// and ResetRunningJobs was called before workers connected
	q.logger.Debug("StreamJobs: notifying new subscription about pending jobs", "assigneeID", assigneeID, "subID", sub.id, "tags", tags)
	q.notifyWorkersForSubscription(sub)

	// Main loop: wait for notifications or context cancellation
	q.logger.Debug("StreamJobs: entering main loop", "assigneeID", assigneeID, "subID", sub.id)
	for {
		select {
		case <-ctx.Done():
			// Context cancelled - cleanup assigned but undelivered jobs
			q.logger.Debug("StreamJobs: context cancelled", "assigneeID", assigneeID, "subID", sub.id, "error", ctx.Err())
			// Mark channel as closed to prevent tryDequeueForSubscription from sending to it
			sub.mu.Lock()
			sub.chClosed = true
			sub.mu.Unlock()
			// Run cleanup in goroutine to avoid blocking StreamJobs exit
			// This allows StreamJobs to exit immediately
			go q.cleanupSubscriptionJobs(ctx, sub)
			close(ch)
			return ctx.Err()
		case <-sub.notifyCh:
			// Notification received - try to dequeue jobs
			q.logger.Debug("StreamJobs: notification received", "assigneeID", sub.assigneeID, "subID", sub.id)
			// Check if already dequeuing to avoid concurrent dequeues
			sub.mu.Lock()
			dequeuing := sub.dequeuing
			if !dequeuing {
				sub.dequeuing = true
			}
			sub.mu.Unlock()

			if dequeuing {
				// Already dequeuing, skip this notification
				q.logger.Debug("StreamJobs: already dequeuing, skipping notification", "assigneeID", sub.assigneeID, "subID", sub.id)
				continue
			}

			// Run tryDequeueForSubscription in a goroutine to avoid blocking the select loop
			// This allows StreamJobs to check ctx.Done() even if tryDequeueForSubscription is blocking
			go func() {
				defer func() {
					sub.mu.Lock()
					sub.dequeuing = false
					sub.mu.Unlock()
				}()
				q.tryDequeueForSubscription(ctx, sub)
			}()
		case <-q.closeCh:
			// Queue closed
			q.logger.Debug("StreamJobs: queue closed", "assigneeID", assigneeID, "subID", sub.id)
			// Mark channel as closed to prevent tryDequeueForSubscription from sending to it
			sub.mu.Lock()
			sub.chClosed = true
			sub.mu.Unlock()
			// Run cleanup in goroutine to avoid blocking StreamJobs exit
			// This allows StreamJobs to exit immediately when queue is closed
			go q.cleanupSubscriptionJobs(ctx, sub)
			close(ch)
			return nil
		}
	}
}

// registerSubscription registers a new StreamJobs subscription.
func (q *PoolQueue) registerSubscription(assigneeID string, tags []string, maxCapacity int, ch chan<- []*Job) *subscription {
	q.logger.Debug("registerSubscription", "assigneeID", assigneeID, "tags", tags, "maxCapacity", maxCapacity)
	q.mu.Lock()
	defer q.mu.Unlock()

	sub := &subscription{
		id:              q.nextSubID,
		assigneeID:      assigneeID,
		tags:            tags,
		maxCapacity:     maxCapacity,
		currentCapacity: maxCapacity,
		ch:              ch,
		assignedJobs:    make(map[string]bool),
		notifyCh:        make(chan struct{}, 1), // buffered for non-blocking
		done:            make(chan struct{}),    // closed when StreamJobs exits
	}
	q.nextSubID++
	q.subscriptions[sub.id] = sub
	q.logger.Debug("registerSubscription: registered", "subID", sub.id, "assigneeID", assigneeID, "totalSubscriptions", len(q.subscriptions))

	return sub
}

// unregisterSubscription removes a subscription.
func (q *PoolQueue) unregisterSubscription(subID uint64) {
	q.logger.Debug("unregisterSubscription", "subID", subID)
	q.mu.Lock()
	defer q.mu.Unlock()
	sub, exists := q.subscriptions[subID]
	if exists {
		q.logger.Debug("unregisterSubscription: removing", "subID", subID, "assigneeID", sub.assigneeID, "remainingSubscriptions", len(q.subscriptions)-1)
	} else {
		q.logger.Debug("unregisterSubscription: subID not found", "subID", subID)
	}
	delete(q.subscriptions, subID)
	// Note: We rely on context cancellation in StreamJobs to exit properly
	// The subscription is removed from the map, so no new notifications will be sent to it
}

// tryDequeueForSubscription attempts to dequeue jobs for a subscription.
func (q *PoolQueue) tryDequeueForSubscription(ctx context.Context, sub *subscription) {
	q.logger.Debug("tryDequeueForSubscription", "assigneeID", sub.assigneeID, "subID", sub.id)
	for {
		// Check context cancellation first
		select {
		case <-ctx.Done():
			q.logger.Debug("tryDequeueForSubscription: context cancelled", "assigneeID", sub.assigneeID, "error", ctx.Err())
			return
		default:
		}

		// Check if queue is closed - if so, exit immediately
		q.mu.RLock()
		queueClosed := q.closed
		q.mu.RUnlock()
		if queueClosed {
			q.logger.Debug("tryDequeueForSubscription: queue closed, exiting", "assigneeID", sub.assigneeID)
			return
		}

		sub.mu.Lock()
		availableCapacity := sub.currentCapacity
		sub.mu.Unlock()

		q.logger.Debug("tryDequeueForSubscription", "assigneeID", sub.assigneeID, "availableCapacity", availableCapacity)
		if availableCapacity <= 0 {
			q.logger.Debug("tryDequeueForSubscription: no capacity, returning", "assigneeID", sub.assigneeID)
			return
		}

		// Dequeue jobs from backend - this may block, but we check queue closed status before and after
		q.logger.Debug("tryDequeueForSubscription: calling DequeueJobs", "assigneeID", sub.assigneeID, "tags", sub.tags, "availableCapacity", availableCapacity)
		jobs, err := q.backend.DequeueJobs(ctx, sub.assigneeID, sub.tags, availableCapacity)

		// Check if queue was closed during the database operation
		q.mu.RLock()
		queueClosed = q.closed
		q.mu.RUnlock()
		if queueClosed {
			q.logger.Debug("tryDequeueForSubscription: queue closed during DequeueJobs, exiting", "assigneeID", sub.assigneeID)
			return
		}

		if err != nil {
			q.logger.Debug("tryDequeueForSubscription: DequeueJobs error", "error", err, "assigneeID", sub.assigneeID, "tags", sub.tags)
			// Log error but don't fail - will retry on next notification
			return
		}

		q.logger.Debug("tryDequeueForSubscription: DequeueJobs returned", "jobCount", len(jobs), "assigneeID", sub.assigneeID, "tags", sub.tags)
		if len(jobs) == 0 {
			q.logger.Debug("tryDequeueForSubscription: no jobs found, returning", "assigneeID", sub.assigneeID, "tags", sub.tags, "availableCapacity", availableCapacity)
			return
		}

		// Log job IDs and their tags for debugging
		jobInfo := make([]map[string]interface{}, 0, len(jobs))
		for _, job := range jobs {
			jobInfo = append(jobInfo, map[string]interface{}{
				"jobID":   job.ID,
				"jobTags": job.Tags,
				"status":  job.Status,
			})
		}
		q.logger.Debug("tryDequeueForSubscription: jobs dequeued", "assigneeID", sub.assigneeID, "jobs", jobInfo)

		// Update subscription capacity and track assigned jobs
		sub.mu.Lock()
		sub.currentCapacity -= len(jobs)
		for _, job := range jobs {
			sub.assignedJobs[job.ID] = true
			q.logger.Debug("tryDequeueForSubscription: assigned job", "jobID", job.ID, "assigneeID", sub.assigneeID)
		}
		sub.mu.Unlock()

		// Check if channel is closed before trying to send
		sub.mu.Lock()
		chClosed := sub.chClosed
		sub.mu.Unlock()
		if chClosed {
			q.logger.Debug("tryDequeueForSubscription: channel closed, exiting", "assigneeID", sub.assigneeID)
			return
		}

		// Send jobs to channel
		// If channel is full, jobs are still assigned (per spec) but we'll retry sending
		q.logger.Debug("tryDequeueForSubscription: sending jobs to channel", "jobCount", len(jobs), "assigneeID", sub.assigneeID)
		select {
		case <-ctx.Done():
			// Context cancelled, exit immediately
			q.logger.Debug("tryDequeueForSubscription: context cancelled", "assigneeID", sub.assigneeID, "error", ctx.Err())
			return
		case sub.ch <- jobs:
			q.logger.Debug("tryDequeueForSubscription: successfully sent jobs to channel", "jobCount", len(jobs), "assigneeID", sub.assigneeID)
			// Successfully sent - continue to check if more capacity is available
			continue
		default:
			q.logger.Debug("tryDequeueForSubscription: channel full, jobs still assigned", "assigneeID", sub.assigneeID)
			// Channel full - jobs are still assigned and will be sent later
			// Schedule a retry by sending a notification
			select {
			case <-ctx.Done():
				// Context cancelled, exit immediately
				q.logger.Debug("tryDequeueForSubscription: context cancelled while scheduling retry", "assigneeID", sub.assigneeID, "error", ctx.Err())
				return
			case sub.notifyCh <- struct{}{}:
			default:
				// Notification channel also full, will retry on next external notification
			}
			return
		}
	}
}

// cleanupSubscriptionJobs transitions RUNNING jobs assigned to this subscription to FAILED_RETRY.
// This function is called asynchronously (in a goroutine) to avoid blocking StreamJobs exit.
func (q *PoolQueue) cleanupSubscriptionJobs(ctx context.Context, sub *subscription) {
	q.logger.Debug("cleanupSubscriptionJobs", "assigneeID", sub.assigneeID, "subID", sub.id)
	// Use background context with timeout to ensure cleanup completes but doesn't hang forever
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sub.mu.Lock()
	jobIDs := make([]string, 0, len(sub.assignedJobs))
	for jobID := range sub.assignedJobs {
		jobIDs = append(jobIDs, jobID)
	}
	sub.mu.Unlock()
	q.logger.Debug("cleanupSubscriptionJobs: found assigned jobs", "count", len(jobIDs), "assigneeID", sub.assigneeID, "subID", sub.id, "jobIDs", jobIDs)

	// Determine termination reason for error message
	errorMsg := "StreamJobs terminated"
	if ctx.Err() != nil {
		errorMsg = "StreamJobs terminated: " + ctx.Err().Error()
	}
	q.logger.Debug("cleanupSubscriptionJobs", "errorMsg", errorMsg, "assigneeID", sub.assigneeID, "subID", sub.id)

	// Check each job and transition if still RUNNING
	transitionedCount := 0
	for _, jobID := range jobIDs {
		job, err := q.backend.GetJob(cleanupCtx, jobID)
		if err != nil {
			q.logger.Debug("cleanupSubscriptionJobs: GetJob error", "jobID", jobID, "error", err)
			continue
		}
		if job.Status == JobStatusRunning {
			// Transition to FAILED_RETRY with termination error message
			q.logger.Debug("cleanupSubscriptionJobs: transitioning job from RUNNING to FAILED_RETRY", "jobID", jobID)
			_, err := q.backend.FailJob(cleanupCtx, jobID, errorMsg)
			if err != nil {
				q.logger.Debug("cleanupSubscriptionJobs: FailJob error", "jobID", jobID, "error", err)
			} else {
				transitionedCount++
			}
		} else {
			q.logger.Debug("cleanupSubscriptionJobs: job not transitioning", "jobID", jobID, "status", job.Status)
		}
	}
	q.logger.Debug("cleanupSubscriptionJobs: transitioned jobs", "count", transitionedCount, "assigneeID", sub.assigneeID, "subID", sub.id)
}

// notifyWorkers notifies all matching subscriptions about new eligible jobs.
func (q *PoolQueue) notifyWorkers(jobTags []string) {
	q.logger.Debug("notifyWorkers", "jobTags", jobTags)
	q.mu.RLock()
	subs := make([]*subscription, 0, len(q.subscriptions))
	for _, sub := range q.subscriptions {
		subs = append(subs, sub)
	}
	totalSubs := len(q.subscriptions)
	q.mu.RUnlock()
	q.logger.Debug("notifyWorkers: found total subscriptions", "count", totalSubs)

	// Notify all matching subscriptions (at-most-once per subscription)
	notifiedCount := 0
	skippedNoMatch := 0
	skippedNoCapacity := 0
	skippedFullChannel := 0
	for _, sub := range subs {
		// Check if subscription matches job tags
		// If jobTags is nil, notify all subscriptions (used when we don't know which tags are available)
		matches := jobTags == nil || matchesTags(jobTags, sub.tags)
		if !matches {
			skippedNoMatch++
			continue
		}

		sub.mu.Lock()
		hasCapacity := sub.currentCapacity > 0
		currentCap := sub.currentCapacity
		sub.mu.Unlock()

		if !hasCapacity {
			skippedNoCapacity++
			q.logger.Debug("notifyWorkers: skipping subscription (no capacity)", "subID", sub.id, "assigneeID", sub.assigneeID, "capacity", currentCap)
			continue
		}

		// Send notification to this subscription (non-blocking, at-most-once)
		select {
		case sub.notifyCh <- struct{}{}:
			notifiedCount++
			q.logger.Debug("notifyWorkers: notified subscription", "subID", sub.id, "assigneeID", sub.assigneeID, "tags", sub.tags, "capacity", currentCap)
		default:
			skippedFullChannel++
			q.logger.Debug("notifyWorkers: channel full (notification already pending)", "subID", sub.id, "assigneeID", sub.assigneeID)
			// Channel full - notification already pending
		}
	}
	q.logger.Debug("notifyWorkers: completed", "notified", notifiedCount, "skippedNoMatch", skippedNoMatch, "skippedNoCapacity", skippedNoCapacity, "skippedFullChannel", skippedFullChannel)
}

// notifyWorkersForSubscription notifies a specific subscription about pending jobs.
// This is used when a new subscription is registered to ensure it gets a chance to dequeue
// any pending jobs that may have been missed in the initial dequeue.
func (q *PoolQueue) notifyWorkersForSubscription(sub *subscription) {
	sub.mu.Lock()
	hasCapacity := sub.currentCapacity > 0
	currentCap := sub.currentCapacity
	sub.mu.Unlock()

	if !hasCapacity {
		q.logger.Debug("notifyWorkersForSubscription: skipping (no capacity)", "assigneeID", sub.assigneeID, "subID", sub.id, "capacity", currentCap)
		return
	}

	// Send notification to this subscription (non-blocking, at-most-once)
	select {
	case sub.notifyCh <- struct{}{}:
		q.logger.Debug("notifyWorkersForSubscription: notified subscription", "assigneeID", sub.assigneeID, "subID", sub.id, "capacity", currentCap)
	default:
		q.logger.Debug("notifyWorkersForSubscription: channel full (notification already pending)", "assigneeID", sub.assigneeID, "subID", sub.id)
		// Channel full - notification already pending
	}
}

// matchesTags checks if job tags contain all subscription tags (AND logic).
func matchesTags(jobTags []string, subTags []string) bool {
	if len(subTags) == 0 {
		return true // Empty tags means accept all jobs
	}

	jobTagSet := make(map[string]bool)
	for _, tag := range jobTags {
		jobTagSet[tag] = true
	}

	for _, subTag := range subTags {
		if !jobTagSet[subTag] {
			return false
		}
	}

	return true
}

// CompleteJobs completes multiple jobs and frees capacity.
func (q *PoolQueue) CompleteJobs(ctx context.Context, jobID2result map[string][]byte) error {
	q.logger.Debug("CompleteJobs", "count", len(jobID2result))
	if len(jobID2result) == 0 {
		q.logger.Debug("CompleteJobs: empty map, returning")
		return nil
	}

	// Aggregate freed capacity and collect unique tag combinations
	allFreedAssigneeIDs := make(map[string]int)
	tagCombos := make(map[string]bool) // Track unique tag combinations by key
	var firstError error

	// Process each job
	for jobID, result := range jobID2result {
		// Get job before completion to get tags for notification
		job, err := q.backend.GetJob(ctx, jobID)
		if err != nil {
			q.logger.Debug("CompleteJobs: GetJob error", "jobID", jobID, "error", err)
			if firstError == nil {
				firstError = err
			}
			continue
		}
		jobTags := job.Tags
		q.logger.Debug("CompleteJobs", "jobID", jobID, "assigneeID", job.AssigneeID, "tags", jobTags)

		// Complete job in backend
		freedAssigneeIDs, err := q.backend.CompleteJob(ctx, jobID, result)
		if err != nil {
			q.logger.Debug("CompleteJobs: backend.CompleteJob error", "jobID", jobID, "error", err)
			if firstError == nil {
				firstError = err
			}
			continue
		}
		q.logger.Debug("CompleteJobs: backend returned freedAssigneeIDs", "freedAssigneeIDs", freedAssigneeIDs, "jobID", jobID)

		// Aggregate freed capacity
		for assigneeID, count := range freedAssigneeIDs {
			allFreedAssigneeIDs[assigneeID] += count
		}

		// Collect unique tag combinations
		comboKey := tagComboKey(jobTags)
		tagCombos[comboKey] = true
	}

	// Notify workers - use nil to notify all workers since we have multiple tag combinations
	// This ensures all eligible workers are notified regardless of their tag filters
	if len(tagCombos) > 0 || len(allFreedAssigneeIDs) > 0 {
		q.freeCapacityAndNotify(allFreedAssigneeIDs, nil)
	}

	if firstError != nil {
		q.logger.Debug("CompleteJobs: some jobs failed", "error", firstError)
		return firstError
	}

	return nil
}

// FailJobs fails multiple jobs and frees capacity.
func (q *PoolQueue) FailJobs(ctx context.Context, jobID2errorMsg map[string]string) error {
	q.logger.Debug("FailJobs", "count", len(jobID2errorMsg))
	if len(jobID2errorMsg) == 0 {
		q.logger.Debug("FailJobs: empty map, returning")
		return nil
	}

	// Validate all error messages first
	for jobID, errorMsg := range jobID2errorMsg {
		if errorMsg == "" {
			return fmt.Errorf("errorMsg is required for job %s", jobID)
		}
	}

	// Aggregate freed capacity and collect unique tag combinations
	allFreedAssigneeIDs := make(map[string]int)
	tagCombos := make(map[string]bool)
	var firstError error

	// Process each job
	for jobID, errorMsg := range jobID2errorMsg {
		// Get job before failure to get tags for notification
		job, err := q.backend.GetJob(ctx, jobID)
		if err != nil {
			q.logger.Debug("FailJobs: GetJob error", "jobID", jobID, "error", err)
			if firstError == nil {
				firstError = err
			}
			continue
		}
		q.logger.Debug("FailJobs", "jobID", jobID, "currentStatus", job.Status, "assigneeID", job.AssigneeID, "tags", job.Tags)

		// Fail job in backend
		freedAssigneeIDs, err := q.backend.FailJob(ctx, jobID, errorMsg)
		if err != nil {
			q.logger.Debug("FailJobs: backend.FailJob error", "jobID", jobID, "error", err)
			if firstError == nil {
				firstError = err
			}
			continue
		}
		q.logger.Debug("FailJobs: backend.FailJob returned freedAssigneeIDs", "freedAssigneeIDs", freedAssigneeIDs, "jobID", jobID)

		// Aggregate freed capacity
		for assigneeID, count := range freedAssigneeIDs {
			allFreedAssigneeIDs[assigneeID] += count
		}

		// Collect unique tag combinations
		comboKey := tagComboKey(job.Tags)
		tagCombos[comboKey] = true
	}

	// Notify workers - use nil to notify all workers since we have multiple tag combinations
	if len(tagCombos) > 0 || len(allFreedAssigneeIDs) > 0 {
		q.freeCapacityAndNotify(allFreedAssigneeIDs, nil)
	}

	if firstError != nil {
		q.logger.Debug("FailJobs: some jobs failed", "error", firstError)
		return firstError
	}

	return nil
}

// StopJobs stops multiple jobs and frees capacity.
func (q *PoolQueue) StopJobs(ctx context.Context, jobID2errorMsg map[string]string) error {
	q.logger.Debug("StopJobs", "count", len(jobID2errorMsg))
	if len(jobID2errorMsg) == 0 {
		q.logger.Debug("StopJobs: empty map, returning")
		return nil
	}

	// Aggregate freed capacity and collect unique tag combinations
	allFreedAssigneeIDs := make(map[string]int)
	tagCombos := make(map[string]bool)
	var firstError error

	// Process each job
	for jobID, errorMsg := range jobID2errorMsg {
		// Get job before stopping
		job, err := q.backend.GetJob(ctx, jobID)
		if err != nil {
			q.logger.Debug("StopJobs: GetJob error", "jobID", jobID, "error", err)
			if firstError == nil {
				firstError = err
			}
			continue
		}
		q.logger.Debug("StopJobs", "jobID", jobID, "currentStatus", job.Status, "assigneeID", job.AssigneeID, "tags", job.Tags)

		// Stop job in backend
		freedAssigneeIDs, err := q.backend.StopJob(ctx, jobID, errorMsg)
		if err != nil {
			q.logger.Debug("StopJobs: backend.StopJob error", "jobID", jobID, "error", err)
			if firstError == nil {
				firstError = err
			}
			continue
		}
		q.logger.Debug("StopJobs: backend.StopJob returned freedAssigneeIDs", "freedAssigneeIDs", freedAssigneeIDs, "jobID", jobID)

		// Aggregate freed capacity
		for assigneeID, count := range freedAssigneeIDs {
			allFreedAssigneeIDs[assigneeID] += count
		}

		// Collect unique tag combinations
		comboKey := tagComboKey(job.Tags)
		tagCombos[comboKey] = true
	}

	// Notify workers - use nil to notify all workers since we have multiple tag combinations
	if len(tagCombos) > 0 || len(allFreedAssigneeIDs) > 0 {
		q.freeCapacityAndNotify(allFreedAssigneeIDs, nil)
	}

	if firstError != nil {
		q.logger.Debug("StopJobs: some jobs failed", "error", firstError)
		return firstError
	}

	return nil
}

// StopJobsWithRetry stops multiple jobs with retry increment and frees capacity.
func (q *PoolQueue) StopJobsWithRetry(ctx context.Context, jobID2errorMsg map[string]string) error {
	q.logger.Debug("StopJobsWithRetry", "count", len(jobID2errorMsg))
	if len(jobID2errorMsg) == 0 {
		q.logger.Debug("StopJobsWithRetry: empty map, returning")
		return nil
	}

	// Aggregate freed capacity and collect unique tag combinations
	allFreedAssigneeIDs := make(map[string]int)
	tagCombos := make(map[string]bool)
	var firstError error

	// Process each job
	for jobID, errorMsg := range jobID2errorMsg {
		// Get job before stopping
		job, err := q.backend.GetJob(ctx, jobID)
		if err != nil {
			q.logger.Debug("StopJobsWithRetry: GetJob error", "jobID", jobID, "error", err)
			if firstError == nil {
				firstError = err
			}
			continue
		}
		q.logger.Debug("StopJobsWithRetry", "jobID", jobID, "currentStatus", job.Status, "assigneeID", job.AssigneeID, "tags", job.Tags)

		// Stop job with retry in backend
		freedAssigneeIDs, err := q.backend.StopJobWithRetry(ctx, jobID, errorMsg)
		if err != nil {
			q.logger.Debug("StopJobsWithRetry: backend.StopJobWithRetry error", "jobID", jobID, "error", err)
			if firstError == nil {
				firstError = err
			}
			continue
		}
		q.logger.Debug("StopJobsWithRetry: backend.StopJobWithRetry returned freedAssigneeIDs", "freedAssigneeIDs", freedAssigneeIDs, "jobID", jobID)

		// Aggregate freed capacity
		for assigneeID, count := range freedAssigneeIDs {
			allFreedAssigneeIDs[assigneeID] += count
		}

		// Collect unique tag combinations
		comboKey := tagComboKey(job.Tags)
		tagCombos[comboKey] = true
	}

	// Notify workers - use nil to notify all workers since we have multiple tag combinations
	if len(tagCombos) > 0 || len(allFreedAssigneeIDs) > 0 {
		q.freeCapacityAndNotify(allFreedAssigneeIDs, nil)
	}

	if firstError != nil {
		q.logger.Debug("StopJobsWithRetry: some jobs failed", "error", firstError)
		return firstError
	}

	return nil
}

// MarkJobsUnknownStopped marks multiple jobs as unknown stopped and frees capacity.
func (q *PoolQueue) MarkJobsUnknownStopped(ctx context.Context, jobID2errorMsg map[string]string) error {
	q.logger.Debug("MarkJobsUnknownStopped", "count", len(jobID2errorMsg))
	if len(jobID2errorMsg) == 0 {
		q.logger.Debug("MarkJobsUnknownStopped: empty map, returning")
		return nil
	}

	// Aggregate freed capacity and collect unique tag combinations
	allFreedAssigneeIDs := make(map[string]int)
	tagCombos := make(map[string]bool)
	var firstError error

	// Process each job
	for jobID, errorMsg := range jobID2errorMsg {
		// Get job before marking
		job, err := q.backend.GetJob(ctx, jobID)
		if err != nil {
			q.logger.Debug("MarkJobsUnknownStopped: GetJob error", "jobID", jobID, "error", err)
			if firstError == nil {
				firstError = err
			}
			continue
		}
		q.logger.Debug("MarkJobsUnknownStopped", "jobID", jobID, "currentStatus", job.Status, "assigneeID", job.AssigneeID, "tags", job.Tags)

		// Mark job in backend
		freedAssigneeIDs, err := q.backend.MarkJobUnknownStopped(ctx, jobID, errorMsg)
		if err != nil {
			q.logger.Debug("MarkJobsUnknownStopped: backend.MarkJobUnknownStopped error", "jobID", jobID, "error", err)
			if firstError == nil {
				firstError = err
			}
			continue
		}
		q.logger.Debug("MarkJobsUnknownStopped: backend.MarkJobUnknownStopped returned freedAssigneeIDs", "freedAssigneeIDs", freedAssigneeIDs, "jobID", jobID)

		// Aggregate freed capacity
		for assigneeID, count := range freedAssigneeIDs {
			allFreedAssigneeIDs[assigneeID] += count
		}

		// Collect unique tag combinations
		comboKey := tagComboKey(job.Tags)
		tagCombos[comboKey] = true
	}

	// Notify workers - use nil to notify all workers since we have multiple tag combinations
	if len(tagCombos) > 0 || len(allFreedAssigneeIDs) > 0 {
		q.freeCapacityAndNotify(allFreedAssigneeIDs, nil)
	}

	if firstError != nil {
		q.logger.Debug("MarkJobsUnknownStopped: some jobs failed", "error", firstError)
		return firstError
	}

	return nil
}

// freeCapacityAndNotify frees capacity for subscriptions and notifies them.
func (q *PoolQueue) freeCapacityAndNotify(freedAssigneeIDs map[string]int, jobTags []string) {
	q.logger.Debug("freeCapacityAndNotify", "freedAssigneeIDs", freedAssigneeIDs, "jobTags", jobTags)
	if len(freedAssigneeIDs) == 0 {
		// Still notify about eligible jobs even if no capacity was freed
		q.logger.Debug("freeCapacityAndNotify: no capacity freed, notifying all workers")
		q.notifyWorkers(jobTags)
		return
	}

	q.mu.RLock()
	subs := make([]*subscription, 0, len(q.subscriptions))
	for _, sub := range q.subscriptions {
		// Check if this subscription's assigneeID had capacity freed
		if count, ok := freedAssigneeIDs[sub.assigneeID]; ok && count > 0 {
			subs = append(subs, sub)
			q.logger.Debug("freeCapacityAndNotify: found subscription", "assigneeID", sub.assigneeID, "subID", sub.id, "count", count)
		}
	}
	q.mu.RUnlock()

	q.logger.Debug("freeCapacityAndNotify: found matching subscriptions", "count", len(subs))

	// Update capacity for affected subscriptions and notify them
	for _, sub := range subs {
		sub.mu.Lock()
		count := freedAssigneeIDs[sub.assigneeID]
		oldCapacity := sub.currentCapacity
		sub.currentCapacity += count
		if sub.currentCapacity > sub.maxCapacity {
			sub.currentCapacity = sub.maxCapacity
		}
		hasCapacity := sub.currentCapacity > 0
		newCapacity := sub.currentCapacity
		// Note: We don't know which specific jobs were freed, so we can't remove them from assignedJobs
		// This is acceptable - the map will grow but jobs will be cleaned up on subscription end
		sub.mu.Unlock()

		q.logger.Debug("freeCapacityAndNotify: updated capacity", "assigneeID", sub.assigneeID, "subID", sub.id, "oldCapacity", oldCapacity, "newCapacity", newCapacity, "hasCapacity", hasCapacity)

		// Notify subscription if it has capacity (non-blocking)
		if hasCapacity {
			select {
			case sub.notifyCh <- struct{}{}:
				q.logger.Debug("freeCapacityAndNotify: sent notification", "assigneeID", sub.assigneeID, "subID", sub.id)
			default:
				q.logger.Debug("freeCapacityAndNotify: notification channel full", "assigneeID", sub.assigneeID, "subID", sub.id)
				// Notification already pending
			}
		} else {
			q.logger.Debug("freeCapacityAndNotify: no capacity, not notifying", "assigneeID", sub.assigneeID, "subID", sub.id)
		}
	}

	// Also notify all subscriptions about eligible jobs (they may have capacity from other sources)
	q.logger.Debug("freeCapacityAndNotify: calling notifyWorkers for all subscriptions")
	q.notifyWorkers(jobTags)
}

// CancelJobs cancels jobs by tags and/or job IDs.
func (q *PoolQueue) CancelJobs(ctx context.Context, tags []string, jobIDs []string) ([]string, []string, error) {
	q.logger.Debug("CancelJobs", "tags", tags, "jobIDs", jobIDs)
	cancelledByTags, cancelledByIDs, err := q.backend.CancelJobs(ctx, tags, jobIDs)
	if err != nil {
		q.logger.Debug("CancelJobs: backend error", "error", err)
	} else {
		q.logger.Debug("CancelJobs: completed", "cancelledByTags", cancelledByTags, "cancelledByIDs", cancelledByIDs)
	}
	return cancelledByTags, cancelledByIDs, err
}

// AcknowledgeCancellation handles cancellation acknowledgment for multiple jobs.
func (q *PoolQueue) AcknowledgeCancellation(ctx context.Context, jobIDs2wasExecuting map[string]bool) error {
	q.logger.Debug("AcknowledgeCancellation", "count", len(jobIDs2wasExecuting))
	if len(jobIDs2wasExecuting) == 0 {
		q.logger.Debug("AcknowledgeCancellation: empty map, returning")
		return nil
	}

	// Aggregate freed capacity and collect unique tag combinations
	allFreedAssigneeIDs := make(map[string]int)
	tagCombos := make(map[string]bool)
	var firstError error

	// Process each job
	for jobID, wasExecuting := range jobIDs2wasExecuting {
		// Get job before acknowledgment
		job, err := q.backend.GetJob(ctx, jobID)
		if err != nil {
			q.logger.Debug("AcknowledgeCancellation: GetJob error", "jobID", jobID, "error", err)
			if firstError == nil {
				firstError = err
			}
			continue
		}
		q.logger.Debug("AcknowledgeCancellation", "jobID", jobID, "currentStatus", job.Status, "assigneeID", job.AssigneeID, "tags", job.Tags, "wasExecuting", wasExecuting)

		// Acknowledge in backend
		err = q.backend.AcknowledgeCancellation(ctx, jobID, wasExecuting)
		if err != nil {
			q.logger.Debug("AcknowledgeCancellation: backend error", "jobID", jobID, "error", err)
			if firstError == nil {
				firstError = err
			}
			continue
		}
		q.logger.Debug("AcknowledgeCancellation: backend successfully acknowledged cancellation", "jobID", jobID)

		// Free capacity if job was executing
		if wasExecuting && job.AssigneeID != "" {
			allFreedAssigneeIDs[job.AssigneeID]++
			q.logger.Debug("AcknowledgeCancellation: freeing capacity", "assigneeID", job.AssigneeID)
		}

		// Collect unique tag combinations (only if capacity was freed)
		if wasExecuting {
			comboKey := tagComboKey(job.Tags)
			tagCombos[comboKey] = true
		}
	}

	// Notify workers - use nil to notify all workers since we have multiple tag combinations
	if len(tagCombos) > 0 || len(allFreedAssigneeIDs) > 0 {
		q.freeCapacityAndNotify(allFreedAssigneeIDs, nil)
	}

	if firstError != nil {
		q.logger.Debug("AcknowledgeCancellation: some jobs failed", "error", firstError)
		return firstError
	}

	return nil
}

// MarkWorkerUnresponsive marks all jobs for a worker as unresponsive.
func (q *PoolQueue) MarkWorkerUnresponsive(ctx context.Context, assigneeID string) error {
	q.logger.Debug("MarkWorkerUnresponsive", "assigneeID", assigneeID)
	// Mark in backend
	err := q.backend.MarkWorkerUnresponsive(ctx, assigneeID)
	if err != nil {
		q.logger.Debug("MarkWorkerUnresponsive: backend error", "assigneeID", assigneeID, "error", err)
		return err
	}
	q.logger.Debug("MarkWorkerUnresponsive: successfully marked as unresponsive", "assigneeID", assigneeID)

	// Free capacity for all subscriptions with this assigneeID
	q.mu.RLock()
	subs := make([]*subscription, 0)
	for _, sub := range q.subscriptions {
		if sub.assigneeID == assigneeID {
			subs = append(subs, sub)
		}
	}
	q.mu.RUnlock()

	// Update capacity for affected subscriptions
	for _, sub := range subs {
		sub.mu.Lock()
		// Reset capacity to max (all jobs for this worker are now unassigned)
		sub.currentCapacity = sub.maxCapacity
		// Clear assigned jobs tracking
		sub.assignedJobs = make(map[string]bool)
		sub.mu.Unlock()

		// Notify subscription (non-blocking)
		select {
		case sub.notifyCh <- struct{}{}:
		default:
			// Notification already pending
		}
	}

	// Notify all workers about newly eligible jobs
	// Pass nil to notify all subscriptions regardless of tags, since we don't know
	// which tags the newly eligible jobs have
	q.notifyWorkers(nil)

	return nil
}

// GetJob retrieves a job by ID.
func (q *PoolQueue) GetJob(ctx context.Context, jobID string) (*Job, error) {
	job, err := q.backend.GetJob(ctx, jobID)
	if err != nil {
		q.logger.Debug("GetJob: error", "jobID", jobID, "error", err)
	} else {
		q.logger.Debug("GetJob", "jobID", jobID, "status", job.Status)
	}
	return job, err
}

// GetJobs retrieves multiple jobs by IDs.
func (q *PoolQueue) GetJobs(ctx context.Context, jobIDs []string) ([]*Job, error) {
	q.logger.Debug("GetJobs", "count", len(jobIDs))
	if len(jobIDs) == 0 {
		q.logger.Debug("GetJobs: empty slice, returning")
		return []*Job{}, nil
	}

	jobs := make([]*Job, 0, len(jobIDs))
	var firstError error

	for _, jobID := range jobIDs {
		job, err := q.backend.GetJob(ctx, jobID)
		if err != nil {
			q.logger.Debug("GetJobs: GetJob error", "jobID", jobID, "error", err)
			if firstError == nil {
				firstError = err
			}
			continue
		}
		jobs = append(jobs, job)
	}

	if firstError != nil && len(jobs) == 0 {
		// All jobs failed
		return nil, firstError
	}

	q.logger.Debug("GetJobs: completed", "requested", len(jobIDs), "found", len(jobs))
	return jobs, firstError
}

// GetJobStats gets statistics for jobs matching tags.
func (q *PoolQueue) GetJobStats(ctx context.Context, tags []string) (*JobStats, error) {
	q.logger.Debug("GetJobStats", "tags", tags)
	stats, err := q.backend.GetJobStats(ctx, tags)
	if err != nil {
		q.logger.Debug("GetJobStats: backend error", "error", err)
	} else if stats != nil {
		q.logger.Debug("GetJobStats: completed", "TotalJobs", stats.TotalJobs, "PendingJobs", stats.PendingJobs, "RunningJobs", stats.RunningJobs, "CompletedJobs", stats.CompletedJobs, "StoppedJobs", stats.StoppedJobs, "FailedJobs", stats.FailedJobs, "TotalRetries", stats.TotalRetries)
	}
	return stats, err
}

// CleanupExpiredJobs deletes completed jobs older than TTL.
func (q *PoolQueue) CleanupExpiredJobs(ctx context.Context, ttl time.Duration) error {
	q.logger.Debug("CleanupExpiredJobs", "ttl", ttl)
	if ttl <= 0 {
		q.logger.Debug("CleanupExpiredJobs: invalid ttl", "ttl", ttl)
		return fmt.Errorf("ttl must be > 0, got %v", ttl)
	}
	err := q.backend.CleanupExpiredJobs(ctx, ttl)
	if err != nil {
		q.logger.Debug("CleanupExpiredJobs: backend error", "error", err)
	} else {
		q.logger.Debug("CleanupExpiredJobs: successfully cleaned up expired jobs")
	}
	return err
}

// ResetRunningJobs resets all running jobs to unknown retry.
func (q *PoolQueue) ResetRunningJobs(ctx context.Context) error {
	q.logger.Debug("ResetRunningJobs: starting")
	err := q.backend.ResetRunningJobs(ctx)
	if err != nil {
		q.logger.Debug("ResetRunningJobs: backend error", "error", err)
		return err
	}
	q.logger.Debug("ResetRunningJobs: successfully reset all running jobs")

	// Per spec (queue.md lines 436-439): Workers with matching tags notified when jobs become eligible
	// Since we don't know which tags are affected, notify all workers
	q.logger.Debug("ResetRunningJobs: notifying all workers about newly eligible jobs")
	q.notifyWorkers(nil)

	return nil
}

// DeleteJobs deletes jobs by tags and/or job IDs.
func (q *PoolQueue) DeleteJobs(ctx context.Context, tags []string, jobIDs []string) error {
	q.logger.Debug("DeleteJobs", "tags", tags, "jobIDs", jobIDs)
	err := q.backend.DeleteJobs(ctx, tags, jobIDs)
	if err != nil {
		q.logger.Debug("DeleteJobs: backend error", "error", err)
	} else {
		q.logger.Debug("DeleteJobs: successfully deleted jobs")
	}
	return err
}

// UpdateJobStatus updates a job's status, result, and error message.
func (q *PoolQueue) UpdateJobStatus(ctx context.Context, jobID string, status JobStatus, result []byte, errorMsg string) error {
	q.logger.Debug("UpdateJobStatus", "jobID", jobID, "status", status)
	// Get job before update to get tags for notification
	job, err := q.backend.GetJob(ctx, jobID)
	if err != nil {
		q.logger.Debug("UpdateJobStatus: GetJob error", "jobID", jobID, "error", err)
		return err
	}
	jobTags := job.Tags

	// Update job in backend
	freedAssigneeIDs, err := q.backend.UpdateJobStatus(ctx, jobID, status, result, errorMsg)
	if err != nil {
		q.logger.Debug("UpdateJobStatus: backend.UpdateJobStatus error", "jobID", jobID, "error", err)
		return err
	}
	q.logger.Debug("UpdateJobStatus: backend.UpdateJobStatus returned freedAssigneeIDs", "freedAssigneeIDs", freedAssigneeIDs, "jobID", jobID)

	// Free capacity for affected subscriptions and notify
	q.freeCapacityAndNotify(freedAssigneeIDs, jobTags)

	return nil
}

// Close closes the queue and all active StreamJobs calls.
func (q *PoolQueue) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	q.closed = true
	close(q.closeCh)
	// Build defensive copy of subscriptions to release lock quickly
	subs := make([]*subscription, 0, len(q.subscriptions))
	for _, sub := range q.subscriptions {
		subs = append(subs, sub)
	}
	q.mu.Unlock()

	// Close all subscription channels
	// Note: StreamJobs will close the channels when it detects queue closure
	// We don't close them here to avoid double-close issues

	// Wait for all StreamJobs to exit after seeing q.closeCh
	// This prevents backend.Close() from blocking on active database operations
	// StreamJobs should exit quickly when it sees q.closeCh in its select statement
	deadline := time.Now().Add(2 * time.Second)
	for _, sub := range subs {
		select {
		case <-sub.done:
			// StreamJobs exited
		case <-time.After(time.Until(deadline)):
			// Timeout waiting for StreamJobs to exit
			q.logger.Warn("Timeout waiting for StreamJobs to exit", "assigneeID", sub.assigneeID, "subID", sub.id)
		}
		// Don't wait longer than deadline
		if time.Now().After(deadline) {
			break
		}
	}

	// Close backend
	// Note: If there are still active operations, this may block briefly,
	// but we've waited for StreamJobs to exit, so it should be safe
	return q.backend.Close()
}
