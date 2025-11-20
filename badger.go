package jobpool

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// BadgerBackend implements the Backend interface using BadgerDB.
// It provides high-performance key-value storage and is suitable for
// high-throughput scenarios.
type BadgerBackend struct {
	db     *badger.DB
	logger *slog.Logger
}

// NewBadgerBackend creates a new BadgerDB backend.
// The database directory will be created if it doesn't exist.
// dbPath is the path to the BadgerDB database directory.
// logger is the logger instance for logging backend operations.
// Note: BadgerDB uses its own logger interface, so its internal logging is disabled.
func NewBadgerBackend(dbPath string, logger *slog.Logger) (*BadgerBackend, error) {
	opts := badger.DefaultOptions(dbPath)
	opts.Logger = nil // Disable BadgerDB's internal logging (uses different logger interface)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	backend := &BadgerBackend{
		db:     db,
		logger: logger,
	}

	return backend, nil
}

// Close closes the database connection
func (b *BadgerBackend) Close() error {
	return b.db.Close()
}

// retryUpdate retries a BadgerDB update operation on transaction conflicts.
// This provides deterministic retry behavior suitable for tests (fixed delay, no jitter).
func (b *BadgerBackend) retryUpdate(ctx context.Context, fn func(txn *badger.Txn) error) error {
	const maxRetries = 50                   // Increased for high concurrency scenarios
	const retryDelay = 1 * time.Millisecond // Fixed delay for deterministic tests

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Check context before retrying
			if err := ctx.Err(); err != nil {
				return err
			}
			// Fixed delay (no jitter) for deterministic test behavior
			time.Sleep(retryDelay)
		}

		err := b.db.Update(fn)
		if err == nil {
			return nil
		}

		// Check if it's a transaction conflict error
		// BadgerDB v4 returns errors with "Transaction Conflict" message
		errStr := err.Error()
		if errors.Is(err, badger.ErrConflict) || errStr == "Transaction Conflict. Please retry" {
			lastErr = err
			continue // Retry on conflict
		}

		// For other errors, return immediately
		return err
	}

	// Return last conflict error after max retries
	if lastErr != nil {
		return fmt.Errorf("transaction conflict after %d retries: %w", maxRetries, lastErr)
	}
	return fmt.Errorf("transaction conflict after %d retries", maxRetries)
}

// key prefixes
const (
	keyPrefixJob     = "job:"
	keyPrefixTag     = "tag:"
	keyPrefixIndex   = "idx:"
	keyPrefixPending = "idx:pending:"
	keyPrefixRunning = "idx:running:"
)

// jobKey returns the key for a job
func jobKey(jobID string) []byte {
	return []byte(keyPrefixJob + jobID)
}

// tagKey returns the key for a job tag index
func tagKey(tag, jobID string) []byte {
	return []byte(fmt.Sprintf("%s%s:%s", keyPrefixTag, tag, jobID))
}

// pendingIndexKey returns the key for pending job index
func pendingIndexKey(jobID string, timestamp int64) []byte {
	key := make([]byte, 0, len(keyPrefixPending)+len(jobID)+8)
	key = append(key, []byte(keyPrefixPending)...)
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(timestamp))
	key = append(key, tsBytes...)
	key = append(key, []byte(jobID)...)
	return key
}

// runningIndexKey returns the key for running job index by assignee
func runningIndexKey(assigneeID, jobID string) []byte {
	return []byte(fmt.Sprintf("%s%s:%s", keyPrefixRunning, assigneeID, jobID))
}

// EnqueueJob enqueues a single job
func (b *BadgerBackend) EnqueueJob(ctx context.Context, job *Job) (string, error) {
	jobIDs, err := b.EnqueueJobs(ctx, []*Job{job})
	if err != nil {
		return "", err
	}
	if len(jobIDs) == 0 {
		return "", fmt.Errorf("no job ID returned")
	}
	return jobIDs[0], nil
}

// EnqueueJobs enqueues multiple jobs in a batch
func (b *BadgerBackend) EnqueueJobs(ctx context.Context, jobs []*Job) ([]string, error) {
	var err error
	if ctx, err = normalizeContext(ctx); err != nil {
		return nil, err
	}
	if len(jobs) == 0 {
		return []string{}, nil
	}

	seen := make(map[string]struct{}, len(jobs))
	now := time.Now()
	for idx, job := range jobs {
		if job == nil {
			return nil, fmt.Errorf("job at index %d is nil", idx)
		}
		if job.ID == "" {
			return nil, fmt.Errorf("job at index %d is missing ID", idx)
		}
		if job.Status != JobStatusInitialPending {
			return nil, fmt.Errorf("job %s must have status %s", job.ID, JobStatusInitialPending)
		}
		if _, exists := seen[job.ID]; exists {
			return nil, fmt.Errorf("duplicate job ID %s in batch", job.ID)
		}
		seen[job.ID] = struct{}{}
		if job.CreatedAt.IsZero() {
			job.CreatedAt = now
		}
		job.AssigneeID = ""
		job.AssignedAt = nil
		job.StartedAt = nil
		job.FinalizedAt = nil
		job.ErrorMessage = ""
		job.Result = nil
		job.RetryCount = 0
		job.LastRetryAt = nil

		b.logger.Debug("EnqueueJob: validated job for enqueue", "jobID", job.ID, "tags", job.Tags, "status", job.Status)
	}

	resultIDs := make([]string, 0, len(jobs))

	const maxAttempts = 5
	for attempt := 0; attempt < maxAttempts; attempt++ {
		tempIDs := make([]string, 0, len(jobs))
		err = b.db.Update(func(txn *badger.Txn) error {
			if err := ctx.Err(); err != nil {
				return err
			}

			for _, job := range jobs {
				if err := ctx.Err(); err != nil {
					return err
				}

				if _, err := txn.Get(jobKey(job.ID)); err == nil {
					return fmt.Errorf("job already exists: %s", job.ID)
				} else if err != badger.ErrKeyNotFound {
					return fmt.Errorf("failed to check existing job: %w", err)
				}

				jobData, err := json.Marshal(job)
				if err != nil {
					return fmt.Errorf("failed to marshal job: %w", err)
				}

				if err := txn.Set(jobKey(job.ID), jobData); err != nil {
					return fmt.Errorf("failed to store job: %w", err)
				}

				// Index by status (pending)
				timestamp := job.CreatedAt.Unix()
				if job.LastRetryAt != nil {
					timestamp = job.LastRetryAt.Unix()
				}
				if err := txn.Set(pendingIndexKey(job.ID, timestamp), []byte(job.ID)); err != nil {
					return fmt.Errorf("failed to index pending job: %w", err)
				}

				// Index by tags
				for _, tag := range job.Tags {
					if err := txn.Set(tagKey(tag, job.ID), []byte(job.ID)); err != nil {
						return fmt.Errorf("failed to index tag: %w", err)
					}
				}

				tempIDs = append(tempIDs, job.ID)
			}
			return nil
		})

		if err == nil {
			resultIDs = append(resultIDs[:0], tempIDs...)
			break
		}
		if errors.Is(err, badger.ErrConflict) {
			time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
			continue
		}
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	return resultIDs, nil
}

// DequeueJobs dequeues pending jobs up to the limit and assigns them to the given assigneeID
// tags: Filter jobs by tags using AND logic (jobs must have ALL provided tags).
//
//	Empty slice or nil means "accept all jobs" (no filtering).
func (b *BadgerBackend) DequeueJobs(ctx context.Context, assigneeID string, tags []string, limit int) ([]*Job, error) {
	var err error
	if ctx, err = normalizeContext(ctx); err != nil {
		return nil, err
	}
	if assigneeID == "" {
		return nil, fmt.Errorf("assigneeID is required")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be greater than 0")
	}

	b.logger.Debug("DequeueJobs: starting", "assigneeID", assigneeID, "tags", tags, "limit", limit)

	// Helper function to check if job matches worker tags
	// Returns true if all worker tags are present in job tags (subset check)
	// Empty worker tags means accept all jobs
	matchesTags := func(jobTags []string, workerTags []string) bool {
		if len(workerTags) == 0 {
			return true // Empty tags means accept all
		}

		// Create a set of job tags for efficient lookup
		jobTagSet := make(map[string]bool, len(jobTags))
		for _, tag := range jobTags {
			jobTagSet[tag] = true
		}

		// Check if all worker tags are present in job tags
		for _, workerTag := range workerTags {
			if !jobTagSet[workerTag] {
				return false // Worker tag not found in job tags
			}
		}

		return true // All worker tags found in job tags
	}

	// Collect assigned jobs inside transaction to avoid duplicates on retry
	var assignedJobs []*Job
	err = b.retryUpdate(ctx, func(txn *badger.Txn) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		// Clear assigned jobs at start of each retry attempt
		assignedJobs = make([]*Job, 0, limit)

		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(keyPrefixPending)
		opts.PrefetchValues = true
		opts.Reverse = false // iterate oldest first

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek([]byte(keyPrefixPending)); it.Valid() && len(assignedJobs) < limit; it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}

			item := it.Item()
			jobIDBytes, err := item.ValueCopy(nil)
			if err != nil {
				continue
			}
			jobID := string(jobIDBytes)

			jobItem, err := txn.Get(jobKey(jobID))
			if err != nil {
				continue
			}

			jobData, err := jobItem.ValueCopy(nil)
			if err != nil {
				continue
			}

			var job Job
			if err := json.Unmarshal(jobData, &job); err != nil {
				continue
			}

			if job.Status != JobStatusInitialPending && job.Status != JobStatusFailedRetry && job.Status != JobStatusUnknownRetry {
				_ = txn.Delete(item.Key())
				continue
			}

			if !matchesTags(job.Tags, tags) {
				b.logger.Debug("DequeueJobs: job tags don't match worker tags", "jobID", job.ID, "jobTags", job.Tags, "workerTags", tags)
				continue
			}

			b.logger.Debug("DequeueJobs: job matches tags, assigning", "jobID", job.ID, "jobTags", job.Tags, "workerTags", tags)

			assignTime := time.Now()
			job.AssigneeID = assigneeID
			job.AssignedAt = &assignTime
			if job.StartedAt == nil {
				startedAt := assignTime
				job.StartedAt = &startedAt
			}
			job.Status = JobStatusRunning

			updatedJobData, err := json.Marshal(&job)
			if err != nil {
				return fmt.Errorf("failed to marshal updated job: %w", err)
			}

			if err := txn.Set(jobKey(job.ID), updatedJobData); err != nil {
				return fmt.Errorf("failed to update job: %w", err)
			}

			if err := txn.Delete(item.Key()); err != nil {
				return fmt.Errorf("failed to remove from pending index: %w", err)
			}

			if err := txn.Set(runningIndexKey(assigneeID, job.ID), []byte(job.ID)); err != nil {
				return fmt.Errorf("failed to add to running index: %w", err)
			}

			// Create a copy of the job to return (job is already updated with RUNNING status, assigneeID, tags, etc.)
			jobCopy := job
			// Deep copy Tags slice and other reference types to avoid sharing
			if len(job.Tags) > 0 {
				jobCopy.Tags = make([]string, len(job.Tags))
				copy(jobCopy.Tags, job.Tags)
			}
			// Deep copy JobDefinition if present
			if len(job.JobDefinition) > 0 {
				jobCopy.JobDefinition = make([]byte, len(job.JobDefinition))
				copy(jobCopy.JobDefinition, job.JobDefinition)
			}
			// Deep copy Result if present
			if len(job.Result) > 0 {
				jobCopy.Result = make([]byte, len(job.Result))
				copy(jobCopy.Result, job.Result)
			}
			assignedJobs = append(assignedJobs, &jobCopy)
		}

		return nil
	})
	if err != nil {
		b.logger.Debug("DequeueJobs: error", "assigneeID", assigneeID, "tags", tags, "error", err)
		return nil, err
	}

	b.logger.Debug("DequeueJobs: completed", "assigneeID", assigneeID, "tags", tags, "assignedCount", len(assignedJobs))
	if len(assignedJobs) > 0 {
		jobInfo := make([]map[string]interface{}, 0, len(assignedJobs))
		for _, job := range assignedJobs {
			jobInfo = append(jobInfo, map[string]interface{}{
				"jobID":   job.ID,
				"jobTags": job.Tags,
				"status":  job.Status,
			})
		}
		b.logger.Debug("DequeueJobs: assigned jobs", "assigneeID", assigneeID, "jobs", jobInfo)
	} else {
		b.logger.Debug("DequeueJobs: no jobs assigned", "assigneeID", assigneeID, "tags", tags, "limit", limit)
	}

	return assignedJobs, nil
}

// CompleteJob atomically transitions a job to COMPLETED with the given result.
// Supports RUNNING, CANCELLING, UNKNOWN_RETRY, and UNKNOWN_STOPPED → COMPLETED transitions.
func (b *BadgerBackend) CompleteJob(ctx context.Context, jobID string, result []byte) (map[string]int, error) {
	var err error
	if ctx, err = normalizeContext(ctx); err != nil {
		return nil, err
	}
	now := time.Now()
	freedAssigneeIDs := make(map[string]int)

	err = b.db.Update(func(txn *badger.Txn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Get job
		item, err := txn.Get(jobKey(jobID))
		if err != nil {
			return fmt.Errorf("job not found: %w", err)
		}

		jobData, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("failed to get job data: %w", err)
		}

		var job Job
		if err := json.Unmarshal(jobData, &job); err != nil {
			return fmt.Errorf("failed to unmarshal job: %w", err)
		}

		// Validate job is in a state that can transition to COMPLETED
		validStates := map[JobStatus]bool{
			JobStatusRunning:        true,
			JobStatusCancelling:     true,
			JobStatusUnknownRetry:   true,
			JobStatusUnknownStopped: true,
		}
		if !validStates[job.Status] {
			return fmt.Errorf("job %s is not in a valid state for completion (current: %s)", jobID, job.Status)
		}

		oldAssigneeID := job.AssigneeID
		shouldFreeAssignee := job.Status == JobStatusRunning || job.Status == JobStatusCancelling

		// Update job to COMPLETED
		job.Status = JobStatusCompleted
		job.Result = result
		completedAt := now
		job.FinalizedAt = &completedAt
		if job.StartedAt == nil {
			startedAt := now
			job.StartedAt = &startedAt
		}
		// Preserve assignee_id for historical tracking (per spec)

		// Remove from running index if it was assigned
		if oldAssigneeID != "" {
			_ = txn.Delete(runningIndexKey(oldAssigneeID, jobID))
		}

		// Save updated job
		updatedJobData, err := json.Marshal(&job)
		if err != nil {
			return fmt.Errorf("failed to marshal updated job: %w", err)
		}

		if err := txn.Set(jobKey(jobID), updatedJobData); err != nil {
			return err
		}

		// Capture freed assignee ID (with count/multiplicity)
		if shouldFreeAssignee && oldAssigneeID != "" {
			freedAssigneeIDs[oldAssigneeID] = 1
		}

		return nil
	})

	return freedAssigneeIDs, err
}

// FailJob atomically transitions a job to FAILED_RETRY, incrementing the retry count.
// Supports RUNNING and UNKNOWN_RETRY → FAILED_RETRY transitions.
// Job remains in FAILED_RETRY state (eligible for scheduling, but never returns to INITIAL_PENDING).
func (b *BadgerBackend) FailJob(ctx context.Context, jobID string, errorMsg string) (map[string]int, error) {
	var err error
	if ctx, err = normalizeContext(ctx); err != nil {
		return nil, err
	}
	if errorMsg == "" {
		return nil, fmt.Errorf("error message must be provided")
	}
	now := time.Now()
	freedAssigneeIDs := make(map[string]int)

	err = b.db.Update(func(txn *badger.Txn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Get job
		item, err := txn.Get(jobKey(jobID))
		if err != nil {
			return fmt.Errorf("job not found: %w", err)
		}

		jobData, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("failed to get job data: %w", err)
		}

		var job Job
		if err := json.Unmarshal(jobData, &job); err != nil {
			return fmt.Errorf("failed to unmarshal job: %w", err)
		}

		// Validate job is in a state that can transition to FAILED_RETRY
		if job.Status != JobStatusRunning && job.Status != JobStatusUnknownRetry {
			return fmt.Errorf("job %s is not in RUNNING or UNKNOWN_RETRY state (current: %s)", jobID, job.Status)
		}

		oldAssigneeID := job.AssigneeID
		shouldFreeAssignee := job.Status == JobStatusRunning

		// Update to FAILED_RETRY with error message and increment retry count
		// Job remains in FAILED_RETRY state (eligible for scheduling, but never returns to INITIAL_PENDING)
		// Preserve assignee_id for historical tracking (per spec)
		job.Status = JobStatusFailedRetry
		job.ErrorMessage = errorMsg
		job.RetryCount++
		lastRetryAt := now
		job.LastRetryAt = &lastRetryAt

		// Remove from running index if it was assigned
		if oldAssigneeID != "" {
			_ = txn.Delete(runningIndexKey(oldAssigneeID, jobID))
		}

		// Add to pending index (FAILED_RETRY jobs are eligible for scheduling like INITIAL_PENDING)
		// Use COALESCE(last_retry_at, created_at) for ordering (failure time >= creation time)
		timestamp := job.CreatedAt.Unix()
		if job.LastRetryAt != nil {
			timestamp = job.LastRetryAt.Unix()
		}
		if err := txn.Set(pendingIndexKey(jobID, timestamp), []byte(jobID)); err != nil {
			return fmt.Errorf("failed to add to pending index: %w", err)
		}

		// Save updated job
		updatedJobData, err := json.Marshal(&job)
		if err != nil {
			return fmt.Errorf("failed to marshal updated job: %w", err)
		}

		if err := txn.Set(jobKey(jobID), updatedJobData); err != nil {
			return err
		}

		// Capture freed assignee ID (with count/multiplicity)
		if shouldFreeAssignee && oldAssigneeID != "" {
			freedAssigneeIDs[oldAssigneeID] = 1
		}

		return nil
	})

	return freedAssigneeIDs, err
}

// StopJob atomically transitions a job to STOPPED with an error message.
// Supports RUNNING, CANCELLING, and UNKNOWN_RETRY → STOPPED transitions.
func (b *BadgerBackend) StopJob(ctx context.Context, jobID string, errorMsg string) (map[string]int, error) {
	var err error
	if ctx, err = normalizeContext(ctx); err != nil {
		return nil, err
	}
	now := time.Now()
	freedAssigneeIDs := make(map[string]int)

	err = b.db.Update(func(txn *badger.Txn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Get job
		item, err := txn.Get(jobKey(jobID))
		if err != nil {
			return fmt.Errorf("job not found: %w", err)
		}

		jobData, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("failed to get job data: %w", err)
		}

		var job Job
		if err := json.Unmarshal(jobData, &job); err != nil {
			return fmt.Errorf("failed to unmarshal job: %w", err)
		}

		// Validate job is in a state that can transition to STOPPED
		validStates := map[JobStatus]bool{
			JobStatusRunning:      true,
			JobStatusCancelling:   true,
			JobStatusUnknownRetry: true,
		}
		if !validStates[job.Status] {
			return fmt.Errorf("job %s is not in a valid state for stopping (current: %s)", jobID, job.Status)
		}

		oldAssigneeID := job.AssigneeID
		shouldFreeAssignee := job.Status == JobStatusRunning || job.Status == JobStatusCancelling

		// Update job to STOPPED
		// Preserve assignee_id for historical tracking (per spec)
		job.Status = JobStatusStopped
		job.ErrorMessage = errorMsg
		if job.FinalizedAt == nil {
			completedAt := now
			job.FinalizedAt = &completedAt
		}

		// Remove from running index if it was assigned
		if oldAssigneeID != "" {
			_ = txn.Delete(runningIndexKey(oldAssigneeID, jobID))
		}

		// Save updated job
		updatedJobData, err := json.Marshal(&job)
		if err != nil {
			return fmt.Errorf("failed to marshal updated job: %w", err)
		}

		if err := txn.Set(jobKey(jobID), updatedJobData); err != nil {
			return err
		}

		// Capture freed assignee ID (with count/multiplicity)
		if shouldFreeAssignee && oldAssigneeID != "" {
			freedAssigneeIDs[oldAssigneeID] = 1
		}

		return nil
	})

	return freedAssigneeIDs, err
}

// StopJobWithRetry atomically transitions a job from CANCELLING to STOPPED with retry increment.
// Applies all effects from the transitory FAILED_RETRY state (retry increment + error message).
func (b *BadgerBackend) StopJobWithRetry(ctx context.Context, jobID string, errorMsg string) (map[string]int, error) {
	var err error
	if ctx, err = normalizeContext(ctx); err != nil {
		return nil, err
	}
	now := time.Now()
	freedAssigneeIDs := make(map[string]int)

	err = b.db.Update(func(txn *badger.Txn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Get job
		item, err := txn.Get(jobKey(jobID))
		if err != nil {
			return fmt.Errorf("job not found: %w", err)
		}

		jobData, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("failed to get job data: %w", err)
		}

		var job Job
		if err := json.Unmarshal(jobData, &job); err != nil {
			return fmt.Errorf("failed to unmarshal job: %w", err)
		}

		if job.Status != JobStatusCancelling {
			return fmt.Errorf("job %s is not in CANCELLING state (current: %s)", jobID, job.Status)
		}

		oldAssigneeID := job.AssigneeID
		shouldFreeAssignee := job.Status == JobStatusCancelling

		// Update job to STOPPED with retry increment
		// Preserve assignee_id for historical tracking (per spec)
		job.Status = JobStatusStopped
		job.ErrorMessage = errorMsg
		job.RetryCount++
		lastRetryAt := now
		job.LastRetryAt = &lastRetryAt
		if job.FinalizedAt == nil {
			completedAt := now
			job.FinalizedAt = &completedAt
		}

		// Remove from running index if it was assigned
		if oldAssigneeID != "" {
			_ = txn.Delete(runningIndexKey(oldAssigneeID, jobID))
		}

		// Save updated job
		updatedJobData, err := json.Marshal(&job)
		if err != nil {
			return fmt.Errorf("failed to marshal updated job: %w", err)
		}

		if err := txn.Set(jobKey(jobID), updatedJobData); err != nil {
			return err
		}

		// Capture freed assignee ID (with count/multiplicity)
		if shouldFreeAssignee && oldAssigneeID != "" {
			freedAssigneeIDs[oldAssigneeID] = 1
		}

		return nil
	})

	return freedAssigneeIDs, err
}

// MarkJobUnknownStopped atomically transitions a job to UNKNOWN_STOPPED with an error message.
// Supports CANCELLING, UNKNOWN_RETRY, and RUNNING → UNKNOWN_STOPPED transitions.
func (b *BadgerBackend) MarkJobUnknownStopped(ctx context.Context, jobID string, errorMsg string) (map[string]int, error) {
	var err error
	if ctx, err = normalizeContext(ctx); err != nil {
		return nil, err
	}
	now := time.Now()
	freedAssigneeIDs := make(map[string]int)

	err = b.db.Update(func(txn *badger.Txn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Get job
		item, err := txn.Get(jobKey(jobID))
		if err != nil {
			return fmt.Errorf("job not found: %w", err)
		}

		jobData, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("failed to get job data: %w", err)
		}

		var job Job
		if err := json.Unmarshal(jobData, &job); err != nil {
			return fmt.Errorf("failed to unmarshal job: %w", err)
		}

		oldAssigneeID := job.AssigneeID
		shouldFreeAssignee := job.Status == JobStatusRunning || job.Status == JobStatusCancelling

		// Update job to UNKNOWN_STOPPED
		job.Status = JobStatusUnknownStopped
		job.ErrorMessage = errorMsg
		if job.FinalizedAt == nil {
			completedAt := now
			job.FinalizedAt = &completedAt
		}
		job.AssigneeID = ""
		job.AssignedAt = nil

		// Remove from running index if it was assigned
		if oldAssigneeID != "" {
			_ = txn.Delete(runningIndexKey(oldAssigneeID, jobID))
		}

		// Save updated job
		updatedJobData, err := json.Marshal(&job)
		if err != nil {
			return fmt.Errorf("failed to marshal updated job: %w", err)
		}

		if err := txn.Set(jobKey(jobID), updatedJobData); err != nil {
			return err
		}

		// Capture freed assignee ID (with count/multiplicity)
		if shouldFreeAssignee && oldAssigneeID != "" {
			freedAssigneeIDs[oldAssigneeID] = 1
		}

		return nil
	})

	return freedAssigneeIDs, err
}

// UpdateJobStatus updates a job's status, result, and error message.
// This is a generic method for edge cases not covered by atomic methods.
func (b *BadgerBackend) UpdateJobStatus(ctx context.Context, jobID string, status JobStatus, result []byte, errorMsg string) (map[string]int, error) {
	var err error
	if ctx, err = normalizeContext(ctx); err != nil {
		return nil, err
	}
	now := time.Now()
	freedAssigneeIDs := make(map[string]int)

	err = b.db.Update(func(txn *badger.Txn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Get job
		item, err := txn.Get(jobKey(jobID))
		if err != nil {
			return fmt.Errorf("job not found: %w", err)
		}

		jobData, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("failed to get job data: %w", err)
		}

		var job Job
		if err := json.Unmarshal(jobData, &job); err != nil {
			return fmt.Errorf("failed to unmarshal job: %w", err)
		}

		oldAssigneeID := job.AssigneeID
		oldStatus := job.Status

		// Validate transition
		if !isValidTransition(oldStatus, status) {
			return fmt.Errorf("invalid transition from %s to %s", oldStatus, status)
		}

		// Get freed assignee ID if transitioning from RUNNING to terminal state (with count/multiplicity)
		wasRunning := oldStatus == JobStatusRunning
		isTerminal := status == JobStatusCompleted || status == JobStatusStopped || status == JobStatusUnknownStopped
		if wasRunning && isTerminal && oldAssigneeID != "" {
			freedAssigneeIDs[oldAssigneeID] = 1
		}

		// Update job status
		job.Status = status
		if result != nil {
			job.Result = result
		}
		if errorMsg != "" {
			job.ErrorMessage = errorMsg
		}

		// Update timestamps based on status
		if status == JobStatusRunning && job.StartedAt == nil {
			startedAt := now
			job.StartedAt = &startedAt
		}
		if (status == JobStatusCompleted || status == JobStatusStopped || status == JobStatusUnknownStopped) && job.FinalizedAt == nil {
			completedAt := now
			job.FinalizedAt = &completedAt
		}

		// Preserve assignee_id and assigned_at for historical tracking (per spec)
		// Do not clear assignee_id - it should be preserved even in terminal states

		// Update indexes
		// Remove from running index if it was assigned and is no longer running
		if oldAssigneeID != "" && (oldStatus == JobStatusRunning || oldStatus == JobStatusCancelling) {
			if status != JobStatusRunning && status != JobStatusCancelling {
				_ = txn.Delete(runningIndexKey(oldAssigneeID, jobID))
			}
		}

		// Add to pending index if job becomes INITIAL_PENDING
		if status == JobStatusInitialPending {
			// Use COALESCE(last_retry_at, created_at) for ordering (failure time >= creation time)
			timestamp := job.CreatedAt.Unix()
			if job.LastRetryAt != nil {
				timestamp = job.LastRetryAt.Unix()
			}
			if err := txn.Set(pendingIndexKey(jobID, timestamp), []byte(jobID)); err != nil {
				return fmt.Errorf("failed to add to pending index: %w", err)
			}
		}

		// Save updated job
		updatedJobData, err := json.Marshal(&job)
		if err != nil {
			return fmt.Errorf("failed to marshal updated job: %w", err)
		}

		return txn.Set(jobKey(jobID), updatedJobData)
	})

	return freedAssigneeIDs, err
}

// GetJobStats gets statistics for jobs matching ALL provided tags (AND logic)
func (b *BadgerBackend) GetJobStats(ctx context.Context, tags []string) (*JobStats, error) {
	var err error
	if ctx, err = normalizeContext(ctx); err != nil {
		return nil, err
	}

	stats := &JobStats{Tags: tags}

	updateStats := func(job *Job) {
		stats.TotalJobs++
		stats.TotalRetries += int32(job.RetryCount)
		switch job.Status {
		case JobStatusInitialPending:
			stats.PendingJobs++
		case JobStatusRunning:
			stats.RunningJobs++
		case JobStatusCompleted:
			stats.CompletedJobs++
		case JobStatusFailedRetry, JobStatusUnknownRetry:
			stats.FailedJobs++
		case JobStatusStopped, JobStatusUnknownStopped, JobStatusUnscheduled:
			stats.StoppedJobs++
		}
	}

	if len(tags) == 0 {
		err = b.db.View(func(txn *badger.Txn) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			opts := badger.DefaultIteratorOptions
			opts.Prefix = []byte(keyPrefixJob)
			opts.PrefetchValues = true

			it := txn.NewIterator(opts)
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				if err := ctx.Err(); err != nil {
					return err
				}
				item := it.Item()
				jobData, err := item.ValueCopy(nil)
				if err != nil {
					continue
				}

				var job Job
				if err := json.Unmarshal(jobData, &job); err != nil {
					continue
				}

				updateStats(&job)
			}

			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("failed to get job stats: %w", err)
		}
		return stats, nil
	}

	// Find jobs that have all tags
	jobIDSet := make(map[string]int)

	err = b.db.View(func(txn *badger.Txn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, tag := range tags {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = []byte(keyPrefixTag + tag + ":")
			opts.PrefetchValues = false

			it := txn.NewIterator(opts)
			for it.Rewind(); it.Valid(); it.Next() {
				if err := ctx.Err(); err != nil {
					it.Close()
					return err
				}
				key := it.Item().Key()
				jobID := string(key[len(keyPrefixTag)+len(tag)+1:])
				jobIDSet[jobID]++
			}
			it.Close()
		}

		for jobID, count := range jobIDSet {
			if count == len(tags) {
				if err := ctx.Err(); err != nil {
					return err
				}
				item, err := txn.Get(jobKey(jobID))
				if err != nil {
					continue
				}

				jobData, err := item.ValueCopy(nil)
				if err != nil {
					continue
				}

				var job Job
				if err := json.Unmarshal(jobData, &job); err != nil {
					continue
				}

				updateStats(&job)
			}
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get job stats: %w", err)
	}

	return stats, nil
}

// ResetRunningJobs marks running jobs as UNKNOWN_RETRY and cancelling jobs as UNKNOWN_STOPPED.
func (b *BadgerBackend) ResetRunningJobs(ctx context.Context) error {
	var err error
	if ctx, err = normalizeContext(ctx); err != nil {
		return err
	}

	runningIDs := make([]string, 0)
	cancellingIDs := make([]string, 0)

	err = b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(keyPrefixRunning)
		opts.PrefetchValues = true

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			jobIDBytes, err := item.ValueCopy(nil)
			if err != nil {
				continue
			}
			jobID := string(jobIDBytes)

			jobItem, err := txn.Get(jobKey(jobID))
			if err != nil {
				continue
			}

			jobData, err := jobItem.ValueCopy(nil)
			if err != nil {
				continue
			}

			var job Job
			if err := json.Unmarshal(jobData, &job); err != nil {
				continue
			}

			switch job.Status {
			case JobStatusRunning:
				runningIDs = append(runningIDs, jobID)
			case JobStatusCancelling:
				cancellingIDs = append(cancellingIDs, jobID)
			default:
				_ = txn.Delete(item.Key())
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	b.logger.Debug("ResetRunningJobs: found jobs to reset", "runningCount", len(runningIDs), "cancellingCount", len(cancellingIDs))

	return b.db.Update(func(txn *badger.Txn) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(keyPrefixRunning)
		opts.PrefetchValues = true

		it := txn.NewIterator(opts)
		defer it.Close()

		resetCount := 0
		cancellingCount := 0

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			jobIDBytes, err := item.ValueCopy(nil)
			if err != nil {
				continue
			}
			jobID := string(jobIDBytes)

			jobItem, err := txn.Get(jobKey(jobID))
			if err != nil {
				_ = txn.Delete(item.Key())
				continue
			}

			jobData, err := jobItem.ValueCopy(nil)
			if err != nil {
				_ = txn.Delete(item.Key())
				continue
			}

			var job Job
			if err := json.Unmarshal(jobData, &job); err != nil {
				_ = txn.Delete(item.Key())
				continue
			}

			_ = txn.Delete(item.Key())

			switch job.Status {
			case JobStatusRunning:
				job.Status = JobStatusUnknownRetry
				now := time.Now()
				job.LastRetryAt = &now
				resetCount++

				if err := txn.Set(pendingIndexKey(jobID, now.Unix()), []byte(jobID)); err != nil {
					return fmt.Errorf("failed to add reset job to pending index: %w", err)
				}
			case JobStatusCancelling:
				job.Status = JobStatusUnknownStopped
				now := time.Now()
				if job.FinalizedAt == nil {
					job.FinalizedAt = &now
				}
				cancellingCount++
			default:
				continue
			}

			updatedJobData, err := json.Marshal(&job)
			if err != nil {
				return fmt.Errorf("failed to marshal updated job: %w", err)
			}

			if err := txn.Set(jobKey(jobID), updatedJobData); err != nil {
				return fmt.Errorf("failed to update job: %w", err)
			}
		}

		b.logger.Debug("ResetRunningJobs: reset jobs", "runningCount", resetCount, "cancellingCount", cancellingCount)

		return nil
	})
}

// CleanupExpiredJobs deletes completed jobs older than TTL
func (b *BadgerBackend) CleanupExpiredJobs(ctx context.Context, ttl time.Duration) error {
	var err error
	if ctx, err = normalizeContext(ctx); err != nil {
		return err
	}
	if ttl <= 0 {
		return fmt.Errorf("ttl must be greater than 0")
	}
	cutoff := time.Now().Add(-ttl)

	return b.db.Update(func(txn *badger.Txn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(keyPrefixJob)
		opts.PrefetchValues = true

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			item := it.Item()
			jobData, err := item.ValueCopy(nil)
			if err != nil {
				continue
			}

			var job Job
			if err := json.Unmarshal(jobData, &job); err != nil {
				continue
			}

			// Check if completed and expired
			if job.Status == JobStatusCompleted && job.FinalizedAt != nil && job.FinalizedAt.Before(cutoff) {
				jobID := job.ID

				// Delete job
				if err := txn.Delete(item.Key()); err != nil {
					return fmt.Errorf("failed to delete job: %w", err)
				}

				// Delete tag indexes
				for _, tag := range job.Tags {
					_ = txn.Delete(tagKey(tag, jobID))
				}
			}
		}

		return nil
	})
}

// DeleteJobs forcefully deletes jobs by tags and/or job IDs
// This method validates that all jobs are in final states (COMPLETED, UNSCHEDULED, STOPPED, UNKNOWN_STOPPED)
// before deletion. If any job is not in a final state, an error is returned.
func (b *BadgerBackend) DeleteJobs(ctx context.Context, tags []string, jobIDs []string) error {
	var err error
	if ctx, err = normalizeContext(ctx); err != nil {
		return err
	}
	if len(tags) == 0 && len(jobIDs) == 0 {
		return fmt.Errorf("either tags or jobIDs must be provided")
	}
	jobIDSet := make(map[string]bool)

	// Collect job IDs from tags (AND logic)
	if len(tags) > 0 {
		err := b.db.View(func(txn *badger.Txn) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			// Find jobs that have all tags
			tagJobSets := make([]map[string]bool, len(tags))
			for i, tag := range tags {
				tagJobSets[i] = make(map[string]bool)
				opts := badger.DefaultIteratorOptions
				opts.Prefix = []byte(keyPrefixTag + tag + ":")
				opts.PrefetchValues = false

				it := txn.NewIterator(opts)
				for it.Rewind(); it.Valid(); it.Next() {
					key := it.Item().Key()
					jobID := string(key[len(keyPrefixTag)+len(tag)+1:])
					tagJobSets[i][jobID] = true
				}
				it.Close()
			}

			// Find intersection (jobs that have all tags)
			if len(tagJobSets) > 0 {
				// Start with first tag's jobs
				for jobID := range tagJobSets[0] {
					hasAllTags := true
					for i := 1; i < len(tagJobSets); i++ {
						if !tagJobSets[i][jobID] {
							hasAllTags = false
							break
						}
					}
					if hasAllTags {
						jobIDSet[jobID] = true
					}
				}
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to query jobs by tags: %w", err)
		}
	}

	// Add job IDs from the jobIDs parameter
	for _, jobID := range jobIDs {
		jobIDSet[jobID] = true
	}

	if len(jobIDSet) == 0 {
		return nil
	}

	// Convert set to slice for validation
	allJobIDs := make([]string, 0, len(jobIDSet))
	for jobID := range jobIDSet {
		allJobIDs = append(allJobIDs, jobID)
	}

	// Validate all jobs are in final states
	nonFinalJobs := make([]string, 0)
	err = b.db.View(func(txn *badger.Txn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, jobID := range allJobIDs {
			item, err := txn.Get(jobKey(jobID))
			if err == badger.ErrKeyNotFound {
				// Job not found - skip it (not an error for cleanup)
				continue
			}
			if err != nil {
				return fmt.Errorf("failed to get job %s: %w", jobID, err)
			}

			jobData, err := item.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("failed to copy job data for %s: %w", jobID, err)
			}

			var job Job
			if err := json.Unmarshal(jobData, &job); err != nil {
				return fmt.Errorf("failed to unmarshal job %s: %w", jobID, err)
			}

			// Check if job is in a final state
			if job.Status != JobStatusCompleted &&
				job.Status != JobStatusUnscheduled &&
				job.Status != JobStatusStopped &&
				job.Status != JobStatusUnknownStopped {
				nonFinalJobs = append(nonFinalJobs, jobID)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// If any job is not in a final state, return error
	if len(nonFinalJobs) > 0 {
		return fmt.Errorf("cannot delete jobs: %d job(s) are not in final states (COMPLETED, UNSCHEDULED, STOPPED, UNKNOWN_STOPPED): %v", len(nonFinalJobs), nonFinalJobs)
	}

	// All jobs are in final states - proceed with deletion
	return b.db.Update(func(txn *badger.Txn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, jobID := range allJobIDs {
			if err := ctx.Err(); err != nil {
				return err
			}
			// Get job to retrieve tags for index cleanup
			item, err := txn.Get(jobKey(jobID))
			if err == badger.ErrKeyNotFound {
				// Job not found - skip it
				continue
			}
			if err != nil {
				return fmt.Errorf("failed to get job %s: %w", jobID, err)
			}

			jobData, err := item.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("failed to copy job data for %s: %w", jobID, err)
			}

			var job Job
			if err := json.Unmarshal(jobData, &job); err != nil {
				return fmt.Errorf("failed to unmarshal job %s: %w", jobID, err)
			}

			// Delete job
			if err := txn.Delete(jobKey(jobID)); err != nil {
				return fmt.Errorf("failed to delete job %s: %w", jobID, err)
			}

			// Delete tag indexes
			for _, tag := range job.Tags {
				_ = txn.Delete(tagKey(tag, jobID))
			}

			// Delete from pending/running indexes if applicable
			if job.Status == JobStatusInitialPending || job.Status == JobStatusFailedRetry || job.Status == JobStatusUnknownRetry {
				timestamp := job.CreatedAt.Unix()
				if job.LastRetryAt != nil {
					timestamp = job.LastRetryAt.Unix()
				}
				_ = txn.Delete(pendingIndexKey(jobID, timestamp))
			}
			if job.Status == JobStatusRunning && job.AssigneeID != "" {
				_ = txn.Delete(runningIndexKey(job.AssigneeID, jobID))
			}
		}

		return nil
	})
}

// GetJob retrieves a job by ID
func (b *BadgerBackend) GetJob(ctx context.Context, jobID string) (*Job, error) {
	var err error
	if ctx, err = normalizeContext(ctx); err != nil {
		return nil, err
	}
	var job *Job

	err = b.db.View(func(txn *badger.Txn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		item, err := txn.Get(jobKey(jobID))
		if err == badger.ErrKeyNotFound {
			return fmt.Errorf("job not found: %s", jobID)
		}
		if err != nil {
			return fmt.Errorf("failed to get job: %w", err)
		}

		jobData, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("failed to copy job data: %w", err)
		}

		var j Job
		if err := json.Unmarshal(jobData, &j); err != nil {
			return fmt.Errorf("failed to unmarshal job: %w", err)
		}

		job = &j
		return nil
	})

	if err != nil {
		return nil, err
	}

	return job, nil
}

// CancelJobs cancels jobs by tags and/or job IDs (batch cancellation)
// tags: Filter jobs by tags using AND logic (jobs must have ALL provided tags)
// jobIDs: Specific job IDs to cancel
// Both tags and jobIDs are processed (union of both sets)
// Returns: (cancelledJobIDs []string, unknownJobIDs []string, error)
// State transitions:
// - INITIAL_PENDING → UNSCHEDULED
// - RUNNING → CANCELLING
// - FAILED_RETRY → STOPPED
// - UNKNOWN_RETRY → STOPPED
func (b *BadgerBackend) CancelJobs(ctx context.Context, tags []string, jobIDs []string) ([]string, []string, error) {
	var err error
	if ctx, err = normalizeContext(ctx); err != nil {
		return nil, nil, err
	}
	if len(tags) == 0 && len(jobIDs) == 0 {
		return nil, nil, fmt.Errorf("either tags or jobIDs must be provided")
	}
	cancelledJobIDs := make([]string, 0)
	unknownJobIDs := make([]string, 0)
	jobIDSet := make(map[string]bool)

	// Collect job IDs from tags (AND logic)
	if len(tags) > 0 {
		err := b.db.View(func(txn *badger.Txn) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			// Find jobs that have all tags
			tagJobSets := make([]map[string]bool, len(tags))
			for i, tag := range tags {
				tagJobSets[i] = make(map[string]bool)
				opts := badger.DefaultIteratorOptions
				opts.Prefix = []byte(keyPrefixTag + tag + ":")
				opts.PrefetchValues = false

				it := txn.NewIterator(opts)
				for it.Rewind(); it.Valid(); it.Next() {
					key := it.Item().Key()
					jobID := string(key[len(keyPrefixTag)+len(tag)+1:])
					tagJobSets[i][jobID] = true
				}
				it.Close()
			}

			// Find intersection (jobs that have all tags)
			if len(tagJobSets) > 0 {
				// Start with first tag's jobs
				for jobID := range tagJobSets[0] {
					hasAllTags := true
					for i := 1; i < len(tagJobSets); i++ {
						if !tagJobSets[i][jobID] {
							hasAllTags = false
							break
						}
					}
					if hasAllTags {
						jobIDSet[jobID] = true
					}
				}
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return nil, nil, fmt.Errorf("failed to query jobs by tags: %w", err)
		}
	}

	// Add job IDs from the jobIDs parameter
	for _, jobID := range jobIDs {
		jobIDSet[jobID] = true
	}

	if len(jobIDSet) == 0 {
		return []string{}, []string{}, nil
	}

	// Process all job IDs
	err = b.db.Update(func(txn *badger.Txn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for jobID := range jobIDSet {
			if err := ctx.Err(); err != nil {
				return err
			}
			// Get job
			item, err := txn.Get(jobKey(jobID))
			if err == badger.ErrKeyNotFound {
				unknownJobIDs = append(unknownJobIDs, jobID)
				continue
			}
			if err != nil {
				unknownJobIDs = append(unknownJobIDs, jobID)
				continue
			}

			jobData, err := item.ValueCopy(nil)
			if err != nil {
				unknownJobIDs = append(unknownJobIDs, jobID)
				continue
			}

			var job Job
			if err := json.Unmarshal(jobData, &job); err != nil {
				unknownJobIDs = append(unknownJobIDs, jobID)
				continue
			}

			// Check if job is in a terminal state
			if job.Status == JobStatusCompleted ||
				job.Status == JobStatusStopped || job.Status == JobStatusUnscheduled ||
				job.Status == JobStatusUnknownStopped {
				unknownJobIDs = append(unknownJobIDs, jobID)
				continue
			}

			// Check if job is already in cancelling state
			if job.Status == JobStatusCancelling {
				cancelledJobIDs = append(cancelledJobIDs, jobID)
				continue
			}

			// Determine new status based on current status
			var newStatus JobStatus
			if job.Status == JobStatusInitialPending {
				newStatus = JobStatusUnscheduled
			} else if job.Status == JobStatusRunning {
				newStatus = JobStatusCancelling
			} else if job.Status == JobStatusFailedRetry {
				newStatus = JobStatusStopped
			} else if job.Status == JobStatusUnknownRetry {
				newStatus = JobStatusStopped
			} else {
				unknownJobIDs = append(unknownJobIDs, jobID)
				continue
			}

			// Update job status
			oldStatus := job.Status
			job.Status = newStatus
			now := time.Now()
			if newStatus == JobStatusStopped && (oldStatus == JobStatusFailedRetry || oldStatus == JobStatusUnknownRetry) {
				job.FinalizedAt = &now
			}

			// Update indexes
			if newStatus == JobStatusStopped && job.AssigneeID != "" {
				_ = txn.Delete(runningIndexKey(job.AssigneeID, jobID))
			}
			if newStatus == JobStatusUnscheduled {
				timestamp := job.CreatedAt.Unix()
				if job.LastRetryAt != nil {
					timestamp = job.LastRetryAt.Unix()
				}
				_ = txn.Delete(pendingIndexKey(jobID, timestamp))
			}

			// Save updated job
			updatedJobData, err := json.Marshal(&job)
			if err != nil {
				unknownJobIDs = append(unknownJobIDs, jobID)
				continue
			}

			if err := txn.Set(jobKey(jobID), updatedJobData); err != nil {
				unknownJobIDs = append(unknownJobIDs, jobID)
				continue
			}

			cancelledJobIDs = append(cancelledJobIDs, jobID)
		}
		return nil
	})

	if err != nil {
		return nil, nil, fmt.Errorf("failed to cancel jobs: %w", err)
	}

	return cancelledJobIDs, unknownJobIDs, nil
}

// MarkWorkerUnresponsive marks all jobs assigned to the given assigneeID as unresponsive
// RUNNING → UNKNOWN_RETRY, CANCELLING → UNKNOWN_STOPPED
func (b *BadgerBackend) MarkWorkerUnresponsive(ctx context.Context, assigneeID string) error {
	var err error
	if ctx, err = normalizeContext(ctx); err != nil {
		return err
	}
	if assigneeID == "" {
		return fmt.Errorf("assigneeID is required")
	}
	now := time.Now()
	return b.db.Update(func(txn *badger.Txn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(keyPrefixRunning + assigneeID + ":")
		opts.PrefetchValues = true

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			item := it.Item()
			jobIDBytes, err := item.ValueCopy(nil)
			if err != nil {
				continue
			}
			jobID := string(jobIDBytes)

			// Get job
			jobItem, err := txn.Get(jobKey(jobID))
			if err != nil {
				continue
			}

			jobData, err := jobItem.ValueCopy(nil)
			if err != nil {
				continue
			}

			var job Job
			if err := json.Unmarshal(jobData, &job); err != nil {
				continue
			}

			if job.AssigneeID != assigneeID {
				continue
			}

			// Remove from running index
			_ = txn.Delete(item.Key())

			// Update based on status
			if job.Status == JobStatusRunning {
				// RUNNING → UNKNOWN_RETRY
				job.Status = JobStatusUnknownRetry
				job.LastRetryAt = &now

				// Add to pending index so it can be dequeued again
				if err := txn.Set(pendingIndexKey(jobID, now.Unix()), []byte(jobID)); err != nil {
					return fmt.Errorf("failed to add to pending index: %w", err)
				}
			} else if job.Status == JobStatusCancelling {
				// CANCELLING → UNKNOWN_STOPPED
				job.Status = JobStatusUnknownStopped
				if job.FinalizedAt == nil {
					job.FinalizedAt = &now
				}
			} else {
				// Skip other statuses
				continue
			}

			// Save updated job
			updatedJobData, err := json.Marshal(&job)
			if err != nil {
				return fmt.Errorf("failed to marshal updated job: %w", err)
			}

			if err := txn.Set(jobKey(jobID), updatedJobData); err != nil {
				return fmt.Errorf("failed to update job: %w", err)
			}
		}

		return nil
	})
}

// AcknowledgeCancellation handles cancellation acknowledgment from worker
// wasExecuting: true if job was executing when cancellation was processed, false if job was unknown/finished
// CANCELLING → STOPPED (wasExecuting=true) or UNKNOWN_STOPPED (wasExecuting=false)
func (b *BadgerBackend) AcknowledgeCancellation(ctx context.Context, jobID string, wasExecuting bool) error {
	var err error
	if ctx, err = normalizeContext(ctx); err != nil {
		return err
	}
	now := time.Now()
	return b.db.Update(func(txn *badger.Txn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Get job
		item, err := txn.Get(jobKey(jobID))
		if err == badger.ErrKeyNotFound {
			return fmt.Errorf("job not found: %s", jobID)
		}
		if err != nil {
			return fmt.Errorf("failed to get job: %w", err)
		}

		jobData, err := item.ValueCopy(nil)
		if err != nil {
			return fmt.Errorf("failed to copy job data: %w", err)
		}

		var job Job
		if err := json.Unmarshal(jobData, &job); err != nil {
			return fmt.Errorf("failed to unmarshal job: %w", err)
		}

		if job.Status != JobStatusCancelling {
			return fmt.Errorf("job %s is not in CANCELLING state (current: %s)", jobID, job.Status)
		}

		oldAssigneeID := job.AssigneeID

		// Determine new status
		var newStatus JobStatus
		if wasExecuting {
			newStatus = JobStatusStopped
		} else {
			newStatus = JobStatusUnknownStopped
		}

		// Update job
		job.Status = newStatus
		if job.FinalizedAt == nil {
			job.FinalizedAt = &now
		}

		// Remove from running index
		if oldAssigneeID != "" {
			_ = txn.Delete(runningIndexKey(oldAssigneeID, jobID))
		}

		// Save updated job
		updatedJobData, err := json.Marshal(&job)
		if err != nil {
			return fmt.Errorf("failed to marshal updated job: %w", err)
		}

		return txn.Set(jobKey(jobID), updatedJobData)
	})
}
