package jobpool

import (
	"context"
	"encoding/binary"
	"encoding/json"
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
	if len(jobs) == 0 {
		return []string{}, nil
	}

	jobIDs := make([]string, 0, len(jobs))

	err := b.db.Update(func(txn *badger.Txn) error {
		for _, job := range jobs {
			// Serialize job
			jobData, err := json.Marshal(job)
			if err != nil {
				return fmt.Errorf("failed to marshal job: %w", err)
			}

			// Store job
			if err := txn.Set(jobKey(job.ID), jobData); err != nil {
				return fmt.Errorf("failed to store job: %w", err)
			}

			// Index by status (pending)
			if job.Status == JobStatusInitialPending {
				// Use COALESCE(last_retry_at, created_at) for ordering (failure time >= creation time)
				timestamp := job.CreatedAt.Unix()
				if job.LastRetryAt != nil {
					timestamp = job.LastRetryAt.Unix()
				}
				if err := txn.Set(pendingIndexKey(job.ID, timestamp), []byte(job.ID)); err != nil {
					return fmt.Errorf("failed to index pending job: %w", err)
				}
			}

			// Index by tags
			for _, tag := range job.Tags {
				if err := txn.Set(tagKey(tag, job.ID), []byte(job.ID)); err != nil {
					return fmt.Errorf("failed to index tag: %w", err)
				}
			}

			jobIDs = append(jobIDs, job.ID)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return jobIDs, nil
}

// DequeueJobs dequeues pending jobs up to the limit and assigns them to the given assigneeID
// tags: Filter jobs by tags using AND logic (jobs must have ALL provided tags).
//
//	Empty slice or nil means "accept all jobs" (no filtering).
func (b *BadgerBackend) DequeueJobs(ctx context.Context, assigneeID string, tags []string, limit int) ([]*Job, error) {
	jobs := make([]*Job, 0, limit)
	now := time.Now()

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

	err := b.db.Update(func(txn *badger.Txn) error {
		// Iterate over pending jobs in ascending order (oldest first)
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(keyPrefixPending)
		opts.PrefetchValues = true
		opts.Reverse = false // Iterate forward (oldest first)

		it := txn.NewIterator(opts)
		defer it.Close()

		jobIDs := make([]string, 0, limit)

		// Start from the beginning (oldest jobs)
		for it.Seek([]byte(keyPrefixPending)); it.Valid() && len(jobIDs) < limit; it.Next() {
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

			// Process if pending, failed, or unknown_retry (all eligible for scheduling)
			if job.Status != JobStatusInitialPending && job.Status != JobStatusFailedRetry && job.Status != JobStatusUnknownRetry {
				// Clean up stale index entry
				_ = txn.Delete(item.Key())
				continue
			}

			// Check if job matches worker tags (subset check: all worker tags must be in job tags)
			if !matchesTags(job.Tags, tags) {
				continue // Job doesn't match worker tags, skip it
			}

			// Assign job
			job.AssigneeID = assigneeID
			job.AssignedAt = &now
			job.Status = JobStatusRunning

			// Update job
			updatedJobData, err := json.Marshal(&job)
			if err != nil {
				return fmt.Errorf("failed to marshal updated job: %w", err)
			}

			if err := txn.Set(jobKey(job.ID), updatedJobData); err != nil {
				return fmt.Errorf("failed to update job: %w", err)
			}

			// Remove from pending index
			if err := txn.Delete(item.Key()); err != nil {
				return fmt.Errorf("failed to remove from pending index: %w", err)
			}

			// Add to running index
			if err := txn.Set(runningIndexKey(assigneeID, job.ID), []byte(job.ID)); err != nil {
				return fmt.Errorf("failed to add to running index: %w", err)
			}

			jobs = append(jobs, &job)
			jobIDs = append(jobIDs, job.ID)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return jobs, nil
}

// CompleteJob atomically transitions a job to COMPLETED with the given result.
// Supports RUNNING, CANCELLING, UNKNOWN_RETRY, and UNKNOWN_STOPPED → COMPLETED transitions.
func (b *BadgerBackend) CompleteJob(ctx context.Context, jobID string, result []byte) (map[string]int, error) {
	now := time.Now()
	freedAssigneeIDs := make(map[string]int)

	err := b.db.Update(func(txn *badger.Txn) error {
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
		if oldAssigneeID != "" {
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
	now := time.Now()
	freedAssigneeIDs := make(map[string]int)

	err := b.db.Update(func(txn *badger.Txn) error {
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
		if oldAssigneeID != "" {
			freedAssigneeIDs[oldAssigneeID] = 1
		}

		return nil
	})

	return freedAssigneeIDs, err
}

// StopJob atomically transitions a job to STOPPED with an error message.
// Supports RUNNING, CANCELLING, and UNKNOWN_RETRY → STOPPED transitions.
func (b *BadgerBackend) StopJob(ctx context.Context, jobID string, errorMsg string) (map[string]int, error) {
	now := time.Now()
	freedAssigneeIDs := make(map[string]int)

	err := b.db.Update(func(txn *badger.Txn) error {
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
		if oldAssigneeID != "" {
			freedAssigneeIDs[oldAssigneeID] = 1
		}

		return nil
	})

	return freedAssigneeIDs, err
}

// StopJobWithRetry atomically transitions a job from CANCELLING to STOPPED with retry increment.
// Applies all effects from the transitory FAILED_RETRY state (retry increment + error message).
func (b *BadgerBackend) StopJobWithRetry(ctx context.Context, jobID string, errorMsg string) (map[string]int, error) {
	now := time.Now()
	freedAssigneeIDs := make(map[string]int)

	err := b.db.Update(func(txn *badger.Txn) error {
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
		if oldAssigneeID != "" {
			freedAssigneeIDs[oldAssigneeID] = 1
		}

		return nil
	})

	return freedAssigneeIDs, err
}

// MarkJobUnknownStopped atomically transitions a job to UNKNOWN_STOPPED with an error message.
// Supports CANCELLING, UNKNOWN_RETRY, and RUNNING → UNKNOWN_STOPPED transitions.
func (b *BadgerBackend) MarkJobUnknownStopped(ctx context.Context, jobID string, errorMsg string) (map[string]int, error) {
	now := time.Now()
	freedAssigneeIDs := make(map[string]int)

	err := b.db.Update(func(txn *badger.Txn) error {
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
		if oldAssigneeID != "" {
			freedAssigneeIDs[oldAssigneeID] = 1
		}

		return nil
	})

	return freedAssigneeIDs, err
}

// UpdateJobStatus updates a job's status, result, and error message.
// This is a generic method for edge cases not covered by atomic methods.
func (b *BadgerBackend) UpdateJobStatus(ctx context.Context, jobID string, status JobStatus, result []byte, errorMsg string) (map[string]int, error) {
	now := time.Now()
	freedAssigneeIDs := make(map[string]int)

	err := b.db.Update(func(txn *badger.Txn) error {
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
	if len(tags) == 0 {
		return &JobStats{
			Tags:          tags,
			TotalJobs:     0,
			PendingJobs:   0,
			RunningJobs:   0,
			CompletedJobs: 0,
			FailedJobs:    0,
			TotalRetries:  0,
		}, nil
	}

	stats := &JobStats{
		Tags:          tags,
		TotalJobs:     0,
		PendingJobs:   0,
		RunningJobs:   0,
		CompletedJobs: 0,
		FailedJobs:    0,
		TotalRetries:  0,
	}

	// Find jobs that have all tags
	jobIDSet := make(map[string]int) // jobID -> tag count

	err := b.db.View(func(txn *badger.Txn) error {
		// For each tag, find all jobs
		for _, tag := range tags {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = []byte(keyPrefixTag + tag + ":")
			opts.PrefetchValues = false

			it := txn.NewIterator(opts)
			for it.Rewind(); it.Valid(); it.Next() {
				key := it.Item().Key()
				// Extract job ID from key: "tag:TAG:JOBID"
				jobID := string(key[len(keyPrefixTag)+len(tag)+1:])
				jobIDSet[jobID]++
			}
			it.Close()
		}

		// Only jobs that appear in all tag sets
		for jobID, count := range jobIDSet {
			if count == len(tags) {
				// Get job to check status
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

				stats.TotalJobs++
				stats.TotalRetries += int32(job.RetryCount)

				switch job.Status {
				case JobStatusInitialPending:
					stats.PendingJobs++
				case JobStatusRunning:
					stats.RunningJobs++
				case JobStatusCompleted:
					stats.CompletedJobs++
				case JobStatusFailedRetry:
					stats.FailedJobs++
				}
			}
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get job stats: %w", err)
	}

	return stats, nil
}

// ResetRunningJobs marks all running and cancelling jobs as unknown (for service restart)
// This is called when the service restarts and there are jobs that were in progress
// RUNNING jobs -> UNKNOWN_RETRY (eligible for retry)
// CANCELLING jobs -> UNKNOWN_STOPPED (terminal state, cancellation was in progress)
func (b *BadgerBackend) ResetRunningJobs(ctx context.Context) error {
	return b.db.Update(func(txn *badger.Txn) error {
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

			// Remove from running index
			_ = txn.Delete(item.Key())

			if job.Status == JobStatusRunning {
				// Mark as UNKNOWN_RETRY (eligible for retry)
				job.Status = JobStatusUnknownRetry
				job.AssigneeID = ""
				job.AssignedAt = nil
			} else if job.Status == JobStatusCancelling {
				// Mark as UNKNOWN_STOPPED (terminal state, cancellation was in progress)
				job.Status = JobStatusUnknownStopped
				job.AssigneeID = ""
				job.AssignedAt = nil
				now := time.Now()
				if job.FinalizedAt == nil {
					job.FinalizedAt = &now
				}
			} else {
				// Skip other statuses
				continue
			}

			// Don't add to pending index - jobs marked as unknown stay out of the queue

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

// CleanupExpiredJobs deletes completed jobs older than TTL
func (b *BadgerBackend) CleanupExpiredJobs(ctx context.Context, ttl time.Duration) error {
	cutoff := time.Now().Add(-ttl)

	return b.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(keyPrefixJob)
		opts.PrefetchValues = true

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
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
	jobIDSet := make(map[string]bool)

	// Collect job IDs from tags (AND logic)
	if len(tags) > 0 {
		err := b.db.View(func(txn *badger.Txn) error {
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
	err := b.db.View(func(txn *badger.Txn) error {
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
		for _, jobID := range allJobIDs {
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
	var job *Job

	err := b.db.View(func(txn *badger.Txn) error {
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
	cancelledJobIDs := make([]string, 0)
	unknownJobIDs := make([]string, 0)
	jobIDSet := make(map[string]bool)

	// Collect job IDs from tags (AND logic)
	if len(tags) > 0 {
		err := b.db.View(func(txn *badger.Txn) error {
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
	err := b.db.Update(func(txn *badger.Txn) error {
		for jobID := range jobIDSet {
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
	now := time.Now()
	return b.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(keyPrefixRunning + assigneeID + ":")
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
				job.AssigneeID = ""
				job.AssignedAt = nil

				// Add to pending index so it can be dequeued again
				// Use COALESCE(last_retry_at, created_at) for ordering (failure time >= creation time)
				timestamp := job.CreatedAt.Unix()
				if job.LastRetryAt != nil {
					timestamp = job.LastRetryAt.Unix()
				}
				if err := txn.Set(pendingIndexKey(jobID, timestamp), []byte(jobID)); err != nil {
					return fmt.Errorf("failed to add to pending index: %w", err)
				}
			} else if job.Status == JobStatusCancelling {
				// CANCELLING → UNKNOWN_STOPPED
				job.Status = JobStatusUnknownStopped
				if job.FinalizedAt == nil {
					job.FinalizedAt = &now
				}
				job.AssigneeID = ""
				job.AssignedAt = nil
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
	now := time.Now()
	return b.db.Update(func(txn *badger.Txn) error {
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
		job.AssigneeID = ""
		job.AssignedAt = nil

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
