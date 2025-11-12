package jobpool

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// BadgerBackend implements the Backend interface using BadgerDB.
// It provides high-performance key-value storage and is suitable for
// high-throughput scenarios.
type BadgerBackend struct {
	db *badger.DB
}

// NewBadgerBackend creates a new BadgerDB backend.
// The database directory will be created if it doesn't exist.
// dbPath is the path to the BadgerDB database directory.
func NewBadgerBackend(dbPath string) (*BadgerBackend, error) {
	opts := badger.DefaultOptions(dbPath)
	opts.Logger = nil // Disable logging for now

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	backend := &BadgerBackend{db: db}

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
			if job.Status == JobStatusPending {
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
func (b *BadgerBackend) DequeueJobs(ctx context.Context, assigneeID string, tags []string, limit int) ([]*Job, error) {
	// TODO: Implement tag filtering for Badger backend
	// For now, ignore tags parameter
	jobs := make([]*Job, 0, limit)
	now := time.Now()

	err := b.db.Update(func(txn *badger.Txn) error {
		// Iterate over pending jobs in order
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(keyPrefixPending)
		opts.PrefetchValues = true

		it := txn.NewIterator(opts)
		defer it.Close()

		jobIDs := make([]string, 0, limit)

		for it.Rewind(); it.Valid() && len(jobIDs) < limit; it.Next() {
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

			// Only process if still pending
			if job.Status != JobStatusPending {
				// Clean up stale index entry
				_ = txn.Delete(item.Key())
				continue
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

// UpdateJobStatus updates a job's status
func (b *BadgerBackend) UpdateJobStatus(ctx context.Context, jobID string, status JobStatus, result []byte, errorMsg string) error {
	now := time.Now()

	return b.db.Update(func(txn *badger.Txn) error {
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

		oldStatus := job.Status
		oldAssigneeID := job.AssigneeID

		// Update job
		job.Status = status
		job.Result = result
		job.ErrorMessage = errorMsg

		if status == JobStatusRunning && job.StartedAt == nil {
			job.StartedAt = &now
		}
		if status == JobStatusCompleted || status == JobStatusFailed {
			job.CompletedAt = &now
		}

		// Update indexes
		if oldStatus == JobStatusRunning && oldAssigneeID != "" {
			// Remove from running index
			_ = txn.Delete(runningIndexKey(oldAssigneeID, jobID))
		}

		if status == JobStatusCompleted || status == JobStatusFailed {
			// Job is done, no need to index
		} else if status == JobStatusRunning && oldStatus != JobStatusRunning {
			// Add to running index
			if job.AssigneeID != "" {
				if err := txn.Set(runningIndexKey(job.AssigneeID, jobID), []byte(jobID)); err != nil {
					return fmt.Errorf("failed to add to running index: %w", err)
				}
			}
		}

		// Save updated job
		updatedJobData, err := json.Marshal(&job)
		if err != nil {
			return fmt.Errorf("failed to marshal updated job: %w", err)
		}

		return txn.Set(jobKey(jobID), updatedJobData)
	})
}

// IncrementRetryCount increments the retry count for a job and sets it back to pending
func (b *BadgerBackend) IncrementRetryCount(ctx context.Context, jobID string) error {
	now := time.Now()

	return b.db.Update(func(txn *badger.Txn) error {
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

		// Update job
		job.RetryCount++
		job.LastRetryAt = &now
		job.Status = JobStatusPending
		job.AssigneeID = ""
		job.AssignedAt = nil

		// Remove from running index if was running
		if oldAssigneeID != "" {
			_ = txn.Delete(runningIndexKey(oldAssigneeID, jobID))
		}

		// Add to pending index
		timestamp := job.LastRetryAt.Unix()
		if err := txn.Set(pendingIndexKey(jobID, timestamp), []byte(jobID)); err != nil {
			return fmt.Errorf("failed to add to pending index: %w", err)
		}

		// Save updated job
		updatedJobData, err := json.Marshal(&job)
		if err != nil {
			return fmt.Errorf("failed to marshal updated job: %w", err)
		}

		return txn.Set(jobKey(jobID), updatedJobData)
	})
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
				case JobStatusPending:
					stats.PendingJobs++
				case JobStatusRunning:
					stats.RunningJobs++
				case JobStatusCompleted:
					stats.CompletedJobs++
				case JobStatusFailed:
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

// ResetRunningJobs marks all running jobs as unknown (for service restart)
// This is called when the service restarts and there are jobs that were in progress
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

			if job.Status == JobStatusRunning {
				// Mark as unknown
				job.Status = JobStatusUnknownRetry
				job.AssigneeID = ""
				job.AssignedAt = nil

				// Remove from running index
				_ = txn.Delete(item.Key())

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
		}

		return nil
	})
}

// ReturnJobsToPending returns all jobs assigned to the given assigneeID back to pending
func (b *BadgerBackend) ReturnJobsToPending(ctx context.Context, assigneeID string) error {
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

			if job.Status == JobStatusRunning && job.AssigneeID == assigneeID {
				// Reset to pending
				job.Status = JobStatusPending
				job.AssigneeID = ""
				job.AssignedAt = nil

				// Remove from running index
				_ = txn.Delete(item.Key())

				// Add to pending index
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
					return fmt.Errorf("failed to update job: %w", err)
				}
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
			if job.Status == JobStatusCompleted && job.CompletedAt != nil && job.CompletedAt.Before(cutoff) {
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

// CancelJob cancels a job by ID
func (b *BadgerBackend) CancelJob(ctx context.Context, jobID string) error {
	return b.db.Update(func(txn *badger.Txn) error {
		// Get the job
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

		// Check if job is in a terminal state
		if job.Status == JobStatusCompleted || job.Status == JobStatusFailed ||
			job.Status == JobStatusStopped || job.Status == JobStatusUnscheduled ||
			job.Status == JobStatusUnknownRetry {
			return fmt.Errorf("cannot cancel job in terminal state: %s", job.Status)
		}

		// Determine new status based on current status
		var newStatus JobStatus
		if job.Status == JobStatusPending {
			newStatus = JobStatusUnscheduled
		} else if job.Status == JobStatusRunning {
			newStatus = JobStatusStopped
		} else {
			return fmt.Errorf("unexpected job status for cancellation: %s", job.Status)
		}

		// Update job status
		job.Status = newStatus

		// If job was running, remove from running index
		if newStatus == JobStatusStopped && job.AssigneeID != "" {
			runningKey := runningIndexKey(job.AssigneeID, jobID)
			_ = txn.Delete(runningKey)
		}

		// If job was pending, remove from pending index
		if newStatus == JobStatusUnscheduled {
			timestamp := job.CreatedAt.Unix()
			if job.LastRetryAt != nil {
				timestamp = job.LastRetryAt.Unix()
			}
			pendingKey := pendingIndexKey(jobID, timestamp)
			_ = txn.Delete(pendingKey)
		}

		// Save updated job
		updatedJobData, err := json.Marshal(&job)
		if err != nil {
			return fmt.Errorf("failed to marshal updated job: %w", err)
		}

		if err := txn.Set(jobKey(jobID), updatedJobData); err != nil {
			return fmt.Errorf("failed to update job: %w", err)
		}

		return nil
	})
}

// CancelJobs cancels jobs by tags and/or job IDs (batch cancellation)
// TODO: Implement tag filtering for Badger backend
// For now, only supports jobIDs parameter
func (b *BadgerBackend) CancelJobs(ctx context.Context, tags []string, jobIDs []string) ([]string, []string, error) {
	cancelledJobIDs := make([]string, 0)
	unknownJobIDs := make([]string, 0)

	// TODO: Implement tag-based cancellation for Badger backend
	// For now, only process jobIDs
	for _, jobID := range jobIDs {
		err := b.CancelJob(ctx, jobID)
		if err != nil {
			unknownJobIDs = append(unknownJobIDs, jobID)
		} else {
			cancelledJobIDs = append(cancelledJobIDs, jobID)
		}
	}

	return cancelledJobIDs, unknownJobIDs, nil
}

// MarkJobsAsUnknown marks all running jobs assigned to the given assigneeID as unknown
func (b *BadgerBackend) MarkJobsAsUnknown(ctx context.Context, assigneeID string) error {
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

			if job.Status == JobStatusRunning && job.AssigneeID == assigneeID {
				// Mark as unknown
				job.Status = JobStatusUnknownRetry

				// Remove from running index
				_ = txn.Delete(item.Key())

				// Save updated job
				updatedJobData, err := json.Marshal(&job)
				if err != nil {
					return fmt.Errorf("failed to marshal updated job: %w", err)
				}

				if err := txn.Set(jobKey(jobID), updatedJobData); err != nil {
					return fmt.Errorf("failed to update job: %w", err)
				}
			}
		}

		return nil
	})
}
