package jobpool

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// InMemoryBackend implements the Backend interface using in-memory storage.
// It uses a single mutex for thread-safety and is suitable for testing.
type InMemoryBackend struct {
	mu    sync.RWMutex
	jobs  map[string]*Job
	tags  map[string]map[string]bool // jobID -> tag -> true
	jobID map[string]map[string]bool // tag -> jobID -> true (reverse index)
}

// NewInMemoryBackend creates a new in-memory backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		jobs:  make(map[string]*Job),
		tags:  make(map[string]map[string]bool),
		jobID: make(map[string]map[string]bool),
	}
}

// Close closes the backend (no-op for in-memory)
func (b *InMemoryBackend) Close() error {
	return nil
}

// EnqueueJob enqueues a single job
func (b *InMemoryBackend) EnqueueJob(ctx context.Context, job *Job) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Create a copy to avoid external modifications
	jobCopy := *job
	b.jobs[job.ID] = &jobCopy

	// Store tags
	b.tags[job.ID] = make(map[string]bool)
	for _, tag := range job.Tags {
		b.tags[job.ID][tag] = true
		if b.jobID[tag] == nil {
			b.jobID[tag] = make(map[string]bool)
		}
		b.jobID[tag][job.ID] = true
	}

	return job.ID, nil
}

// EnqueueJobs enqueues multiple jobs in a batch
func (b *InMemoryBackend) EnqueueJobs(ctx context.Context, jobs []*Job) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	jobIDs := make([]string, 0, len(jobs))
	for _, job := range jobs {
		// Create a copy to avoid external modifications
		jobCopy := *job
		b.jobs[job.ID] = &jobCopy

		// Store tags
		b.tags[job.ID] = make(map[string]bool)
		for _, tag := range job.Tags {
			b.tags[job.ID][tag] = true
			if b.jobID[tag] == nil {
				b.jobID[tag] = make(map[string]bool)
			}
			b.jobID[tag][job.ID] = true
		}

		jobIDs = append(jobIDs, job.ID)
	}

	return jobIDs, nil
}

// DequeueJobs dequeues pending jobs up to the limit and assigns them to the given assigneeID
func (b *InMemoryBackend) DequeueJobs(ctx context.Context, assigneeID string, tags []string, limit int) ([]*Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var eligibleJobs []*Job
	now := time.Now()

	// Find eligible jobs
	for _, job := range b.jobs {
		// Check if job is eligible (INITIAL_PENDING, FAILED_RETRY, or UNKNOWN_RETRY)
		if job.Status != JobStatusInitialPending && job.Status != JobStatusFailedRetry && job.Status != JobStatusUnknownRetry {
			continue
		}

		// Check if job matches tags (AND logic)
		if len(tags) > 0 {
			jobTags := b.tags[job.ID]
			matches := true
			for _, tag := range tags {
				if !jobTags[tag] {
					matches = false
					break
				}
			}
			if !matches {
				continue
			}
		}

		eligibleJobs = append(eligibleJobs, job)
	}

	// Sort by COALESCE(last_retry_at, created_at) in ascending order (oldest first)
	// Failure time is always >= creation time, so COALESCE is sufficient
	for i := 0; i < len(eligibleJobs)-1; i++ {
		for j := i + 1; j < len(eligibleJobs); j++ {
			// Use COALESCE(last_retry_at, created_at) for each job
			timeI := eligibleJobs[i].CreatedAt
			if eligibleJobs[i].LastRetryAt != nil {
				timeI = *eligibleJobs[i].LastRetryAt
			}
			timeJ := eligibleJobs[j].CreatedAt
			if eligibleJobs[j].LastRetryAt != nil {
				timeJ = *eligibleJobs[j].LastRetryAt
			}
			// Sort ascending (oldest first)
			if timeI.After(timeJ) {
				eligibleJobs[i], eligibleJobs[j] = eligibleJobs[j], eligibleJobs[i]
			}
		}
	}

	// Take up to limit
	if limit > len(eligibleJobs) {
		limit = len(eligibleJobs)
	}
	selectedJobs := eligibleJobs[:limit]

	// Update jobs to RUNNING and assign to worker
	result := make([]*Job, 0, len(selectedJobs))
	for _, job := range selectedJobs {
		job.Status = JobStatusRunning
		job.AssigneeID = assigneeID
		assignedAt := now
		job.AssignedAt = &assignedAt
		if job.StartedAt == nil {
			job.StartedAt = &assignedAt
		}

		// Create a copy for return
		jobCopy := *job
		result = append(result, &jobCopy)
	}

	return result, nil
}

// getFreedAssigneeID returns the assignee ID if job was assigned, empty string otherwise
func (b *InMemoryBackend) getFreedAssigneeID(jobID string) string {
	job, exists := b.jobs[jobID]
	if !exists || job.AssigneeID == "" {
		return ""
	}
	return job.AssigneeID
}

// CompleteJob atomically transitions a job to COMPLETED
func (b *InMemoryBackend) CompleteJob(ctx context.Context, jobID string, result []byte) (map[string]int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	job, exists := b.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}

	// Validate job is in a valid state that can transition to COMPLETED
	validStates := map[JobStatus]bool{
		JobStatusRunning:        true,
		JobStatusCancelling:     true,
		JobStatusUnknownRetry:   true,
		JobStatusUnknownStopped: true,
	}
	if !validStates[job.Status] {
		return nil, fmt.Errorf("job %s is not in a valid state for completion (current: %s)", jobID, job.Status)
	}

	// Get freed assignee ID before clearing
	freedAssigneeID := b.getFreedAssigneeID(jobID)

	// Update job
	now := time.Now()
	job.Status = JobStatusCompleted
	job.Result = result
	job.FinalizedAt = &now
	if job.StartedAt == nil {
		job.StartedAt = &now
	}
	// Preserve AssigneeID and AssignedAt for historical tracking

	// Return freed assignee ID (with count/multiplicity)
	freedAssigneeIDs := make(map[string]int)
	if freedAssigneeID != "" {
		freedAssigneeIDs[freedAssigneeID] = 1
	}
	return freedAssigneeIDs, nil
}

// FailJob atomically transitions a job to FAILED_RETRY, incrementing the retry count.
// Job remains in FAILED_RETRY state (eligible for scheduling, but never returns to INITIAL_PENDING).
func (b *InMemoryBackend) FailJob(ctx context.Context, jobID string, errorMsg string) (map[string]int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	job, exists := b.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}

	// Get freed assignee ID before clearing
	freedAssigneeID := b.getFreedAssigneeID(jobID)

	// Validate job is in a state that can transition to FAILED_RETRY
	if job.Status != JobStatusRunning && job.Status != JobStatusUnknownRetry {
		return nil, fmt.Errorf("job %s is not in RUNNING or UNKNOWN_RETRY state (current: %s)", jobID, job.Status)
	}

	// Update job to FAILED_RETRY with error message and increment retry count
	// Job remains in FAILED_RETRY state (eligible for scheduling, but never returns to INITIAL_PENDING)
	now := time.Now()
	job.Status = JobStatusFailedRetry
	job.ErrorMessage = errorMsg
	job.RetryCount++
	job.LastRetryAt = &now
	// Preserve AssigneeID and AssignedAt for historical tracking

	// Return freed assignee ID (with count/multiplicity)
	freedAssigneeIDs := make(map[string]int)
	if freedAssigneeID != "" {
		freedAssigneeIDs[freedAssigneeID] = 1
	}
	return freedAssigneeIDs, nil
}

// StopJob atomically transitions a job to STOPPED
func (b *InMemoryBackend) StopJob(ctx context.Context, jobID string, errorMsg string) (map[string]int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	job, exists := b.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}

	// Get freed assignee ID before clearing
	freedAssigneeID := b.getFreedAssigneeID(jobID)

	// Update job
	now := time.Now()
	job.Status = JobStatusStopped
	job.ErrorMessage = errorMsg
	if job.FinalizedAt == nil {
		job.FinalizedAt = &now
	}
	// Preserve AssigneeID and AssignedAt for historical tracking

	// Return freed assignee ID (with count/multiplicity)
	freedAssigneeIDs := make(map[string]int)
	if freedAssigneeID != "" {
		freedAssigneeIDs[freedAssigneeID] = 1
	}
	return freedAssigneeIDs, nil
}

// StopJobWithRetry atomically transitions a job from CANCELLING to STOPPED with retry increment
func (b *InMemoryBackend) StopJobWithRetry(ctx context.Context, jobID string, errorMsg string) (map[string]int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	job, exists := b.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}

	if job.Status != JobStatusCancelling {
		return nil, fmt.Errorf("job is not in CANCELLING state: %s", job.Status)
	}

	// Get freed assignee ID before clearing
	freedAssigneeID := b.getFreedAssigneeID(jobID)

	// Update job
	now := time.Now()
	job.Status = JobStatusStopped
	job.ErrorMessage = errorMsg
	job.RetryCount++
	job.LastRetryAt = &now
	if job.FinalizedAt == nil {
		job.FinalizedAt = &now
	}
	// Preserve AssigneeID and AssignedAt for historical tracking

	// Return freed assignee ID (with count/multiplicity)
	freedAssigneeIDs := make(map[string]int)
	if freedAssigneeID != "" {
		freedAssigneeIDs[freedAssigneeID] = 1
	}
	return freedAssigneeIDs, nil
}

// MarkJobUnknownStopped atomically transitions a job to UNKNOWN_STOPPED
func (b *InMemoryBackend) MarkJobUnknownStopped(ctx context.Context, jobID string, errorMsg string) (map[string]int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	job, exists := b.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}

	// Get freed assignee ID before clearing
	freedAssigneeID := b.getFreedAssigneeID(jobID)

	// Update job
	now := time.Now()
	job.Status = JobStatusUnknownStopped
	job.ErrorMessage = errorMsg
	if job.FinalizedAt == nil {
		job.FinalizedAt = &now
	}
	// Preserve AssigneeID and AssignedAt for historical tracking

	// Return freed assignee ID (with count/multiplicity)
	freedAssigneeIDs := make(map[string]int)
	if freedAssigneeID != "" {
		freedAssigneeIDs[freedAssigneeID] = 1
	}
	return freedAssigneeIDs, nil
}

// UpdateJobStatus updates a job's status, result, and error message
func (b *InMemoryBackend) UpdateJobStatus(ctx context.Context, jobID string, status JobStatus, result []byte, errorMsg string) (map[string]int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	job, exists := b.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}

	// Get freed assignee ID if transitioning from RUNNING to terminal state
	var freedAssigneeID string
	wasRunning := job.Status == JobStatusRunning
	isTerminal := status == JobStatusCompleted || status == JobStatusStopped || status == JobStatusUnknownStopped
	if wasRunning && isTerminal {
		freedAssigneeID = b.getFreedAssigneeID(jobID)
	}

	// Update job
	now := time.Now()
	job.Status = status
	if result != nil {
		job.Result = result
	}
	if errorMsg != "" {
		job.ErrorMessage = errorMsg
	}

	// Update timestamps
	if status == JobStatusRunning && job.StartedAt == nil {
		job.StartedAt = &now
	}
	if status == JobStatusCompleted || status == JobStatusStopped || status == JobStatusUnknownStopped {
		if job.FinalizedAt == nil {
			job.FinalizedAt = &now
		}
	}

	// Preserve AssigneeID and AssignedAt for historical tracking

	// Return freed assignee ID (with count/multiplicity)
	freedAssigneeIDs := make(map[string]int)
	if freedAssigneeID != "" {
		freedAssigneeIDs[freedAssigneeID] = 1
	}
	return freedAssigneeIDs, nil
}

// CancelJobs cancels jobs by tags and/or job IDs
func (b *InMemoryBackend) CancelJobs(ctx context.Context, tags []string, jobIDs []string) ([]string, []string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cancelledJobIDs := make([]string, 0)
	unknownJobIDs := make([]string, 0)

	// Process job IDs
	for _, jobID := range jobIDs {
		job, exists := b.jobs[jobID]
		if !exists {
			unknownJobIDs = append(unknownJobIDs, jobID)
			continue
		}

		switch job.Status {
		case JobStatusInitialPending:
			job.Status = JobStatusUnscheduled
			cancelledJobIDs = append(cancelledJobIDs, jobID)
		case JobStatusRunning:
			job.Status = JobStatusCancelling
			cancelledJobIDs = append(cancelledJobIDs, jobID)
		case JobStatusCancelling:
			// Already cancelling - treat as no-op, include in cancelledJobIDs
			cancelledJobIDs = append(cancelledJobIDs, jobID)
		case JobStatusFailedRetry:
			job.Status = JobStatusStopped
			cancelledJobIDs = append(cancelledJobIDs, jobID)
		case JobStatusUnknownRetry:
			job.Status = JobStatusStopped
			cancelledJobIDs = append(cancelledJobIDs, jobID)
		default:
			unknownJobIDs = append(unknownJobIDs, jobID)
		}
	}

	// Process tags
	if len(tags) > 0 {
		// Find jobs matching all tags
		matchingJobIDs := make(map[string]bool)
		firstTag := true

		for _, tag := range tags {
			tagJobs := b.jobID[tag]
			if firstTag {
				for jobID := range tagJobs {
					matchingJobIDs[jobID] = true
				}
				firstTag = false
			} else {
				// Intersection
				for jobID := range matchingJobIDs {
					if !tagJobs[jobID] {
						delete(matchingJobIDs, jobID)
					}
				}
			}
		}

		// Cancel matching jobs
		for jobID := range matchingJobIDs {
			job := b.jobs[jobID]
			switch job.Status {
			case JobStatusInitialPending:
				job.Status = JobStatusUnscheduled
				cancelledJobIDs = append(cancelledJobIDs, jobID)
			case JobStatusRunning:
				job.Status = JobStatusCancelling
				cancelledJobIDs = append(cancelledJobIDs, jobID)
			case JobStatusCancelling:
				// Already cancelling - treat as no-op, include in cancelledJobIDs
				cancelledJobIDs = append(cancelledJobIDs, jobID)
			case JobStatusFailedRetry:
				job.Status = JobStatusStopped
				cancelledJobIDs = append(cancelledJobIDs, jobID)
			case JobStatusUnknownRetry:
				job.Status = JobStatusStopped
				cancelledJobIDs = append(cancelledJobIDs, jobID)
			}
		}
	}

	return cancelledJobIDs, unknownJobIDs, nil
}

// AcknowledgeCancellation handles cancellation acknowledgment from worker
func (b *InMemoryBackend) AcknowledgeCancellation(ctx context.Context, jobID string, wasExecuting bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	job, exists := b.jobs[jobID]
	if !exists {
		return fmt.Errorf("job not found: %s", jobID)
	}

	if job.Status != JobStatusCancelling {
		return fmt.Errorf("job is not in CANCELLING state: %s", job.Status)
	}

	now := time.Now()
	if wasExecuting {
		job.Status = JobStatusStopped
	} else {
		job.Status = JobStatusUnknownStopped
	}
	if job.FinalizedAt == nil {
		job.FinalizedAt = &now
	}
	// Preserve AssigneeID and AssignedAt for historical tracking

	return nil
}

// MarkWorkerUnresponsive marks all jobs assigned to the given assigneeID as unresponsive
func (b *InMemoryBackend) MarkWorkerUnresponsive(ctx context.Context, assigneeID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, job := range b.jobs {
		if job.AssigneeID == assigneeID {
			switch job.Status {
			case JobStatusRunning:
				job.Status = JobStatusUnknownRetry
				// Preserve AssigneeID and AssignedAt for historical tracking
			case JobStatusCancelling:
				job.Status = JobStatusUnknownStopped
				// Preserve AssigneeID and AssignedAt for historical tracking
			}
		}
	}

	return nil
}

// GetJob retrieves a job by ID
func (b *InMemoryBackend) GetJob(ctx context.Context, jobID string) (*Job, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	job, exists := b.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}

	// Create a copy to avoid external modifications
	jobCopy := *job
	jobCopy.Tags = make([]string, 0, len(b.tags[jobID]))
	for tag := range b.tags[jobID] {
		jobCopy.Tags = append(jobCopy.Tags, tag)
	}

	return &jobCopy, nil
}

// GetJobStats gets statistics for jobs matching ALL provided tags
func (b *InMemoryBackend) GetJobStats(ctx context.Context, tags []string) (*JobStats, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	stats := &JobStats{
		Tags: tags,
	}

	// Find jobs matching all tags
	matchingJobIDs := make(map[string]bool)
	if len(tags) == 0 {
		// No tags means all jobs
		for jobID := range b.jobs {
			matchingJobIDs[jobID] = true
		}
	} else {
		firstTag := true
		for _, tag := range tags {
			tagJobs := b.jobID[tag]
			if firstTag {
				for jobID := range tagJobs {
					matchingJobIDs[jobID] = true
				}
				firstTag = false
			} else {
				// Intersection
				for jobID := range matchingJobIDs {
					if !tagJobs[jobID] {
						delete(matchingJobIDs, jobID)
					}
				}
			}
		}
	}

	// Count stats
	for jobID := range matchingJobIDs {
		job := b.jobs[jobID]
		stats.TotalJobs++
		switch job.Status {
		case JobStatusInitialPending:
			stats.PendingJobs++
		case JobStatusRunning:
			stats.RunningJobs++
		case JobStatusCompleted:
			stats.CompletedJobs++
		case JobStatusFailedRetry, JobStatusUnknownRetry:
			stats.FailedJobs++
		case JobStatusStopped, JobStatusUnscheduled, JobStatusUnknownStopped:
			stats.StoppedJobs++
		}
		stats.TotalRetries += int32(job.RetryCount)
	}

	return stats, nil
}

// ResetRunningJobs marks all running jobs as unknown (for service restart)
func (b *InMemoryBackend) ResetRunningJobs(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, job := range b.jobs {
		if job.Status == JobStatusRunning {
			job.Status = JobStatusUnknownRetry
			// Preserve AssigneeID and AssignedAt for historical tracking
		}
	}

	return nil
}

// CleanupExpiredJobs deletes completed jobs older than TTL
func (b *InMemoryBackend) CleanupExpiredJobs(ctx context.Context, ttl time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	cutoff := time.Now().Add(-ttl)
	for jobID, job := range b.jobs {
		if job.Status == JobStatusCompleted && job.FinalizedAt != nil && job.FinalizedAt.Before(cutoff) {
			delete(b.jobs, jobID)
			delete(b.tags, jobID)
			for tag := range b.tags[jobID] {
				delete(b.jobID[tag], jobID)
			}
		}
	}

	return nil
}

// DeleteJobs forcefully deletes jobs by tags and/or job IDs
func (b *InMemoryBackend) DeleteJobs(ctx context.Context, tags []string, jobIDs []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Validate all jobs are in final states
	for _, jobID := range jobIDs {
		job, exists := b.jobs[jobID]
		if !exists {
			continue
		}
		if job.Status != JobStatusCompleted && job.Status != JobStatusUnscheduled &&
			job.Status != JobStatusStopped && job.Status != JobStatusUnknownStopped {
			return fmt.Errorf("job %s is not in a final state: %s", jobID, job.Status)
		}
	}

	// Delete by job IDs
	for _, jobID := range jobIDs {
		if job, exists := b.jobs[jobID]; exists {
			if job.Status == JobStatusCompleted || job.Status == JobStatusUnscheduled ||
				job.Status == JobStatusStopped || job.Status == JobStatusUnknownStopped {
				delete(b.jobs, jobID)
				for tag := range b.tags[jobID] {
					delete(b.jobID[tag], jobID)
				}
				delete(b.tags, jobID)
			}
		}
	}

	// Delete by tags
	if len(tags) > 0 {
		matchingJobIDs := make(map[string]bool)
		firstTag := true
		for _, tag := range tags {
			tagJobs := b.jobID[tag]
			if firstTag {
				for jobID := range tagJobs {
					matchingJobIDs[jobID] = true
				}
				firstTag = false
			} else {
				for jobID := range matchingJobIDs {
					if !tagJobs[jobID] {
						delete(matchingJobIDs, jobID)
					}
				}
			}
		}

		for jobID := range matchingJobIDs {
			if job, exists := b.jobs[jobID]; exists {
				if job.Status == JobStatusCompleted || job.Status == JobStatusUnscheduled ||
					job.Status == JobStatusStopped || job.Status == JobStatusUnknownStopped {
					delete(b.jobs, jobID)
					for tag := range b.tags[jobID] {
						delete(b.jobID[tag], jobID)
					}
					delete(b.tags, jobID)
				}
			}
		}
	}

	return nil
}
