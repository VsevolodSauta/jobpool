package jobpool

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// InMemoryBackend implements the Backend interface using in-memory storage.
// It uses a single mutex for thread-safety and is suitable for testing.
type InMemoryBackend struct {
	mu     sync.RWMutex
	jobs   map[string]*Job
	tags   map[string]map[string]bool // jobID -> tag -> true
	jobID  map[string]map[string]bool // tag -> jobID -> true (reverse index)
	closed bool
}

// NewInMemoryBackend creates a new in-memory backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		jobs:  make(map[string]*Job),
		tags:  make(map[string]map[string]bool),
		jobID: make(map[string]map[string]bool),
	}
}

// Close closes the backend and prevents further operations.
func (b *InMemoryBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}
	b.closed = true
	return nil
}

// EnqueueJob enqueues a single job.
func (b *InMemoryBackend) EnqueueJob(ctx context.Context, job *Job) (string, error) {
	if _, err := normalizeContext(ctx); err != nil {
		return "", err
	}
	if job == nil {
		return "", fmt.Errorf("job is nil")
	}
	if job.ID == "" {
		return "", fmt.Errorf("job ID is required")
	}
	if job.Status != JobStatusInitialPending {
		return "", fmt.Errorf("job %s must have status %s", job.ID, JobStatusInitialPending)
	}

	now := time.Now()
	prepared := prepareJobForEnqueue(job, now)

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpenLocked(); err != nil {
		return "", err
	}
	if _, exists := b.jobs[job.ID]; exists {
		return "", fmt.Errorf("job already exists: %s", job.ID)
	}

	b.storeJob(prepared)
	return prepared.ID, nil
}

// EnqueueJobs enqueues multiple jobs in a batch.
func (b *InMemoryBackend) EnqueueJobs(ctx context.Context, jobs []*Job) ([]string, error) {
	if _, err := normalizeContext(ctx); err != nil {
		return nil, err
	}
	if len(jobs) == 0 {
		return []string{}, nil
	}

	now := time.Now()
	seen := make(map[string]bool, len(jobs))
	prepared := make([]*Job, len(jobs))

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
		if seen[job.ID] {
			return nil, fmt.Errorf("duplicate job ID %s in batch", job.ID)
		}
		seen[job.ID] = true
		prepared[idx] = prepareJobForEnqueue(job, now)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpenLocked(); err != nil {
		return nil, err
	}
	for _, job := range prepared {
		if _, exists := b.jobs[job.ID]; exists {
			return nil, fmt.Errorf("job already exists: %s", job.ID)
		}
	}

	jobIDs := make([]string, 0, len(prepared))
	for _, job := range prepared {
		b.storeJob(job)
		jobIDs = append(jobIDs, job.ID)
	}
	return jobIDs, nil
}

// DequeueJobs dequeues pending jobs up to the limit and assigns them to the given assigneeID.
func (b *InMemoryBackend) DequeueJobs(ctx context.Context, assigneeID string, tags []string, limit int) ([]*Job, error) {
	if _, err := normalizeContext(ctx); err != nil {
		return nil, err
	}
	if assigneeID == "" {
		return nil, fmt.Errorf("assigneeID is required")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be greater than 0")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpenLocked(); err != nil {
		return nil, err
	}

	eligibleJobs := make([]*Job, 0)
	for _, job := range b.jobs {
		if !isEligibleForScheduling(job.Status) {
			continue
		}
		if !b.jobMatchesTags(job.ID, tags) {
			continue
		}
		eligibleJobs = append(eligibleJobs, job)
	}

	sort.Slice(eligibleJobs, func(i, j int) bool {
		return jobScheduleTime(eligibleJobs[i]).Before(jobScheduleTime(eligibleJobs[j]))
	})

	if len(eligibleJobs) > limit {
		eligibleJobs = eligibleJobs[:limit]
	}

	now := time.Now()
	result := make([]*Job, 0, len(eligibleJobs))
	for _, job := range eligibleJobs {
		job.Status = JobStatusRunning
		job.AssigneeID = assigneeID
		assigned := now
		job.AssignedAt = &assigned
		if job.StartedAt == nil {
			started := assigned
			job.StartedAt = &started
		}
		result = append(result, cloneJob(job))
	}

	return result, nil
}

// CompleteJob atomically transitions a job to COMPLETED.
func (b *InMemoryBackend) CompleteJob(ctx context.Context, jobID string, result []byte) (map[string]int, error) {
	if _, err := normalizeContext(ctx); err != nil {
		return nil, err
	}
	if jobID == "" {
		return nil, fmt.Errorf("jobID is required")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpenLocked(); err != nil {
		return nil, err
	}

	job, exists := b.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}

	validStates := map[JobStatus]bool{
		JobStatusRunning:        true,
		JobStatusCancelling:     true,
		JobStatusUnknownRetry:   true,
		JobStatusUnknownStopped: true,
	}
	if !validStates[job.Status] {
		return nil, fmt.Errorf("job %s is not in a valid state for completion (current: %s)", jobID, job.Status)
	}

	freed := freedAssignee(job.AssigneeID, job.Status == JobStatusRunning || job.Status == JobStatusCancelling)
	now := time.Now()
	job.Status = JobStatusCompleted
	job.Result = copyBytes(result)
	job.FinalizedAt = &now
	if job.StartedAt == nil {
		started := now
		job.StartedAt = &started
	}

	return freed, nil
}

// FailJob atomically transitions a job to FAILED_RETRY, incrementing the retry count.
func (b *InMemoryBackend) FailJob(ctx context.Context, jobID string, errorMsg string) (map[string]int, error) {
	if _, err := normalizeContext(ctx); err != nil {
		return nil, err
	}
	if jobID == "" {
		return nil, fmt.Errorf("jobID is required")
	}
	if errorMsg == "" {
		return nil, fmt.Errorf("error message is required")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpenLocked(); err != nil {
		return nil, err
	}

	job, exists := b.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}
	if job.Status != JobStatusRunning && job.Status != JobStatusUnknownRetry {
		return nil, fmt.Errorf("job %s is not in RUNNING or UNKNOWN_RETRY state (current: %s)", jobID, job.Status)
	}

	freed := freedAssignee(job.AssigneeID, job.Status == JobStatusRunning)
	now := time.Now()
	job.Status = JobStatusFailedRetry
	job.ErrorMessage = errorMsg
	job.RetryCount++
	job.LastRetryAt = &now

	return freed, nil
}

// StopJob atomically transitions a job to STOPPED.
func (b *InMemoryBackend) StopJob(ctx context.Context, jobID string, errorMsg string) (map[string]int, error) {
	if _, err := normalizeContext(ctx); err != nil {
		return nil, err
	}
	if jobID == "" {
		return nil, fmt.Errorf("jobID is required")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpenLocked(); err != nil {
		return nil, err
	}

	job, exists := b.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}
	validStates := map[JobStatus]bool{
		JobStatusRunning:      true,
		JobStatusCancelling:   true,
		JobStatusUnknownRetry: true,
	}
	if !validStates[job.Status] {
		return nil, fmt.Errorf("job %s is not in a valid state for stopping (current: %s)", jobID, job.Status)
	}

	freed := freedAssignee(job.AssigneeID, job.Status == JobStatusRunning || job.Status == JobStatusCancelling)
	now := time.Now()
	job.Status = JobStatusStopped
	job.ErrorMessage = errorMsg
	if job.FinalizedAt == nil {
		finalized := now
		job.FinalizedAt = &finalized
	}

	return freed, nil
}

// StopJobWithRetry atomically transitions a job from CANCELLING to STOPPED with retry increment.
func (b *InMemoryBackend) StopJobWithRetry(ctx context.Context, jobID string, errorMsg string) (map[string]int, error) {
	if _, err := normalizeContext(ctx); err != nil {
		return nil, err
	}
	if jobID == "" {
		return nil, fmt.Errorf("jobID is required")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpenLocked(); err != nil {
		return nil, err
	}

	job, exists := b.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}
	if job.Status != JobStatusCancelling {
		return nil, fmt.Errorf("job %s is not in CANCELLING state (current: %s)", jobID, job.Status)
	}

	freed := freedAssignee(job.AssigneeID, true)
	now := time.Now()
	job.Status = JobStatusStopped
	job.ErrorMessage = errorMsg
	job.RetryCount++
	job.LastRetryAt = &now
	if job.FinalizedAt == nil {
		finalized := now
		job.FinalizedAt = &finalized
	}

	return freed, nil
}

// MarkJobUnknownStopped atomically transitions a job to UNKNOWN_STOPPED.
func (b *InMemoryBackend) MarkJobUnknownStopped(ctx context.Context, jobID string, errorMsg string) (map[string]int, error) {
	if _, err := normalizeContext(ctx); err != nil {
		return nil, err
	}
	if jobID == "" {
		return nil, fmt.Errorf("jobID is required")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpenLocked(); err != nil {
		return nil, err
	}

	job, exists := b.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}

	freed := freedAssignee(job.AssigneeID, job.Status == JobStatusRunning || job.Status == JobStatusCancelling)
	now := time.Now()
	job.Status = JobStatusUnknownStopped
	job.ErrorMessage = errorMsg
	if job.FinalizedAt == nil {
		finalized := now
		job.FinalizedAt = &finalized
	}

	return freed, nil
}

// UpdateJobStatus updates a job's status, result, and error message.
func (b *InMemoryBackend) UpdateJobStatus(ctx context.Context, jobID string, status JobStatus, result []byte, errorMsg string) (map[string]int, error) {
	if _, err := normalizeContext(ctx); err != nil {
		return nil, err
	}
	if jobID == "" {
		return nil, fmt.Errorf("jobID is required")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpenLocked(); err != nil {
		return nil, err
	}

	job, exists := b.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}
	if !isValidTransition(job.Status, status) {
		return nil, fmt.Errorf("invalid transition from %s to %s", job.Status, status)
	}

	shouldFree := (job.Status == JobStatusRunning || job.Status == JobStatusCancelling) && isFinalStatus(status)
	freed := freedAssignee(job.AssigneeID, shouldFree)
	now := time.Now()

	job.Status = status
	if result != nil {
		job.Result = copyBytes(result)
	}
	if errorMsg != "" {
		job.ErrorMessage = errorMsg
	}

	if status == JobStatusRunning && job.StartedAt == nil {
		started := now
		job.StartedAt = &started
	}
	if isFinalStatus(status) && job.FinalizedAt == nil {
		finalized := now
		job.FinalizedAt = &finalized
	}

	return freed, nil
}

// CancelJobs cancels jobs by tags and/or job IDs (batch cancellation).
func (b *InMemoryBackend) CancelJobs(ctx context.Context, tags []string, jobIDs []string) ([]string, []string, error) {
	if _, err := normalizeContext(ctx); err != nil {
		return nil, nil, err
	}
	if len(tags) == 0 && len(jobIDs) == 0 {
		return nil, nil, fmt.Errorf("either tags or jobIDs must be provided")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpenLocked(); err != nil {
		return nil, nil, err
	}

	targetIDs := make(map[string]bool)
	for _, id := range jobIDs {
		targetIDs[id] = true
	}
	if len(tags) > 0 {
		for id := range b.jobs {
			if b.jobMatchesTags(id, tags) {
				targetIDs[id] = true
			}
		}
	}

	if len(targetIDs) == 0 {
		return []string{}, []string{}, nil
	}

	ids := make([]string, 0, len(targetIDs))
	for id := range targetIDs {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	cancelled := make([]string, 0, len(ids))
	unknown := make([]string, 0)

	for _, id := range ids {
		job, exists := b.jobs[id]
		if !exists {
			unknown = append(unknown, id)
			continue
		}

		switch job.Status {
		case JobStatusInitialPending:
			job.Status = JobStatusUnscheduled
			cancelled = append(cancelled, id)
		case JobStatusRunning:
			job.Status = JobStatusCancelling
			cancelled = append(cancelled, id)
		case JobStatusCancelling:
			cancelled = append(cancelled, id)
		case JobStatusFailedRetry, JobStatusUnknownRetry:
			now := time.Now()
			job.Status = JobStatusStopped
			if job.FinalizedAt == nil {
				finalized := now
				job.FinalizedAt = &finalized
			}
			cancelled = append(cancelled, id)
		default:
			unknown = append(unknown, id)
		}
	}

	return cancelled, unknown, nil
}

// AcknowledgeCancellation handles cancellation acknowledgment from worker.
func (b *InMemoryBackend) AcknowledgeCancellation(ctx context.Context, jobID string, wasExecuting bool) error {
	if _, err := normalizeContext(ctx); err != nil {
		return err
	}
	if jobID == "" {
		return fmt.Errorf("jobID is required")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpenLocked(); err != nil {
		return err
	}

	job, exists := b.jobs[jobID]
	if !exists {
		return fmt.Errorf("job not found: %s", jobID)
	}
	if job.Status != JobStatusCancelling {
		return fmt.Errorf("job %s is not in CANCELLING state (current: %s)", jobID, job.Status)
	}

	now := time.Now()
	if wasExecuting {
		job.Status = JobStatusStopped
	} else {
		job.Status = JobStatusUnknownStopped
	}
	if job.FinalizedAt == nil {
		finalized := now
		job.FinalizedAt = &finalized
	}

	return nil
}

// MarkWorkerUnresponsive marks all jobs assigned to the given assigneeID as unresponsive.
func (b *InMemoryBackend) MarkWorkerUnresponsive(ctx context.Context, assigneeID string) error {
	if _, err := normalizeContext(ctx); err != nil {
		return err
	}
	if assigneeID == "" {
		return fmt.Errorf("assigneeID is required")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpenLocked(); err != nil {
		return err
	}

	now := time.Now()
	for _, job := range b.jobs {
		if job.AssigneeID != assigneeID {
			continue
		}
		switch job.Status {
		case JobStatusRunning:
			job.Status = JobStatusUnknownRetry
		case JobStatusCancelling:
			job.Status = JobStatusUnknownStopped
			if job.FinalizedAt == nil {
				finalized := now
				job.FinalizedAt = &finalized
			}
		}
	}

	return nil
}

// GetJob retrieves a job by ID.
func (b *InMemoryBackend) GetJob(ctx context.Context, jobID string) (*Job, error) {
	if _, err := normalizeContext(ctx); err != nil {
		return nil, err
	}
	if jobID == "" {
		return nil, fmt.Errorf("jobID is required")
	}

	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, fmt.Errorf("backend is closed")
	}

	job, exists := b.jobs[jobID]
	if !exists {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}

	jobCopy := cloneJob(job)
	jobCopy.Tags = make([]string, 0, len(b.tags[jobID]))
	for tag := range b.tags[jobID] {
		jobCopy.Tags = append(jobCopy.Tags, tag)
	}

	return jobCopy, nil
}

// GetJobStats gets statistics for jobs matching ALL provided tags.
func (b *InMemoryBackend) GetJobStats(ctx context.Context, tags []string) (*JobStats, error) {
	if _, err := normalizeContext(ctx); err != nil {
		return nil, err
	}

	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, fmt.Errorf("backend is closed")
	}

	stats := &JobStats{
		Tags: tags,
	}

	for jobID, job := range b.jobs {
		if !b.jobMatchesTags(jobID, tags) {
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
		case JobStatusFailedRetry, JobStatusUnknownRetry:
			stats.FailedJobs++
		case JobStatusStopped, JobStatusUnscheduled, JobStatusUnknownStopped:
			stats.StoppedJobs++
		}
	}

	return stats, nil
}

// ResetRunningJobs marks all running and cancelling jobs as unknown (for service restart).
func (b *InMemoryBackend) ResetRunningJobs(ctx context.Context) error {
	if _, err := normalizeContext(ctx); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpenLocked(); err != nil {
		return err
	}

	now := time.Now()
	for _, job := range b.jobs {
		switch job.Status {
		case JobStatusRunning:
			job.Status = JobStatusUnknownRetry
		case JobStatusCancelling:
			job.Status = JobStatusUnknownStopped
			if job.FinalizedAt == nil {
				finalized := now
				job.FinalizedAt = &finalized
			}
		}
	}

	return nil
}

// CleanupExpiredJobs deletes completed jobs older than TTL.
func (b *InMemoryBackend) CleanupExpiredJobs(ctx context.Context, ttl time.Duration) error {
	if _, err := normalizeContext(ctx); err != nil {
		return err
	}
	if ttl <= 0 {
		return fmt.Errorf("ttl must be greater than 0")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpenLocked(); err != nil {
		return err
	}

	cutoff := time.Now().Add(-ttl)
	for jobID, job := range b.jobs {
		if job.Status == JobStatusCompleted && job.FinalizedAt != nil && job.FinalizedAt.Before(cutoff) {
			b.removeJob(jobID)
		}
	}

	return nil
}

// DeleteJobs forcefully deletes jobs by tags and/or job IDs.
func (b *InMemoryBackend) DeleteJobs(ctx context.Context, tags []string, jobIDs []string) error {
	if _, err := normalizeContext(ctx); err != nil {
		return err
	}
	if len(tags) == 0 && len(jobIDs) == 0 {
		return fmt.Errorf("either tags or jobIDs must be provided")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ensureOpenLocked(); err != nil {
		return err
	}

	targetIDs := make(map[string]bool)
	for _, id := range jobIDs {
		targetIDs[id] = true
	}
	if len(tags) > 0 {
		for id := range b.jobs {
			if b.jobMatchesTags(id, tags) {
				targetIDs[id] = true
			}
		}
	}

	if len(targetIDs) == 0 {
		return nil
	}

	for id := range targetIDs {
		job, exists := b.jobs[id]
		if !exists {
			continue
		}
		if !isFinalStatus(job.Status) {
			return fmt.Errorf("job %s is not in a final state: %s", id, job.Status)
		}
	}

	for id := range targetIDs {
		if _, exists := b.jobs[id]; exists {
			b.removeJob(id)
		}
	}

	return nil
}

// Helper functions

func (b *InMemoryBackend) storeJob(job *Job) {
	jobCopy := cloneJob(job)
	b.jobs[jobCopy.ID] = jobCopy

	tagSet := make(map[string]bool, len(jobCopy.Tags))
	for _, tag := range jobCopy.Tags {
		tagSet[tag] = true
		if b.jobID[tag] == nil {
			b.jobID[tag] = make(map[string]bool)
		}
		b.jobID[tag][jobCopy.ID] = true
	}
	b.tags[jobCopy.ID] = tagSet
}

func (b *InMemoryBackend) removeJob(jobID string) {
	if tagSet, ok := b.tags[jobID]; ok {
		for tag := range tagSet {
			if jobIDs, exists := b.jobID[tag]; exists {
				delete(jobIDs, jobID)
				if len(jobIDs) == 0 {
					delete(b.jobID, tag)
				}
			}
		}
		delete(b.tags, jobID)
	}
	delete(b.jobs, jobID)
}

func (b *InMemoryBackend) jobMatchesTags(jobID string, tags []string) bool {
	if len(tags) == 0 {
		return true
	}
	tagSet := b.tags[jobID]
	if tagSet == nil {
		return false
	}
	for _, tag := range tags {
		if !tagSet[tag] {
			return false
		}
	}
	return true
}

func (b *InMemoryBackend) ensureOpenLocked() error {
	if b.closed {
		return fmt.Errorf("backend is closed")
	}
	return nil
}

func prepareJobForEnqueue(job *Job, now time.Time) *Job {
	prepared := cloneJob(job)
	if prepared.CreatedAt.IsZero() {
		prepared.CreatedAt = now
	}
	prepared.Status = JobStatusInitialPending
	prepared.StartedAt = nil
	prepared.FinalizedAt = nil
	prepared.ErrorMessage = ""
	prepared.Result = nil
	prepared.RetryCount = 0
	prepared.LastRetryAt = nil
	prepared.AssigneeID = ""
	prepared.AssignedAt = nil
	return prepared
}

func cloneJob(job *Job) *Job {
	if job == nil {
		return nil
	}
	clone := *job
	clone.JobDefinition = copyBytes(job.JobDefinition)
	clone.Result = copyBytes(job.Result)
	clone.Tags = copyStringSlice(job.Tags)
	clone.StartedAt = copyTimePtr(job.StartedAt)
	clone.FinalizedAt = copyTimePtr(job.FinalizedAt)
	clone.LastRetryAt = copyTimePtr(job.LastRetryAt)
	clone.AssignedAt = copyTimePtr(job.AssignedAt)
	return &clone
}

func copyBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func copyStringSlice(src []string) []string {
	if src == nil {
		return nil
	}
	dst := make([]string, len(src))
	copy(dst, src)
	return dst
}

func copyTimePtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	val := *t
	return &val
}

func jobScheduleTime(job *Job) time.Time {
	if job.LastRetryAt != nil {
		return *job.LastRetryAt
	}
	return job.CreatedAt
}

func freedAssignee(assigneeID string, shouldFree bool) map[string]int {
	if !shouldFree || assigneeID == "" {
		return map[string]int{}
	}
	return map[string]int{assigneeID: 1}
}

func isEligibleForScheduling(status JobStatus) bool {
	return status == JobStatusInitialPending ||
		status == JobStatusFailedRetry ||
		status == JobStatusUnknownRetry
}

func isFinalStatus(status JobStatus) bool {
	switch status {
	case JobStatusCompleted, JobStatusStopped, JobStatusUnscheduled, JobStatusUnknownStopped:
		return true
	default:
		return false
	}
}

func isValidTransition(current, target JobStatus) bool {
	if current == target {
		return true
	}

	switch current {
	case JobStatusInitialPending:
		return target == JobStatusUnscheduled
	case JobStatusRunning:
		return target == JobStatusCancelling ||
			target == JobStatusCompleted ||
			target == JobStatusFailedRetry ||
			target == JobStatusStopped ||
			target == JobStatusUnknownRetry ||
			target == JobStatusUnknownStopped
	case JobStatusCancelling:
		return target == JobStatusStopped || target == JobStatusUnknownStopped
	case JobStatusFailedRetry:
		return target == JobStatusStopped || target == JobStatusUnknownStopped
	case JobStatusUnknownRetry:
		return target == JobStatusRunning || target == JobStatusFailedRetry || target == JobStatusStopped || target == JobStatusUnknownStopped || target == JobStatusCompleted
	case JobStatusUnknownStopped:
		return target == JobStatusStopped || target == JobStatusInitialPending || target == JobStatusUnknownStopped
	case JobStatusStopped, JobStatusUnscheduled, JobStatusCompleted:
		return false
	default:
		return false
	}
}
