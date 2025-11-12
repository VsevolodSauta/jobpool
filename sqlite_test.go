//go:build sqlite
// +build sqlite

package jobpool_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/VsevolodSauta/jobpool"
)

func TestSQLiteBackend_EnqueueDequeue(t *testing.T) {
	// Create temporary database
	tmpFile, err := os.CreateTemp("", "test_jobpool_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	backend, err := jobpool.NewSQLiteBackend(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to create SQLite backend: %v", err)
	}
	defer backend.Close()

	queue := jobpool.NewPoolQueue(backend)
	ctx := context.Background()

	// Enqueue a job
	job := &jobpool.Job{
		ID:            "job-1",
		Status:        jobpool.JobStatusPending,
		JobType:       "test",
		JobDefinition: []byte("test data"),
		Tags:          []string{"tag1"},
		CreatedAt:     time.Now(),
		RetryCount:    0,
	}

	jobID, err := queue.EnqueueJob(ctx, job)
	if err != nil {
		t.Fatalf("Failed to enqueue job: %v", err)
	}
	if jobID != "job-1" {
		t.Errorf("Expected job ID 'job-1', got '%s'", jobID)
	}

	// Dequeue the job
	assigneeID := "worker-1"
	jobs, err := queue.DequeueJobs(ctx, assigneeID, nil, 10)
	if err != nil {
		t.Fatalf("Failed to dequeue jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("Expected 1 job, got %d", len(jobs))
	}

	dequeuedJob := jobs[0]
	if dequeuedJob.ID != "job-1" {
		t.Errorf("Expected job ID 'job-1', got '%s'", dequeuedJob.ID)
	}
	if dequeuedJob.AssigneeID != assigneeID {
		t.Errorf("Expected assignee ID '%s', got '%s'", assigneeID, dequeuedJob.AssigneeID)
	}
	if dequeuedJob.AssignedAt == nil {
		t.Error("Expected AssignedAt to be set")
	}
}

func TestSQLiteBackend_AssigneeTracking(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_jobpool_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	backend, err := jobpool.NewSQLiteBackend(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to create SQLite backend: %v", err)
	}
	defer backend.Close()

	queue := jobpool.NewPoolQueue(backend)
	ctx := context.Background()

	// Enqueue jobs
	for i := 0; i < 5; i++ {
		job := &jobpool.Job{
			ID:            fmt.Sprintf("job-%d", i),
			Status:        jobpool.JobStatusPending,
			JobType:       "test",
			JobDefinition: []byte("test"),
			Tags:          []string{"tag1"},
			CreatedAt:     time.Now(),
			RetryCount:    0,
		}
		_, err := queue.EnqueueJob(ctx, job)
		if err != nil {
			t.Fatalf("Failed to enqueue job: %v", err)
		}
	}

	// Assign jobs to worker-1
	jobs1, err := queue.DequeueJobs(ctx, "worker-1", nil, 3)
	if err != nil {
		t.Fatalf("Failed to dequeue jobs: %v", err)
	}
	if len(jobs1) != 3 {
		t.Fatalf("Expected 3 jobs, got %d", len(jobs1))
	}

	// Assign remaining jobs to worker-2
	jobs2, err := queue.DequeueJobs(ctx, "worker-2", nil, 10)
	if err != nil {
		t.Fatalf("Failed to dequeue jobs: %v", err)
	}
	if len(jobs2) != 2 {
		t.Fatalf("Expected 2 jobs, got %d", len(jobs2))
	}

	// Return worker-1's jobs to pending
	err = queue.ReturnJobsToPending(ctx, "worker-1")
	if err != nil {
		t.Fatalf("Failed to return jobs to pending: %v", err)
	}

	// Verify worker-1's jobs are back to pending
	jobs3, err := queue.DequeueJobs(ctx, "worker-3", nil, 10)
	if err != nil {
		t.Fatalf("Failed to dequeue jobs: %v", err)
	}
	if len(jobs3) != 3 {
		t.Fatalf("Expected 3 jobs (returned from worker-1), got %d", len(jobs3))
	}
}

func TestSQLiteBackend_JobStats(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_jobpool_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	backend, err := jobpool.NewSQLiteBackend(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to create SQLite backend: %v", err)
	}
	defer backend.Close()

	queue := jobpool.NewPoolQueue(backend)
	ctx := context.Background()

	// Enqueue jobs with different tags
	jobs := []*jobpool.Job{
		{ID: "job-1", Status: jobpool.JobStatusPending, JobType: "test", JobDefinition: []byte("test"), Tags: []string{"tag1", "tag2"}, CreatedAt: time.Now(), RetryCount: 0},
		{ID: "job-2", Status: jobpool.JobStatusPending, JobType: "test", JobDefinition: []byte("test"), Tags: []string{"tag1", "tag2"}, CreatedAt: time.Now(), RetryCount: 0},
		{ID: "job-3", Status: jobpool.JobStatusPending, JobType: "test", JobDefinition: []byte("test"), Tags: []string{"tag1"}, CreatedAt: time.Now(), RetryCount: 0},
	}

	_, err = queue.EnqueueJobs(ctx, jobs)
	if err != nil {
		t.Fatalf("Failed to enqueue jobs: %v", err)
	}

	// Get stats for jobs with both tags (AND logic)
	stats, err := queue.GetJobStats(ctx, []string{"tag1", "tag2"})
	if err != nil {
		t.Fatalf("Failed to get job stats: %v", err)
	}
	if stats.TotalJobs != 2 {
		t.Errorf("Expected 2 jobs with both tags, got %d", stats.TotalJobs)
	}
	if stats.PendingJobs != 2 {
		t.Errorf("Expected 2 pending jobs, got %d", stats.PendingJobs)
	}
}

func TestSQLiteBackend_RetryCount(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_jobpool_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	backend, err := jobpool.NewSQLiteBackend(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to create SQLite backend: %v", err)
	}
	defer backend.Close()

	queue := jobpool.NewPoolQueue(backend)
	ctx := context.Background()

	// Enqueue a job
	job := &jobpool.Job{
		ID:            "job-1",
		Status:        jobpool.JobStatusPending,
		JobType:       "test",
		JobDefinition: []byte("test"),
		Tags:          []string{"tag1"},
		CreatedAt:     time.Now(),
		RetryCount:    0,
	}
	_, err = queue.EnqueueJob(ctx, job)
	if err != nil {
		t.Fatalf("Failed to enqueue job: %v", err)
	}

	// Dequeue and mark as failed
	jobs, err := queue.DequeueJobs(ctx, "worker-1", nil, 1)
	if err != nil {
		t.Fatalf("Failed to dequeue job: %v", err)
	}
	err = queue.UpdateJobStatus(ctx, jobs[0].ID, jobpool.JobStatusFailed, nil, "error")
	if err != nil {
		t.Fatalf("Failed to update job status: %v", err)
	}

	// Increment retry count
	err = queue.IncrementRetryCount(ctx, jobs[0].ID)
	if err != nil {
		t.Fatalf("Failed to increment retry count: %v", err)
	}

	// Job should be back to pending
	stats, err := queue.GetJobStats(ctx, []string{"tag1"})
	if err != nil {
		t.Fatalf("Failed to get job stats: %v", err)
	}
	if stats.PendingJobs != 1 {
		t.Errorf("Expected 1 pending job after retry, got %d", stats.PendingJobs)
	}
}
