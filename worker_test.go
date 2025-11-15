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

func TestWorker_ProcessJobs(t *testing.T) {
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
	_, err = queue.EnqueueJob(ctx, job)
	if err != nil {
		t.Fatalf("Failed to enqueue job: %v", err)
	}

	// Create worker
	processedJobs := make(chan string, 10)
	processor := func(ctx context.Context, job *jobpool.Job) ([]byte, error) {
		processedJobs <- job.ID
		return []byte("result"), nil
	}

	config := &jobpool.Config{
		TTL:             30 * 24 * time.Hour,
		CleanupInterval: 1 * time.Hour,
		BatchSize:       10,
	}

	assigneeID := "test-worker"
	worker := jobpool.NewWorker(queue, processor, config, assigneeID)

	// Start worker
	if err := worker.Start(ctx); err != nil {
		t.Fatalf("Failed to start worker: %v", err)
	}

	// Wait for job to be processed
	select {
	case jobID := <-processedJobs:
		if jobID != "job-1" {
			t.Errorf("Expected job ID 'job-1', got '%s'", jobID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Job was not processed within timeout")
	}

	// Give the worker time to complete the job (CompleteJob call)
	time.Sleep(100 * time.Millisecond)

	// Verify job is completed before stopping worker
	stats, err := queue.GetJobStats(ctx, []string{"tag1"})
	if err != nil {
		t.Fatalf("Failed to get job stats: %v", err)
	}
	if stats.CompletedJobs != 1 {
		t.Errorf("Expected 1 completed job, got %d", stats.CompletedJobs)
	}

	// Stop worker
	worker.Stop()
}

func TestWorker_RetryFailedJobs(t *testing.T) {
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
	_, err = queue.EnqueueJob(ctx, job)
	if err != nil {
		t.Fatalf("Failed to enqueue job: %v", err)
	}

	// Create worker that fails on first attempt, succeeds on second
	attempts := 0
	processor := func(ctx context.Context, job *jobpool.Job) ([]byte, error) {
		attempts++
		if attempts == 1 {
			return nil, fmt.Errorf("first attempt failed")
		}
		return []byte("result"), nil
	}

	config := &jobpool.Config{
		TTL:             30 * 24 * time.Hour,
		CleanupInterval: 1 * time.Hour,
		BatchSize:       10,
	}

	assigneeID := "test-worker"
	worker := jobpool.NewWorker(queue, processor, config, assigneeID)

	// Start worker
	if err := worker.Start(ctx); err != nil {
		t.Fatalf("Failed to start worker: %v", err)
	}

	// Wait for job to be processed (will fail first, then retry and succeed)
	time.Sleep(3 * time.Second)

	// Stop worker
	worker.Stop()

	// Verify job is completed after retry
	stats, err := queue.GetJobStats(ctx, []string{"tag1"})
	if err != nil {
		t.Fatalf("Failed to get job stats: %v", err)
	}
	if stats.CompletedJobs != 1 {
		t.Errorf("Expected 1 completed job after retry, got %d", stats.CompletedJobs)
	}
	if attempts < 2 {
		t.Errorf("Expected at least 2 attempts (initial + retry), got %d", attempts)
	}
}
