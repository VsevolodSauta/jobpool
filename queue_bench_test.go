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

func BenchmarkEnqueueJob(b *testing.B) {
	tmpFile, err := os.CreateTemp("", "bench_jobpool_*.db")
	if err != nil {
		b.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	backend, err := jobpool.NewSQLiteBackend(tmpFile.Name())
	if err != nil {
		b.Fatalf("Failed to create SQLite backend: %v", err)
	}
	defer backend.Close()

	queue := jobpool.NewPoolQueue(backend)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		job := &jobpool.Job{
			ID:            fmt.Sprintf("job-%d", i),
			Status:        jobpool.JobStatusPending,
			JobType:       "benchmark",
			JobDefinition: []byte("test data"),
			Tags:          []string{"bench"},
			CreatedAt:     time.Now(),
		}
		_, err := queue.EnqueueJob(ctx, job)
		if err != nil {
			b.Fatalf("Failed to enqueue job: %v", err)
		}
	}
}

func BenchmarkEnqueueJobs_Batch10(b *testing.B) {
	tmpFile, err := os.CreateTemp("", "bench_jobpool_*.db")
	if err != nil {
		b.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	backend, err := jobpool.NewSQLiteBackend(tmpFile.Name())
	if err != nil {
		b.Fatalf("Failed to create SQLite backend: %v", err)
	}
	defer backend.Close()

	queue := jobpool.NewPoolQueue(backend)
	ctx := context.Background()

	batchSize := 10
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		jobs := make([]*jobpool.Job, batchSize)
		for j := 0; j < batchSize; j++ {
			jobs[j] = &jobpool.Job{
				ID:            fmt.Sprintf("job-%d", i*batchSize+j),
				Status:        jobpool.JobStatusPending,
				JobType:       "benchmark",
				JobDefinition: []byte("test data"),
				Tags:          []string{"bench"},
				CreatedAt:     time.Now(),
			}
		}
		_, err := queue.EnqueueJobs(ctx, jobs)
		if err != nil {
			b.Fatalf("Failed to enqueue jobs: %v", err)
		}
	}
}

func BenchmarkDequeueJobs(b *testing.B) {
	tmpFile, err := os.CreateTemp("", "bench_jobpool_*.db")
	if err != nil {
		b.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	backend, err := jobpool.NewSQLiteBackend(tmpFile.Name())
	if err != nil {
		b.Fatalf("Failed to create SQLite backend: %v", err)
	}
	defer backend.Close()

	queue := jobpool.NewPoolQueue(backend)
	ctx := context.Background()

	// Pre-populate with jobs
	for i := 0; i < 1000; i++ {
		job := &jobpool.Job{
			ID:            fmt.Sprintf("job-%d", i),
			Status:        jobpool.JobStatusPending,
			JobType:       "benchmark",
			JobDefinition: []byte("test data"),
			Tags:          []string{"bench"},
			CreatedAt:     time.Now(),
		}
		_, err := queue.EnqueueJob(ctx, job)
		if err != nil {
			b.Fatalf("Failed to enqueue job: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Use backend directly for benchmarking
		_, err := backend.DequeueJobs(ctx, "worker-1", nil, 10)
		if err != nil {
			b.Fatalf("Failed to dequeue jobs: %v", err)
		}
		// Re-enqueue to keep benchmark going
		if i%100 == 0 {
			job := &jobpool.Job{
				ID:            fmt.Sprintf("job-new-%d", i),
				Status:        jobpool.JobStatusPending,
				JobType:       "benchmark",
				JobDefinition: []byte("test data"),
				Tags:          []string{"bench"},
				CreatedAt:     time.Now(),
			}
			queue.EnqueueJob(ctx, job)
		}
	}
}

func BenchmarkCompleteJob(b *testing.B) {
	tmpFile, err := os.CreateTemp("", "bench_jobpool_*.db")
	if err != nil {
		b.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	backend, err := jobpool.NewSQLiteBackend(tmpFile.Name())
	if err != nil {
		b.Fatalf("Failed to create SQLite backend: %v", err)
	}
	defer backend.Close()

	queue := jobpool.NewPoolQueue(backend)
	ctx := context.Background()

	// Pre-populate with jobs and assign them
	jobIDs := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		job := &jobpool.Job{
			ID:            fmt.Sprintf("job-%d", i),
			Status:        jobpool.JobStatusPending,
			JobType:       "benchmark",
			JobDefinition: []byte("test data"),
			Tags:          []string{"bench"},
			CreatedAt:     time.Now(),
		}
		jobID, err := queue.EnqueueJob(ctx, job)
		if err != nil {
			b.Fatalf("Failed to enqueue job: %v", err)
		}
		jobIDs[i] = jobID
		// Assign job to make it RUNNING
		_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
		if err != nil {
			b.Fatalf("Failed to assign job: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := queue.CompleteJob(ctx, jobIDs[i], []byte("result"))
		if err != nil {
			b.Fatalf("Failed to complete job: %v", err)
		}
	}
}

func BenchmarkGetJobStats(b *testing.B) {
	tmpFile, err := os.CreateTemp("", "bench_jobpool_*.db")
	if err != nil {
		b.Fatalf("Failed to create temp file: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	backend, err := jobpool.NewSQLiteBackend(tmpFile.Name())
	if err != nil {
		b.Fatalf("Failed to create SQLite backend: %v", err)
	}
	defer backend.Close()

	queue := jobpool.NewPoolQueue(backend)
	ctx := context.Background()

	// Pre-populate with jobs
	for i := 0; i < 1000; i++ {
		job := &jobpool.Job{
			ID:            fmt.Sprintf("job-stats-%d", i),
			Status:        jobpool.JobStatusPending,
			JobType:       "benchmark",
			JobDefinition: []byte("test data"),
			Tags:          []string{"bench", "tag1"},
			CreatedAt:     time.Now(),
		}
		_, err := queue.EnqueueJob(ctx, job)
		if err != nil {
			b.Fatalf("Failed to enqueue job: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := queue.GetJobStats(ctx, []string{"bench"})
		if err != nil {
			b.Fatalf("Failed to get job stats: %v", err)
		}
	}
}
