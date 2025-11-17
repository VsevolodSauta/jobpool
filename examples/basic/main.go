//go:build sqlite
// +build sqlite

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/VsevolodSauta/jobpool"
)

// newSQLiteBackend is a wrapper that will only compile with sqlite build tag
func newSQLiteBackend(path string, logger *slog.Logger) (jobpool.Backend, error) {
	return jobpool.NewSQLiteBackend(path, logger)
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	// Create backend (SQLite requires CGO, BadgerDB does not)
	// This example tries SQLite first, falls back to BadgerDB
	// For builds without CGO, use examples/basic-badger instead
	var backend jobpool.Backend
	var err error

	// Try SQLite first (requires CGO and sqlite build tag)
	// This will fail at compile time if built without -tags sqlite
	// In that case, use examples/basic-badger instead
	backend, err = newSQLiteBackend("./example.db", logger)
	if err != nil {
		// Fallback to BadgerDB if SQLite is not available
		backend, err = jobpool.NewBadgerBackend("./example-data", logger)
		if err != nil {
			logger.Error("Failed to create backend", "error", err)
			os.Exit(1)
		}
	}
	defer backend.Close()

	queue := jobpool.NewPoolQueue(backend, logger)
	ctx := context.Background()

	// Enqueue a job
	job := &jobpool.Job{
		ID:            "job-1",
		Status:        jobpool.JobStatusInitialPending,
		JobType:       "example_task",
		JobDefinition: []byte(`{"message": "Hello, World!"}`),
		Tags:          []string{"example", "demo"},
		CreatedAt:     time.Now(),
		RetryCount:    0,
	}

	jobID, err := queue.EnqueueJob(ctx, job)
	if err != nil {
		logger.Error("Failed to enqueue job", "error", err)
		os.Exit(1)
	}

	fmt.Printf("Enqueued job: %s\n", jobID)

	// Stream jobs (push-based)
	jobCh := make(chan []*jobpool.Job, 1)
	go func() {
		defer close(jobCh)
		if err := queue.StreamJobs(ctx, "worker-1", nil, 1, jobCh); err != nil {
			logger.Warn("Failed to stream jobs", "error", err)
		}
	}()

	// Process jobs from the channel
	for jobs := range jobCh {
		if len(jobs) > 0 {
			fmt.Printf("Dequeued job: %s (assigned to: %s)\n", jobs[0].ID, jobs[0].AssigneeID)

			// Process the job
			result := []byte(`{"status": "completed"}`)
			err = queue.CompleteJob(ctx, jobs[0].ID, result)
			if err != nil {
				logger.Error("Failed to complete job", "error", err)
				os.Exit(1)
			}

			fmt.Printf("Job completed successfully\n")
		}
	}

	// Get statistics
	stats, err := queue.GetJobStats(ctx, []string{"example"})
	if err != nil {
		logger.Error("Failed to get stats", "error", err)
		os.Exit(1)
	}

	fmt.Printf("Statistics: Total=%d, Completed=%d\n", stats.TotalJobs, stats.CompletedJobs)
}
