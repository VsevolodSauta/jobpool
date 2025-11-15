//go:build sqlite
// +build sqlite

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/VsevolodSauta/jobpool"
)

// newSQLiteBackend is a wrapper that will only compile with sqlite build tag
func newSQLiteBackend(path string) (jobpool.Backend, error) {
	return jobpool.NewSQLiteBackend(path)
}

func main() {
	// Create backend (SQLite requires CGO, BadgerDB does not)
	// This example tries SQLite first, falls back to BadgerDB
	// For builds without CGO, use examples/basic-badger instead
	var backend jobpool.Backend
	var err error

	// Try SQLite first (requires CGO and sqlite build tag)
	// This will fail at compile time if built without -tags sqlite
	// In that case, use examples/basic-badger instead
	backend, err = newSQLiteBackend("./example.db")
	if err != nil {
		// Fallback to BadgerDB if SQLite is not available
		backend, err = jobpool.NewBadgerBackend("./example-data")
		if err != nil {
			log.Fatalf("Failed to create backend: %v", err)
		}
	}
	defer backend.Close()

	queue := jobpool.NewPoolQueue(backend)
	ctx := context.Background()

	// Enqueue a job
	job := &jobpool.Job{
		ID:            "job-1",
		Status:        jobpool.JobStatusPending,
		JobType:       "example_task",
		JobDefinition: []byte(`{"message": "Hello, World!"}`),
		Tags:          []string{"example", "demo"},
		CreatedAt:     time.Now(),
		RetryCount:    0,
	}

	jobID, err := queue.EnqueueJob(ctx, job)
	if err != nil {
		log.Fatalf("Failed to enqueue job: %v", err)
	}

	fmt.Printf("Enqueued job: %s\n", jobID)

	// Stream jobs (push-based)
	jobCh := make(chan []*jobpool.Job, 1)
	go func() {
		defer close(jobCh)
		if err := queue.StreamJobs(ctx, "worker-1", nil, 1, jobCh); err != nil {
			log.Printf("Failed to stream jobs: %v", err)
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
				log.Fatalf("Failed to complete job: %v", err)
			}

			fmt.Printf("Job completed successfully\n")
		}
	}

	// Get statistics
	stats, err := queue.GetJobStats(ctx, []string{"example"})
	if err != nil {
		log.Fatalf("Failed to get stats: %v", err)
	}

	fmt.Printf("Statistics: Total=%d, Completed=%d\n", stats.TotalJobs, stats.CompletedJobs)
}
