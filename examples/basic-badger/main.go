package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/VsevolodSauta/jobpool"
)

func main() {
	// Create BadgerDB backend (no CGO required)
	backend, err := jobpool.NewBadgerBackend("./example-data")
	if err != nil {
		log.Fatalf("Failed to create backend: %v", err)
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
