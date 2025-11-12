//go:build sqlite
// +build sqlite

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/VsevolodSauta/jobpool"
)

// newSQLiteBackend is a wrapper that will only compile with sqlite build tag
func newSQLiteBackend(path string) (jobpool.Backend, error) {
	return jobpool.NewSQLiteBackend(path)
}

func main() {
	// Create backend (SQLite requires CGO, BadgerDB does not)
	// This example uses SQLite (requires -tags sqlite)
	// For builds without CGO, modify to use BadgerDB instead
	var backend jobpool.Backend
	var err error

	// Use SQLite (requires CGO and sqlite build tag)
	// This will fail at compile time if built without -tags sqlite
	backend, err = newSQLiteBackend("./worker-example.db")
	if err != nil {
		log.Fatalf("Failed to create backend: %v", err)
	}
	defer backend.Close()

	queue := jobpool.NewPoolQueue(backend)
	ctx := context.Background()

	// Enqueue some jobs
	for i := 0; i < 5; i++ {
		job := &jobpool.Job{
			ID:            fmt.Sprintf("job-%d", i),
			Status:        jobpool.JobStatusPending,
			JobType:       "process_task",
			JobDefinition: []byte(fmt.Sprintf(`{"task_id": %d}`, i)),
			Tags:          []string{"worker-demo"},
			CreatedAt:     time.Now(),
			RetryCount:    0,
		}

		_, err := queue.EnqueueJob(ctx, job)
		if err != nil {
			log.Fatalf("Failed to enqueue job: %v", err)
		}
	}

	// Create worker
	processor := func(ctx context.Context, job *jobpool.Job) ([]byte, error) {
		fmt.Printf("Processing job: %s (type: %s)\n", job.ID, job.JobType)
		// Simulate work
		time.Sleep(100 * time.Millisecond)
		return []byte(`{"processed": true}`), nil
	}

	config := &jobpool.Config{
		TTL:             30 * 24 * time.Hour,
		CleanupInterval: 1 * time.Hour,
		BatchSize:       10,
	}

	worker := jobpool.NewWorker(queue, processor, config, "example-worker")

	// Start worker
	if err := worker.Start(ctx); err != nil {
		log.Fatalf("Failed to start worker: %v", err)
	}

	fmt.Println("Worker started. Press Ctrl+C to stop...")

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nStopping worker...")
	worker.Stop()
	fmt.Println("Worker stopped")
}
