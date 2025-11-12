package jobpool

import (
	"context"
	"fmt"
	"log"
	"time"
)

// JobProcessor is a function type that processes a job.
// It receives the job context and job data, and returns the result or an error.
// If an error is returned, the job will be marked as failed and retried.
type JobProcessor func(ctx context.Context, job *Job) ([]byte, error)

// Worker represents a background worker that processes jobs from a queue.
// It automatically dequeues jobs, processes them, and updates their status.
type Worker struct {
	queue      Queue
	processor  JobProcessor
	config     *Config
	stopCh     chan struct{}
	doneCh     chan struct{}
	assigneeID string
}

// NewWorker creates a new worker.
// queue is the queue to process jobs from.
// processor is the function that will process each job.
// config contains worker configuration (TTL, cleanup interval, batch size).
// assigneeID is a unique identifier for this worker (used for job assignment tracking).
func NewWorker(queue Queue, processor JobProcessor, config *Config, assigneeID string) *Worker {
	return &Worker{
		queue:      queue,
		processor:  processor,
		config:     config,
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
		assigneeID: assigneeID,
	}
}

// Start starts the worker and begins processing jobs from the queue.
// It performs the following operations:
//   - Resets all running jobs to pending state (for recovery after restart)
//   - Starts a background goroutine for periodic cleanup of expired jobs
//   - Starts a background goroutine for processing jobs from the queue
//
// The worker will continue processing jobs until Stop() is called.
// This method returns immediately after starting the background goroutines.
// If initialization fails (e.g., resetting running jobs), an error is returned.
func (w *Worker) Start(ctx context.Context) error {
	// Reset running jobs to pending on initialization
	if err := w.queue.ResetRunningJobs(ctx); err != nil {
		return fmt.Errorf("failed to reset running jobs: %w", err)
	}

	// Start cleanup goroutine
	go w.cleanupLoop(ctx)

	// Start processing loop
	go w.processLoop(ctx)

	return nil
}

// Stop stops the worker gracefully.
// It signals the worker to stop processing new jobs and waits for
// the current processing loop to finish before returning.
// Any jobs currently being processed will complete before the worker stops.
// This method blocks until the worker has fully stopped.
func (w *Worker) Stop() {
	close(w.stopCh)
	<-w.doneCh
}

// processLoop continuously processes jobs
func (w *Worker) processLoop(ctx context.Context) {
	defer close(w.doneCh)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.processBatch(ctx)
		}
	}
}

// processBatch processes a batch of jobs
func (w *Worker) processBatch(ctx context.Context) {
	// Dequeue jobs with assignee ID (no tag filtering for legacy worker)
	jobs, err := w.queue.DequeueJobs(ctx, w.assigneeID, nil, w.config.BatchSize)
	if err != nil {
		log.Printf("[Worker] Failed to dequeue jobs: %v", err)
		return
	}

	if len(jobs) == 0 {
		return
	}

	// Process each job
	for _, job := range jobs {
		w.processJob(ctx, job)
	}
}

// processJob processes a single job
func (w *Worker) processJob(ctx context.Context, job *Job) {
	// Update status to running
	if err := w.queue.UpdateJobStatus(ctx, job.ID, JobStatusRunning, nil, ""); err != nil {
		log.Printf("[Worker] Failed to update job status to running: %v", err)
		return
	}

	// Process the job
	result, err := w.processor(ctx, job)
	if err != nil {
		// Job failed - update status and reschedule
		if updateErr := w.queue.UpdateJobStatus(ctx, job.ID, JobStatusFailed, nil, err.Error()); updateErr != nil {
			log.Printf("[Worker] Failed to update job status to failed: %v", updateErr)
			return
		}

		// Reschedule to end of queue (increment retry count)
		if retryErr := w.queue.IncrementRetryCount(ctx, job.ID); retryErr != nil {
			log.Printf("[Worker] Failed to reschedule job: %v", retryErr)
		}
		return
	}

	// Job succeeded - update status to completed
	if err := w.queue.UpdateJobStatus(ctx, job.ID, JobStatusCompleted, result, ""); err != nil {
		log.Printf("[Worker] Failed to update job status to completed: %v", err)
		return
	}
}

// cleanupLoop periodically cleans up expired jobs
func (w *Worker) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(w.config.CleanupInterval)
	defer ticker.Stop()

	// Run cleanup immediately on start
	w.cleanup(ctx)

	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.cleanup(ctx)
		}
	}
}

// cleanup performs cleanup of expired jobs
func (w *Worker) cleanup(ctx context.Context) {
	if err := w.queue.CleanupExpiredJobs(ctx, w.config.TTL); err != nil {
		log.Printf("[Worker] Failed to cleanup expired jobs: %v", err)
		return
	}
}
