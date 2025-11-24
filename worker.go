package jobpool

import (
	"context"
	"fmt"
	"log/slog"
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
	logger     *slog.Logger
	stopCh     chan struct{}
	doneCh     chan struct{}
	assigneeID string
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewWorker creates a new worker.
// queue is the queue to process jobs from.
// processor is the function that will process each job.
// config contains worker configuration (TTL, cleanup interval, batch size).
// assigneeID is a unique identifier for this worker (used for job assignment tracking).
// logger is the logger instance for logging worker operations.
func NewWorker(queue Queue, processor JobProcessor, config *Config, assigneeID string, logger *slog.Logger) *Worker {
	ctx, cancel := context.WithCancel(context.Background())
	return &Worker{
		queue:      queue,
		processor:  processor,
		config:     config,
		logger:     logger,
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),
		assigneeID: assigneeID,
		ctx:        ctx,
		cancel:     cancel,
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
	go w.cleanupLoop(w.ctx)

	// Start processing loop
	go w.processLoop(w.ctx)

	return nil
}

// Stop stops the worker gracefully.
// It signals the worker to stop processing new jobs and waits for
// the current processing loop to finish before returning.
// Any jobs currently being processed will complete before the worker stops.
// This method blocks until the worker has fully stopped.
func (w *Worker) Stop() {
	// Cancel the context to stop all StreamJobs calls
	w.cancel()
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
	// Use StreamJobs for push-based job assignment
	// Create a channel for receiving jobs
	jobCh := make(chan []*Job, 1)

	// Start streaming jobs in a goroutine
	// Use the worker's context so it can be cancelled when Stop() is called
	// Note: StreamJobs will close the channel when it exits, so we don't close it here
	streamDone := make(chan struct{})
	go func() {
		defer close(streamDone)
		if err := w.queue.StreamJobs(w.ctx, w.assigneeID, nil, w.config.BatchSize, jobCh); err != nil {
			w.logger.Error("Failed to stream jobs", "error", err)
		}
	}()

	// Process jobs from the channel
	// Exit if context is cancelled or worker is stopped
	for {
		select {
		case <-w.stopCh:
			// Worker is stopping, wait for StreamJobs to finish
			<-streamDone
			return
		case <-w.ctx.Done():
			// Context cancelled, wait for StreamJobs to finish
			<-streamDone
			return
		case jobs, ok := <-jobCh:
			if !ok {
				// Channel closed by StreamJobs, it finished
				return
			}
			w.processJobs(ctx, jobs)
		}
	}
}

// processJobs processes a batch of jobs
func (w *Worker) processJobs(ctx context.Context, jobs []*Job) {
	// Prepare batch operations
	completeJobs := make(map[string][]byte)
	failJobs := make(map[string]string)

	// Process each job
	for _, job := range jobs {
		// Job is already in RUNNING state (assigned by StreamJobs/DequeueJobs)
		// Process the job
		result, err := w.processor(ctx, job)
		if err != nil {
			// Job failed - collect for batch FailJobs call
			failJobs[job.ID] = err.Error()
		} else {
			// Job succeeded - collect for batch CompleteJobs call
			completeJobs[job.ID] = result
		}
	}

	// Execute batch operations
	if len(failJobs) > 0 {
		if err := w.queue.FailJobs(ctx, failJobs); err != nil {
			w.logger.Error("Failed to mark jobs as failed", "error", err, "count", len(failJobs))
		}
	}

	if len(completeJobs) > 0 {
		if err := w.queue.CompleteJobs(ctx, completeJobs); err != nil {
			w.logger.Error("Failed to mark jobs as completed", "error", err, "count", len(completeJobs))
		}
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
		w.logger.Error("Failed to cleanup expired jobs", "error", err)
		return
	}
}
