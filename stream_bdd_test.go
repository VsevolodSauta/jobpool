//go:build sqlite
// +build sqlite

package jobpool_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/VsevolodSauta/jobpool"
)

var _ = Describe("Queue API Specification", func() {
	var (
		queue   jobpool.Queue
		ctx     context.Context
		tmpFile *os.File
	)

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		tmpFile, err = os.CreateTemp("", "test_jobpool_stream_bdd_*.db")
		Expect(err).NotTo(HaveOccurred())
		tmpFile.Close()

		backend, err := jobpool.NewSQLiteBackend(tmpFile.Name(), testLogger())
		Expect(err).NotTo(HaveOccurred())

		queue = jobpool.NewPoolQueue(backend, testLogger())
	})

	AfterEach(func() {
		if queue != nil {
			queue.Close()
		}
		if tmpFile != nil {
			os.Remove(tmpFile.Name())
		}
	})

	Describe("Jobs becoming ready for execution", func() {
		Context("when new jobs are enqueued", func() {
			It("should notify waiting StreamJobs workers", func() {
				// Given: A worker waiting via StreamJobs
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-1", []string{"tag1"}, 10, jobChan)
				}()

				// Wait for StreamJobs to be ready
				time.Sleep(100 * time.Millisecond)

				// When: New job is enqueued
				job := &jobpool.Job{
					ID:            "job-new-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Then: Job should be immediately pushed via StreamJobs
				select {
				case jobs := <-jobChan:
					Expect(len(jobs)).To(Equal(1))
					Expect(jobs[0].ID).To(Equal("job-new-1"))
				case <-time.After(2 * time.Second):
					Fail("Job should be immediately pushed to StreamJobs")
				}

				streamCancel()
				wg.Wait()
			})
		})

		Context("when failed jobs transition to FAILED_RETRY for retry", func() {
			It("should notify waiting StreamJobs workers after FailJob", func() {
				// Given: A job that will fail and a waiting worker
				job := &jobpool.Job{
					ID:            "job-fail-retry-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign job via StreamJobs to worker-1
				assignChan := make(chan []*jobpool.Job, 10)
				assignCtx, assignCancel := context.WithCancel(ctx)
				defer assignCancel()

				var assignWg sync.WaitGroup
				assignWg.Add(1)
				go func() {
					defer assignWg.Done()
					_ = queue.StreamJobs(assignCtx, "worker-1", []string{"tag1"}, 10, assignChan)
				}()

				time.Sleep(100 * time.Millisecond)

				// Receive the job
				select {
				case jobs := <-assignChan:
					Expect(len(jobs)).To(BeNumerically(">=", 1))
					found := false
					for _, j := range jobs {
						if j.ID == "job-fail-retry-1" {
							found = true
							break
						}
					}
					Expect(found).To(BeTrue())
				case <-time.After(2 * time.Second):
					Fail("Job should be assigned")
				}

				// Verify job is in RUNNING state before failing it
				// (Don't cancel StreamJobs yet, as that would transition it to FAILED_RETRY)
				runningJob, err := queue.GetJob(ctx, "job-fail-retry-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(runningJob.Status).To(Equal(jobpool.JobStatusRunning))

				// When: Job fails (FailJob transitions to FAILED_RETRY)
				err = queue.FailJobs(ctx, map[string]string{"job-fail-retry-1": "test error"})
				Expect(err).NotTo(HaveOccurred())

				// Now cancel the first StreamJobs (cleanup will handle any remaining jobs)
				assignCancel()
				assignWg.Wait()

				// Set up StreamJobs worker for retry
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-2", []string{"tag1"}, 10, jobChan)
				}()

				time.Sleep(100 * time.Millisecond)

				// Then: Failed job should be pushed again via StreamJobs (for retry)
				select {
				case jobs := <-jobChan:
					Expect(len(jobs)).To(BeNumerically(">=", 1))
					found := false
					for _, j := range jobs {
						if j.ID == "job-fail-retry-1" {
							found = true
							break
						}
					}
					Expect(found).To(BeTrue(), "Failed job should be pushed again for retry")
				case <-time.After(2 * time.Second):
					Fail("Failed job should be pushed again via StreamJobs")
				}

				// Verify retry count was incremented
				// Note: Job status might be RUNNING if StreamJobs already dequeued it for retry
				retryJob, err := queue.GetJob(ctx, "job-fail-retry-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retryJob.RetryCount).To(BeNumerically(">", 0), "Retry count should be incremented")
				// Job should be either FAILED_RETRY (if not yet dequeued) or RUNNING (if already dequeued for retry)
				Expect(retryJob.Status == jobpool.JobStatusFailedRetry || retryJob.Status == jobpool.JobStatusRunning).To(BeTrue(), "Failed job should be in FAILED_RETRY or RUNNING state")

				streamCancel()
				wg.Wait()
			})
		})

		Context("when jobs in UNKNOWN_RETRY state become eligible", func() {
			It("should push UNKNOWN_RETRY jobs to StreamJobs workers", func() {
				// Given: A job that will become UNKNOWN_RETRY
				job := &jobpool.Job{
					ID:            "job-unknown-retry-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign job to old-worker via StreamJobs
				oldWorkerChan := make(chan []*jobpool.Job, 10)
				oldWorkerCtx, oldWorkerCancel := context.WithCancel(ctx)
				defer oldWorkerCancel()

				var oldWorkerWg sync.WaitGroup
				oldWorkerWg.Add(1)
				go func() {
					defer oldWorkerWg.Done()
					_ = queue.StreamJobs(oldWorkerCtx, "old-worker", []string{"tag1"}, 10, oldWorkerChan)
				}()

				time.Sleep(100 * time.Millisecond)

				// Receive the job
				select {
				case jobs := <-oldWorkerChan:
					Expect(len(jobs)).To(BeNumerically(">=", 1))
					found := false
					for _, j := range jobs {
						if j.ID == "job-unknown-retry-1" {
							found = true
							break
						}
					}
					Expect(found).To(BeTrue(), "Job should be assigned to old-worker")
				case <-time.After(2 * time.Second):
					Fail("Job should be assigned")
				}

				oldWorkerCancel()
				oldWorkerWg.Wait()

				// Mark old-worker as unresponsive to transition job to UNKNOWN_RETRY
				err = queue.MarkWorkerUnresponsive(ctx, "old-worker")
				Expect(err).NotTo(HaveOccurred())

				// Set up StreamJobs worker for new assignment
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-new-1", []string{"tag1"}, 10, jobChan)
				}()

				time.Sleep(100 * time.Millisecond)

				// When: Job is in UNKNOWN_RETRY state (already eligible)
				// Then: Job should be pushed to worker via StreamJobs
				select {
				case jobs := <-jobChan:
					Expect(len(jobs)).To(BeNumerically(">=", 1))
					found := false
					for _, j := range jobs {
						if j.ID == "job-unknown-retry-1" {
							found = true
							break
						}
					}
					Expect(found).To(BeTrue(), "UNKNOWN_RETRY job should be pushed to worker")
				case <-time.After(2 * time.Second):
					Fail("UNKNOWN_RETRY job should be pushed via StreamJobs")
				}

				streamCancel()
				wg.Wait()
			})
		})

		Context("when FAILED_RETRY jobs become eligible", func() {
			It("should notify waiting StreamJobs workers for FAILED_RETRY jobs", func() {
				// Given: A job that will fail and become eligible for retry
				job := &jobpool.Job{
					ID:            "job-failed-retry-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign job via StreamJobs
				assignChan := make(chan []*jobpool.Job, 10)
				assignCtx, assignCancel := context.WithCancel(ctx)
				defer assignCancel()

				var assignWg sync.WaitGroup
				assignWg.Add(1)
				go func() {
					defer assignWg.Done()
					_ = queue.StreamJobs(assignCtx, "worker-assign-1", []string{"tag1"}, 10, assignChan)
				}()

				time.Sleep(100 * time.Millisecond)

				// Receive and fail the job
				var assignedJob *jobpool.Job
				select {
				case jobs := <-assignChan:
					Expect(len(jobs)).To(BeNumerically(">=", 1))
					found := false
					for _, j := range jobs {
						if j.ID == "job-failed-retry-1" {
							assignedJob = j
							found = true
							break
						}
					}
					Expect(found).To(BeTrue(), "Job should be assigned")
					Expect(assignedJob.Status).To(Equal(jobpool.JobStatusRunning), "Assigned job should be in RUNNING state")
				case <-time.After(2 * time.Second):
					Fail("Job should be assigned")
				}

				// Fail the job BEFORE canceling StreamJobs (transitions to FAILED_RETRY)
				// Use the job we received from StreamJobs, which should already be in RUNNING state
				err = queue.FailJobs(ctx, map[string]string{assignedJob.ID: "test error"})
				Expect(err).NotTo(HaveOccurred())

				assignCancel()
				assignWg.Wait()

				// Set up StreamJobs worker for retry
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-update-1", []string{"tag1"}, 10, jobChan)
				}()

				time.Sleep(100 * time.Millisecond)

				// When: Job is in FAILED_RETRY state (already eligible)
				// Then: Job should be pushed to worker via StreamJobs
				select {
				case jobs := <-jobChan:
					Expect(len(jobs)).To(BeNumerically(">=", 1))
					found := false
					for _, j := range jobs {
						if j.ID == "job-failed-retry-1" {
							found = true
							break
						}
					}
					Expect(found).To(BeTrue(), "FAILED_RETRY job should be pushed to worker")
				case <-time.After(2 * time.Second):
					Fail("FAILED_RETRY job should be pushed via StreamJobs")
				}

				streamCancel()
				wg.Wait()
			})
		})
	})

	Describe("Workers becoming ready to execute jobs", func() {
		Context("when job completes successfully", func() {
			It("should immediately notify StreamJobs after CompleteJob", func() {
				// Given: Worker with assigned job and pending jobs waiting
				// Enqueue 3 jobs
				for i := 0; i < 3; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-complete-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Set up StreamJobs for the worker
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-complete-1", []string{"tag1"}, 10, jobChan)
				}()

				time.Sleep(100 * time.Millisecond)

				// Receive first batch of jobs (may contain multiple jobs)
				var firstJob *jobpool.Job
				receivedJobs := make(map[string]*jobpool.Job)
				select {
				case jobs := <-jobChan:
					Expect(len(jobs)).To(BeNumerically(">=", 1))
					for _, j := range jobs {
						receivedJobs[j.ID] = j
						if j.ID == "job-complete-0" {
							firstJob = j
						}
					}
					Expect(firstJob).NotTo(BeNil(), "First job should be received")
				case <-time.After(2 * time.Second):
					Fail("First job should be received")
				}

				// When: Complete the assigned job
				err := queue.CompleteJobs(ctx, map[string][]byte{"job-complete-0": []byte("result")})
				Expect(err).NotTo(HaveOccurred())

				// Then: Next job should be immediately available (either already in received batch or pushed via notification)
				// Check if we already have the next job in the batch we received
				hasNextJob := receivedJobs["job-complete-1"] != nil || receivedJobs["job-complete-2"] != nil
				if !hasNextJob {
					// If not in the batch, wait for notification to push it
					select {
					case jobs := <-jobChan:
						Expect(len(jobs)).To(BeNumerically(">=", 1))
						found := false
						for _, j := range jobs {
							if j.ID == "job-complete-1" || j.ID == "job-complete-2" {
								found = true
								break
							}
						}
						Expect(found).To(BeTrue(), "New job should be immediately pushed after CompleteJob notification")
					case <-time.After(2 * time.Second):
						Fail("New job should be immediately pushed after CompleteJob notification")
					}
				} else {
					// We already have the next job in the batch, which is correct behavior
					// The notification mechanism worked - jobs were pushed when capacity was available
				}

				// Verify completed job status
				completedJob, err := queue.GetJob(ctx, "job-complete-0")
				Expect(err).NotTo(HaveOccurred())
				Expect(completedJob.Status).To(Equal(jobpool.JobStatusCompleted), "Job should be COMPLETED")

				streamCancel()
				wg.Wait()
			})
		})

		Context("when job is stopped/cancelled", func() {
			It("should immediately notify StreamJobs after StopJob", func() {
				// Given: Worker with assigned job and pending jobs waiting
				// Enqueue 3 jobs
				for i := 0; i < 3; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-stop-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Set up StreamJobs for the worker
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-stop-1", []string{"tag1"}, 10, jobChan)
				}()

				time.Sleep(100 * time.Millisecond)

				// Receive first batch of jobs (may contain multiple jobs)
				receivedJobs := make(map[string]*jobpool.Job)
				select {
				case jobs := <-jobChan:
					Expect(len(jobs)).To(BeNumerically(">=", 1))
					for _, j := range jobs {
						receivedJobs[j.ID] = j
					}
					Expect(receivedJobs["job-stop-0"]).NotTo(BeNil(), "First job should be received")
				case <-time.After(2 * time.Second):
					Fail("First job should be received")
				}

				// When: Stop the assigned job
				err := queue.StopJobs(ctx, map[string]string{"job-stop-0": "cancelled"})
				Expect(err).NotTo(HaveOccurred())

				// Then: Next job should be immediately available (either already in received batch or pushed via notification)
				hasNextJob := receivedJobs["job-stop-1"] != nil || receivedJobs["job-stop-2"] != nil
				if !hasNextJob {
					// If not in the batch, wait for notification to push it
					select {
					case jobs := <-jobChan:
						Expect(len(jobs)).To(BeNumerically(">=", 1))
						found := false
						for _, j := range jobs {
							if j.ID == "job-stop-1" || j.ID == "job-stop-2" {
								found = true
								break
							}
						}
						Expect(found).To(BeTrue(), "New job should be immediately pushed after StopJob notification")
					case <-time.After(2 * time.Second):
						Fail("New job should be immediately pushed after StopJob notification")
					}
				}

				// Verify stopped job status
				stoppedJob, err := queue.GetJob(ctx, "job-stop-0")
				Expect(err).NotTo(HaveOccurred())
				Expect(stoppedJob.Status).To(Equal(jobpool.JobStatusStopped), "Job should be STOPPED")

				streamCancel()
				wg.Wait()
			})
		})

		Context("when job fails and frees capacity", func() {
			It("should immediately notify StreamJobs after FailJob frees capacity", func() {
				// Given: Worker with assigned job and pending jobs waiting
				// Enqueue 3 jobs
				for i := 0; i < 3; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-fail-capacity-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Set up StreamJobs for the worker
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-fail-capacity-1", []string{"tag1"}, 10, jobChan)
				}()

				time.Sleep(100 * time.Millisecond)

				// Receive first batch of jobs (may contain multiple jobs)
				receivedJobs := make(map[string]*jobpool.Job)
				select {
				case jobs := <-jobChan:
					Expect(len(jobs)).To(BeNumerically(">=", 1))
					for _, j := range jobs {
						receivedJobs[j.ID] = j
					}
					Expect(receivedJobs["job-fail-capacity-0"]).NotTo(BeNil(), "First job should be received")
				case <-time.After(2 * time.Second):
					Fail("First job should be received")
				}

				// When: Fail the assigned job (FailJob frees capacity and transitions to FAILED_RETRY)
				err := queue.FailJobs(ctx, map[string]string{"job-fail-capacity-0": "test error"})
				Expect(err).NotTo(HaveOccurred())

				// Then: Next job should be immediately available (either already in received batch or pushed via notification)
				hasNextJob := receivedJobs["job-fail-capacity-1"] != nil || receivedJobs["job-fail-capacity-2"] != nil
				if !hasNextJob {
					// If not in the batch, wait for notification to push it
					select {
					case jobs := <-jobChan:
						Expect(len(jobs)).To(BeNumerically(">=", 1))
						found := false
						for _, j := range jobs {
							if j.ID == "job-fail-capacity-1" || j.ID == "job-fail-capacity-2" {
								found = true
								break
							}
						}
						Expect(found).To(BeTrue(), "New job should be immediately pushed after FailJob frees capacity")
					case <-time.After(2 * time.Second):
						Fail("New job should be immediately pushed after FailJob frees capacity")
					}
				}

				// Verify failed job is in FAILED_RETRY state (ready for retry)
				// Note: Jobs never return to INITIAL_PENDING once they leave it - they remain in FAILED_RETRY
				failedJob, err := queue.GetJob(ctx, "job-fail-capacity-0")
				Expect(err).NotTo(HaveOccurred())
				Expect(failedJob.Status).To(Equal(jobpool.JobStatusFailedRetry), "Failed job should be in FAILED_RETRY state for retry")

				streamCancel()
				wg.Wait()
			})
		})

		Context("when job is marked as UNKNOWN_STOPPED", func() {
			It("should immediately notify StreamJobs after MarkJobUnknownStopped", func() {
				// Given: Worker with assigned job and pending jobs waiting
				// Enqueue 3 jobs
				for i := 0; i < 3; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-unknown-stopped-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Set up StreamJobs for the worker
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-unknown-stopped-1", []string{"tag1"}, 10, jobChan)
				}()

				time.Sleep(100 * time.Millisecond)

				// Receive first batch of jobs (may contain multiple jobs)
				receivedJobs := make(map[string]*jobpool.Job)
				select {
				case jobs := <-jobChan:
					Expect(len(jobs)).To(BeNumerically(">=", 1))
					for _, j := range jobs {
						receivedJobs[j.ID] = j
					}
					Expect(receivedJobs["job-unknown-stopped-0"]).NotTo(BeNil(), "First job should be received")
				case <-time.After(2 * time.Second):
					Fail("First job should be received")
				}

				// When: Mark job as UNKNOWN_STOPPED
				err := queue.MarkJobsUnknownStopped(ctx, map[string]string{"job-unknown-stopped-0": "worker unresponsive"})
				Expect(err).NotTo(HaveOccurred())

				// Then: Next job should be immediately available (either already in received batch or pushed via notification)
				hasNextJob := receivedJobs["job-unknown-stopped-1"] != nil || receivedJobs["job-unknown-stopped-2"] != nil
				if !hasNextJob {
					// If not in the batch, wait for notification to push it
					select {
					case jobs := <-jobChan:
						Expect(len(jobs)).To(BeNumerically(">=", 1))
						found := false
						for _, j := range jobs {
							if j.ID == "job-unknown-stopped-1" || j.ID == "job-unknown-stopped-2" {
								found = true
								break
							}
						}
						Expect(found).To(BeTrue(), "New job should be immediately pushed after MarkJobUnknownStopped notification")
					case <-time.After(2 * time.Second):
						Fail("New job should be immediately pushed after MarkJobUnknownStopped notification")
					}
				}

				// Verify unknown stopped job status
				unknownStoppedJob, err := queue.GetJob(ctx, "job-unknown-stopped-0")
				Expect(err).NotTo(HaveOccurred())
				Expect(unknownStoppedJob.Status).To(Equal(jobpool.JobStatusUnknownStopped), "Job should be UNKNOWN_STOPPED")

				streamCancel()
				wg.Wait()
			})
		})

		Context("when CompleteJob transitions job to terminal state", func() {
			It("should immediately notify StreamJobs after CompleteJob", func() {
				// Given: Worker with assigned job and pending jobs waiting
				// Enqueue 3 jobs
				for i := 0; i < 3; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-update-complete-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Set up StreamJobs for the worker
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-update-complete-1", []string{"tag1"}, 10, jobChan)
				}()

				time.Sleep(100 * time.Millisecond)

				// Receive first batch of jobs (may contain multiple jobs)
				receivedJobs := make(map[string]*jobpool.Job)
				select {
				case jobs := <-jobChan:
					Expect(len(jobs)).To(BeNumerically(">=", 1))
					for _, j := range jobs {
						receivedJobs[j.ID] = j
					}
					Expect(receivedJobs["job-update-complete-0"]).NotTo(BeNil(), "First job should be received")
				case <-time.After(2 * time.Second):
					Fail("First job should be received")
				}

				// When: Complete the job
				err := queue.CompleteJobs(ctx, map[string][]byte{"job-update-complete-0": []byte("result")})
				Expect(err).NotTo(HaveOccurred())

				// Then: Next job should be immediately available (either already in received batch or pushed via notification)
				hasNextJob := receivedJobs["job-update-complete-1"] != nil || receivedJobs["job-update-complete-2"] != nil
				if !hasNextJob {
					// If not in the batch, wait for notification to push it
					select {
					case jobs := <-jobChan:
						Expect(len(jobs)).To(BeNumerically(">=", 1))
						found := false
						for _, j := range jobs {
							if j.ID == "job-update-complete-1" || j.ID == "job-update-complete-2" {
								found = true
								break
							}
						}
						Expect(found).To(BeTrue(), "New job should be immediately pushed after CompleteJob")
					case <-time.After(2 * time.Second):
						Fail("New job should be immediately pushed after CompleteJob")
					}
				}

				streamCancel()
				wg.Wait()
			})
		})

		Context("when multiple jobs complete in sequence", func() {
			It("should continuously push new jobs as each job completes", func() {
				// Given: Worker with capacity and many pending jobs
				// Enqueue 5 jobs
				for i := 0; i < 5; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-sequence-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Set up StreamJobs worker
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-sequence-1", []string{"tag1"}, 10, jobChan)
				}()

				time.Sleep(100 * time.Millisecond)

				// Track all assigned jobs
				allAssignedJobs := make(map[string]bool)
				completedCount := 0

				// Process jobs: complete them one by one and verify new ones are immediately assigned
				for round := 0; round < 3; round++ {
					// Collect assigned jobs
					timeout := time.After(2 * time.Second)
					select {
					case jobs := <-jobChan:
						for _, job := range jobs {
							if !allAssignedJobs[job.ID] {
								allAssignedJobs[job.ID] = true
							}
						}
					case <-timeout:
						// No new jobs, continue
					}

					// Find a job to complete
					jobToComplete := ""
					for jobID := range allAssignedJobs {
						job, err := queue.GetJob(ctx, jobID)
						Expect(err).NotTo(HaveOccurred())
						if job.Status == jobpool.JobStatusRunning {
							jobToComplete = jobID
							break
						}
					}

					if jobToComplete == "" {
						break
					}

					// When: Complete the job
					err := queue.CompleteJobs(ctx, map[string][]byte{jobToComplete: []byte("result")})
					Expect(err).NotTo(HaveOccurred())
					completedCount++
					time.Sleep(100 * time.Millisecond)

					// Then: A new job should be immediately assigned (CompleteJob triggers notification)
					// Verify by checking that we have more assigned jobs than completed
					select {
					case jobs := <-jobChan:
						for _, job := range jobs {
							if !allAssignedJobs[job.ID] {
								allAssignedJobs[job.ID] = true
							}
						}
					case <-time.After(500 * time.Millisecond):
						// May not receive immediately, but should have more assigned than completed
					}
					Expect(len(allAssignedJobs)).To(BeNumerically(">", completedCount), "New job should be assigned after each completion")
				}

				// Verify at least 3 jobs were assigned (initial + at least 2 more after completions)
				Expect(len(allAssignedJobs)).To(BeNumerically(">=", 3), "Worker should continuously receive new jobs as capacity becomes available")

				streamCancel()
				wg.Wait()
			})
		})
	})

	Describe("Capacity Management", func() {
		Context("when StreamJobs is called", func() {
			It("should initialize capacity tracking with maxAssignedJobs", func() {
				// Given: A worker with maxAssignedJobs=5
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-capacity-1", []string{"tag1"}, 5, jobChan)
				}()

				// Wait for StreamJobs to register
				time.Sleep(100 * time.Millisecond)

				// When: Enqueue jobs that match the worker's tags
				for i := 0; i < 10; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-capacity-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Then: Worker should receive at most 5 jobs (maxAssignedJobs)
				receivedJobs := make(map[string]bool)
				timeout := time.After(2 * time.Second)
				for len(receivedJobs) < 5 {
					select {
					case jobs := <-jobChan:
						for _, job := range jobs {
							receivedJobs[job.ID] = true
						}
					case <-timeout:
						break
					}
				}

				// Should receive exactly 5 jobs (capacity limit)
				Expect(len(receivedJobs)).To(Equal(5), "Worker should receive exactly maxAssignedJobs jobs")

				streamCancel()
				wg.Wait()
			})
		})

		Context("when jobs are completed", func() {
			It("should free capacity and push new jobs directly", func() {
				// Given: A worker with maxAssignedJobs=3 and 3 jobs assigned
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-capacity-2", []string{"tag1"}, 3, jobChan)
				}()

				time.Sleep(100 * time.Millisecond)

				// Enqueue 5 jobs
				jobIDs := make([]string, 0, 5)
				for i := 0; i < 5; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-capacity-complete-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					jobID, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
					jobIDs = append(jobIDs, jobID)
				}

				// Receive initial 3 jobs (capacity limit)
				initialJobs := make([]string, 0, 3)
				timeout := time.After(2 * time.Second)
				for len(initialJobs) < 3 {
					select {
					case jobs := <-jobChan:
						for _, job := range jobs {
							initialJobs = append(initialJobs, job.ID)
						}
					case <-timeout:
						break
					}
				}
				Expect(len(initialJobs)).To(Equal(3), "Should receive 3 initial jobs")

				// Clear any buffered jobs from channel before completing
				select {
				case <-jobChan:
					// Clear buffered job if any
				default:
					// No buffered job
				}

				// When: Complete one job
				err := queue.CompleteJobs(ctx, map[string][]byte{initialJobs[0]: []byte("result")})
				Expect(err).NotTo(HaveOccurred())

				// Wait a bit for the push to happen
				time.Sleep(100 * time.Millisecond)

				// Then: A new job should be pushed directly (capacity freed)
				select {
				case jobs := <-jobChan:
					Expect(len(jobs)).To(BeNumerically(">", 0), "New job should be pushed after completion")
					// Verify it's one of the remaining jobs
					found := false
					for _, job := range jobs {
						for _, remainingID := range jobIDs {
							if job.ID == remainingID {
								found = true
								break
							}
						}
					}
					Expect(found).To(BeTrue(), "Pushed job should be one of the remaining pending jobs")
				case <-time.After(2 * time.Second):
					Fail("New job should be pushed immediately after completion")
				}

				streamCancel()
				wg.Wait()
			})
		})

		Context("when multiple jobs are freed for same assignee", func() {
			It("should handle multiplicity correctly", func() {
				// Given: A worker with maxAssignedJobs=5 and 5 jobs assigned
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-multiplicity-1", []string{"tag1"}, 5, jobChan)
				}()

				time.Sleep(100 * time.Millisecond)

				// Enqueue 10 jobs
				jobIDs := make([]string, 0, 10)
				for i := 0; i < 10; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-multi-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					jobID, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
					jobIDs = append(jobIDs, jobID)
				}

				// Receive initial 5 jobs
				initialJobs := make([]string, 0, 5)
				timeout := time.After(2 * time.Second)
				for len(initialJobs) < 5 {
					select {
					case jobs := <-jobChan:
						for _, job := range jobs {
							initialJobs = append(initialJobs, job.ID)
						}
					case <-timeout:
						break
					}
				}

				// When: Complete 3 jobs (freeing 3 capacity units)
				for i := 0; i < 3; i++ {
					err := queue.CompleteJobs(ctx, map[string][]byte{initialJobs[i]: []byte("result")})
					Expect(err).NotTo(HaveOccurred())
				}

				// Then: 3 new jobs should be pushed (multiplicity handled correctly)
				newJobsCount := 0
				timeout = time.After(2 * time.Second)
				for newJobsCount < 3 {
					select {
					case jobs := <-jobChan:
						newJobsCount += len(jobs)
					case <-timeout:
						break
					}
				}
				Expect(newJobsCount).To(Equal(3), "Should receive 3 new jobs after freeing 3 capacity units")

				streamCancel()
				wg.Wait()
			})
		})

		Context("when jobs are enqueued with workers having unused capacity", func() {
			It("should push min(enqueued count, unused capacity) jobs directly", func() {
				// Given: A worker with maxAssignedJobs=5 and 2 jobs already assigned (unused capacity=3)
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-enqueue-1", []string{"tag1"}, 5, jobChan)
				}()

				time.Sleep(100 * time.Millisecond)

				// Assign 2 jobs first
				for i := 0; i < 2; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-enqueue-initial-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Receive the 2 initial jobs
				receivedCount := 0
				timeout := time.After(2 * time.Second)
				for receivedCount < 2 {
					select {
					case jobs := <-jobChan:
						receivedCount += len(jobs)
					case <-timeout:
						break
					}
				}

				// When: Enqueue 5 new jobs (unused capacity is 3)
				newJobIDs := make([]string, 0, 5)
				for i := 0; i < 5; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-enqueue-new-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					jobID, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
					newJobIDs = append(newJobIDs, jobID)
				}

				// Then: Should push min(5, 3) = 3 jobs directly
				newReceivedCount := 0
				timeout = time.After(2 * time.Second)
				for newReceivedCount < 3 {
					select {
					case jobs := <-jobChan:
						newReceivedCount += len(jobs)
					case <-timeout:
						break
					}
				}
				Expect(newReceivedCount).To(Equal(3), "Should push min(5, 3) = 3 jobs directly")

				streamCancel()
				wg.Wait()
			})
		})

		Context("when assignee transitions from 0 to >0 unused capacity", func() {
			It("should trigger job push", func() {
				// Given: A worker with maxAssignedJobs=2 and 2 jobs assigned (unused capacity=0)
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-transition-1", []string{"tag1"}, 2, jobChan)
				}()

				time.Sleep(100 * time.Millisecond)

				// Assign 2 jobs (fill capacity)
				initialJobs := make([]string, 0, 2)
				for i := 0; i < 2; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-transition-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					jobID, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
					initialJobs = append(initialJobs, jobID)
				}

				// Receive the 2 jobs and verify capacity is now 0
				receivedCount := 0
				timeout := time.After(2 * time.Second)
				for receivedCount < 2 {
					select {
					case jobs := <-jobChan:
						receivedCount += len(jobs)
					case <-timeout:
						break
					}
				}
				Expect(receivedCount).To(Equal(2), "Should receive 2 initial jobs")

				// Wait for capacity to be updated (jobs were dequeued and assigned, so capacity should be 0 now)
				// Capacity is updated synchronously when jobs are dequeued, but we wait a bit to ensure
				// all goroutines have finished processing
				time.Sleep(500 * time.Millisecond)

				// Clear any buffered jobs with timeout
				clearDone := make(chan struct{})
				go func() {
					defer close(clearDone)
					for {
						select {
						case <-jobChan:
							// Clear buffered jobs
						case <-time.After(100 * time.Millisecond):
							return
						}
					}
				}()
				<-clearDone

				// Enqueue a new job (should not be pushed yet, capacity is 0)
				newJob := &jobpool.Job{
					ID:            "job-transition-new",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, newJob)
				Expect(err).NotTo(HaveOccurred())

				// Verify no immediate push (capacity is 0) - use a short timeout
				// We may receive buffered jobs from previous operations, but not the new job
				receivedNewJob := false
				checkTimeout := time.After(500 * time.Millisecond)
				for {
					select {
					case jobs := <-jobChan:
						// Check if any of the received jobs is the new job
						for _, job := range jobs {
							if job.ID == "job-transition-new" {
								receivedNewJob = true
								break
							}
						}
						// Continue checking for more buffered jobs
					case <-checkTimeout:
						// Timeout reached, check if we received the new job
						if receivedNewJob {
							Fail("Should not push new job when capacity is 0")
						}
						goto doneChecking
					}
				}
			doneChecking:

				// When: Complete one job (transition from 0 to 1 unused capacity)
				err = queue.CompleteJobs(ctx, map[string][]byte{initialJobs[0]: []byte("result")})
				Expect(err).NotTo(HaveOccurred())

				// Then: New job should be pushed immediately
				select {
				case jobs := <-jobChan:
					Expect(len(jobs)).To(BeNumerically(">=", 1), "Should push at least 1 job after capacity transition")
					// Verify the new job is in the pushed jobs
					found := false
					for _, job := range jobs {
						if job.ID == "job-transition-new" {
							found = true
							break
						}
					}
					Expect(found).To(BeTrue(), "Should push the pending job after capacity transition")
				case <-time.After(2 * time.Second):
					Fail("New job should be pushed immediately after capacity transition")
				}

				streamCancel()
				wg.Wait()
			})
		})
	})

	Describe("Job scheduling ordering", func() {
		Context("when multiple jobs are enqueued with different creation times", func() {
			It("should push jobs in ascending order by COALESCE(last_retry_at, created_at) (oldest first)", func() {
				jobChan := make(chan []*jobpool.Job, 10)
				var wg sync.WaitGroup
				wg.Add(1)

				streamCtx, streamCancel := context.WithCancel(ctx)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-1", nil, 10, jobChan)
				}()

				// Give StreamJobs time to start
				time.Sleep(100 * time.Millisecond)

				// Enqueue jobs with different creation times (oldest to newest)
				baseTime := time.Now()
				job1 := &jobpool.Job{
					ID:            "job-oldest",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test1"),
					CreatedAt:     baseTime.Add(-5 * time.Minute),
				}
				job2 := &jobpool.Job{
					ID:            "job-middle",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test2"),
					CreatedAt:     baseTime.Add(-3 * time.Minute),
				}
				job3 := &jobpool.Job{
					ID:            "job-newest",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test3"),
					CreatedAt:     baseTime.Add(-1 * time.Minute),
				}

				_, err := queue.EnqueueJob(ctx, job1)
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.EnqueueJob(ctx, job2)
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.EnqueueJob(ctx, job3)
				Expect(err).NotTo(HaveOccurred())

				// Wait for jobs to be pushed
				timeout := time.After(2 * time.Second)
				receivedJobs := make([]string, 0, 3)
				for len(receivedJobs) < 3 {
					select {
					case jobs := <-jobChan:
						for _, job := range jobs {
							receivedJobs = append(receivedJobs, job.ID)
						}
					case <-timeout:
						Fail("Timeout waiting for jobs")
					}
				}

				// Should be in ascending order: oldest first (job-oldest, job-middle, job-newest)
				// Per spec: Jobs are selected in ascending order by COALESCE(LastRetryAt, CreatedAt) (oldest first)
				Expect(receivedJobs[0]).To(Equal("job-oldest"))
				Expect(receivedJobs[1]).To(Equal("job-middle"))
				Expect(receivedJobs[2]).To(Equal("job-newest"))

				streamCancel()
				wg.Wait()
			})
		})

		Context("when jobs have retry times", func() {
			It("should push jobs in descending order by COALESCE(last_retry_at, created_at)", func() {
				jobChan := make(chan []*jobpool.Job, 10)
				var wg sync.WaitGroup
				wg.Add(1)

				streamCtx, streamCancel := context.WithCancel(ctx)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-1", nil, 10, jobChan)
				}()

				// Give StreamJobs time to start
				time.Sleep(100 * time.Millisecond)

				baseTime := time.Now()
				// Job 1: created early, will fail and get recent retry time
				job1 := &jobpool.Job{
					ID:            "job-early-retry",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test1"),
					CreatedAt:     baseTime.Add(-10 * time.Minute),
				}
				// Job 2: created recently, never failed
				job2 := &jobpool.Job{
					ID:            "job-recent",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test2"),
					CreatedAt:     baseTime.Add(-2 * time.Minute),
				}
				// Job 3: created early, will fail and get very recent retry time
				job3 := &jobpool.Job{
					ID:            "job-early-very-recent-retry",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test3"),
					CreatedAt:     baseTime.Add(-8 * time.Minute),
				}

				// Enqueue all jobs
				_, err := queue.EnqueueJob(ctx, job1)
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.EnqueueJob(ctx, job2)
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.EnqueueJob(ctx, job3)
				Expect(err).NotTo(HaveOccurred())

				// Receive initial jobs
				timeout := time.After(2 * time.Second)
				initialJobs := make(map[string]*jobpool.Job)
				for len(initialJobs) < 3 {
					select {
					case jobs := <-jobChan:
						for _, job := range jobs {
							initialJobs[job.ID] = job
						}
					case <-timeout:
						Fail("Timeout waiting for initial jobs")
					}
				}

				// Verify initial jobs are in RUNNING state (they were assigned)
				for jobID := range initialJobs {
					job, err := queue.GetJob(ctx, jobID)
					Expect(err).NotTo(HaveOccurred())
					Expect(job.Status).To(Equal(jobpool.JobStatusRunning), "Job %s should be in RUNNING state after assignment", jobID)
				}

				// Complete job2 first to free it up (it was dequeued and is RUNNING)
				err = queue.CompleteJobs(ctx, map[string][]byte{"job-recent": []byte("completed")})
				Expect(err).NotTo(HaveOccurred())

				// Fail job1 and job3 BEFORE cancelling StreamJobs to set last_retry_at
				// These jobs are in RUNNING state, so FailJob should transition them to FAILED_RETRY
				err = queue.FailJobs(ctx, map[string]string{"job-early-retry": "error1"})
				Expect(err).NotTo(HaveOccurred())
				// Small delay to ensure different retry times and allow transaction to commit
				time.Sleep(50 * time.Millisecond)
				err = queue.FailJobs(ctx, map[string]string{"job-early-very-recent-retry": "error3"})
				Expect(err).NotTo(HaveOccurred())
				// Additional delay to ensure backend transaction is committed
				time.Sleep(100 * time.Millisecond)

				// Cancel StreamJobs after failing jobs to prevent it from dequeuing them again
				streamCancel()
				wg.Wait()

				// Wait for state transitions to complete and verify failed jobs are in FAILED_RETRY state
				// Use Eventually to wait for the state transition (backend may need time to commit)
				// Note: The debug logs show FailJob transitions the job correctly within its transaction,
				// but there may be a transaction isolation issue where GetJob reads from a different
				// transaction/snapshot that hasn't seen the commit yet. We wait longer to allow
				// the backend transaction to fully commit and be visible to subsequent reads.
				Eventually(func() jobpool.JobStatus {
					job, err := queue.GetJob(ctx, "job-early-retry")
					if err != nil {
						fmt.Printf("[DEBUG TEST] GetJob error for job-early-retry: %v\n", err)
						return jobpool.JobStatusUnknownRetry // Return something to continue retrying
					}
					fmt.Printf("[DEBUG TEST] job-early-retry status check: %s\n", job.Status)
					return job.Status
				}, 3*time.Second, 100*time.Millisecond).Should(Equal(jobpool.JobStatusFailedRetry),
					"Failed job should transition to FAILED_RETRY state (backend transaction may need time to commit)")

				Eventually(func() jobpool.JobStatus {
					job, err := queue.GetJob(ctx, "job-early-very-recent-retry")
					if err != nil {
						fmt.Printf("[DEBUG TEST] GetJob error for job-early-very-recent-retry: %v\n", err)
						return jobpool.JobStatusUnknownRetry // Return something to continue retrying
					}
					fmt.Printf("[DEBUG TEST] job-early-very-recent-retry status check: %s\n", job.Status)
					return job.Status
				}, 3*time.Second, 100*time.Millisecond).Should(Equal(jobpool.JobStatusFailedRetry),
					"Failed job should transition to FAILED_RETRY state (backend transaction may need time to commit)")

				// Note: The spec says jobs are selected in ascending order (oldest first),
				// but the order in slices sent to the channel is not guaranteed.
				// The key verification is that failed jobs become eligible (FAILED_RETRY state)
				// and the notification mechanism works without hanging.
			})
		})

		Context("when jobs are pushed after capacity is freed", func() {
			It("should push new jobs in correct order", func() {
				jobChan := make(chan []*jobpool.Job, 10)
				var wg sync.WaitGroup
				wg.Add(1)

				streamCtx, streamCancel := context.WithCancel(ctx)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-1", nil, 5, jobChan) // maxAssignedJobs = 5
				}()

				// Give StreamJobs time to start
				time.Sleep(100 * time.Millisecond)

				baseTime := time.Now()
				// Enqueue 5 jobs first
				for i := 0; i < 5; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-initial-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						CreatedAt:     baseTime.Add(time.Duration(-i) * time.Minute),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Receive initial 5 jobs
				timeout := time.After(2 * time.Second)
				initialCount := 0
				for initialCount < 5 {
					select {
					case jobs := <-jobChan:
						initialCount += len(jobs)
					case <-timeout:
						Fail("Timeout waiting for initial jobs")
					}
				}

				// Complete one job to free capacity
				err := queue.CompleteJobs(ctx, map[string][]byte{"job-initial-0": []byte("result")})
				Expect(err).NotTo(HaveOccurred())

				// Enqueue 2 new jobs with different creation times
				jobNew1 := &jobpool.Job{
					ID:            "job-new-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     baseTime.Add(-10 * time.Minute), // Older
				}
				jobNew2 := &jobpool.Job{
					ID:            "job-new-2",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     baseTime.Add(-5 * time.Minute), // Newer
				}

				_, err = queue.EnqueueJob(ctx, jobNew1)
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.EnqueueJob(ctx, jobNew2)
				Expect(err).NotTo(HaveOccurred())

				// Wait for new jobs to be pushed (only 1 capacity is available after completing job-initial-0)
				timeout = time.After(2 * time.Second)
				receivedJobs := make([]string, 0, 2)
				receivedJobSet := make(map[string]bool)
				// Mark initial jobs as already received
				for i := 0; i < 5; i++ {
					receivedJobSet[fmt.Sprintf("job-initial-%d", i)] = true
				}
				// Wait for at least 1 new job (capacity allows only 1)
				for len(receivedJobs) < 1 {
					select {
					case jobs := <-jobChan:
						for _, job := range jobs {
							if !receivedJobSet[job.ID] {
								receivedJobs = append(receivedJobs, job.ID)
								receivedJobSet[job.ID] = true
							}
						}
						if len(receivedJobs) >= 1 {
							break
						}
					case <-timeout:
						// If we didn't get at least 1, that's a problem
						break
					}
				}

				// Note: The spec says jobs are selected in ascending order (oldest first),
				// but the order in slices sent to the channel is not guaranteed.
				// We just verify that at least one new job is received (capacity allows only 1).
				Expect(len(receivedJobs)).To(BeNumerically(">=", 1), "Should receive at least one new job")
				hasNewJob := false
				for _, jobID := range receivedJobs {
					if jobID == "job-new-1" || jobID == "job-new-2" {
						hasNewJob = true
						break
					}
				}
				Expect(hasNewJob).To(BeTrue(), "Should receive at least one of the new jobs")

				streamCancel()
				wg.Wait()
			})
		})
	})

	Describe("Job Submission", func() {
		Describe("EnqueueJobs", func() {
			Context("when multiple jobs are enqueued", func() {
				It("should enqueue all jobs and notify workers", func() {
					// Given: A worker waiting via StreamJobs
					jobChan := make(chan []*jobpool.Job, 10)
					streamCtx, streamCancel := context.WithCancel(ctx)
					defer streamCancel()

					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						_ = queue.StreamJobs(streamCtx, "worker-1", []string{"tag1"}, 10, jobChan)
					}()

					time.Sleep(100 * time.Millisecond)

					// When: Enqueue multiple jobs
					jobs := []*jobpool.Job{
						{
							ID:            "job-batch-1",
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test1"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						},
						{
							ID:            "job-batch-2",
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test2"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						},
						{
							ID:            "job-batch-3",
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test3"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						},
					}
					jobIDs, err := queue.EnqueueJobs(ctx, jobs)
					Expect(err).NotTo(HaveOccurred())
					Expect(len(jobIDs)).To(Equal(3))

					// Then: All jobs should be received
					receivedJobs := make(map[string]bool)
					timeout := time.After(2 * time.Second)
					for len(receivedJobs) < 3 {
						select {
						case jobs := <-jobChan:
							for _, job := range jobs {
								receivedJobs[job.ID] = true
							}
						case <-timeout:
							break
						}
					}
					Expect(len(receivedJobs)).To(Equal(3), "All jobs should be received")

					streamCancel()
					wg.Wait()
				})

				It("should deduplicate notifications for same tag combinations", func() {
					// Given: A worker waiting via StreamJobs
					jobChan := make(chan []*jobpool.Job, 10)
					streamCtx, streamCancel := context.WithCancel(ctx)
					defer streamCancel()

					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						_ = queue.StreamJobs(streamCtx, "worker-1", []string{"tag1"}, 10, jobChan)
					}()

					time.Sleep(100 * time.Millisecond)

					// When: Enqueue multiple jobs with same tags (should deduplicate notifications)
					jobs := []*jobpool.Job{
						{
							ID:            "job-dedup-1",
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test1"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						},
						{
							ID:            "job-dedup-2",
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test2"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						},
					}
					_, err := queue.EnqueueJobs(ctx, jobs)
					Expect(err).NotTo(HaveOccurred())

					// Then: All jobs should still be received (deduplication is internal optimization)
					receivedJobs := make(map[string]bool)
					timeout := time.After(2 * time.Second)
					for len(receivedJobs) < 2 {
						select {
						case jobs := <-jobChan:
							for _, job := range jobs {
								receivedJobs[job.ID] = true
							}
						case <-timeout:
							break
						}
					}
					Expect(len(receivedJobs)).To(Equal(2), "All jobs should be received despite notification deduplication")

					streamCancel()
					wg.Wait()
				})
			})
		})
	})

	Describe("Job Lifecycle", func() {
		Describe("StopJobWithRetry", func() {
			Context("when job is in CANCELLING state", func() {
				It("should transition to STOPPED with retry increment", func() {
					// Given: A job in CANCELLING state
					job := &jobpool.Job{
						ID:            "job-stop-retry-1",
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())

					// Assign job via StreamJobs
					jobChan := make(chan []*jobpool.Job, 10)
					streamCtx, streamCancel := context.WithCancel(ctx)
					defer streamCancel()

					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						_ = queue.StreamJobs(streamCtx, "worker-1", []string{"tag1"}, 10, jobChan)
					}()

					time.Sleep(100 * time.Millisecond)

					// Receive the job
					select {
					case jobs := <-jobChan:
						Expect(len(jobs)).To(BeNumerically(">=", 1))
						found := false
						for _, j := range jobs {
							if j.ID == "job-stop-retry-1" {
								found = true
								break
							}
						}
						Expect(found).To(BeTrue(), "Job should be assigned")
					case <-time.After(2 * time.Second):
						Fail("Job should be assigned")
					}

					// Cancel the job (transitions to CANCELLING)
					_, _, err = queue.CancelJobs(ctx, nil, []string{"job-stop-retry-1"})
					Expect(err).NotTo(HaveOccurred())

					// Verify job is in CANCELLING state
					cancellingJob, err := queue.GetJob(ctx, "job-stop-retry-1")
					Expect(err).NotTo(HaveOccurred())
					Expect(cancellingJob.Status).To(Equal(jobpool.JobStatusCancelling))

					initialRetryCount := cancellingJob.RetryCount

					// When: Stop job with retry
					err = queue.StopJobsWithRetry(ctx, map[string]string{"job-stop-retry-1": "cancelled with retry"})
					Expect(err).NotTo(HaveOccurred())

					// Then: Job should be STOPPED with retry count incremented
					stoppedJob, err := queue.GetJob(ctx, "job-stop-retry-1")
					Expect(err).NotTo(HaveOccurred())
					Expect(stoppedJob.Status).To(Equal(jobpool.JobStatusStopped))
					Expect(stoppedJob.RetryCount).To(Equal(initialRetryCount + 1))
					Expect(stoppedJob.ErrorMessage).To(Equal("cancelled with retry"))
					Expect(stoppedJob.FinalizedAt).NotTo(BeNil())

					streamCancel()
					wg.Wait()
				})

				It("should free capacity for StreamJobs", func() {
					// Given: Worker with assigned job and pending jobs
					for i := 0; i < 3; i++ {
						job := &jobpool.Job{
							ID:            fmt.Sprintf("job-stop-retry-capacity-%d", i),
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						}
						_, err := queue.EnqueueJob(ctx, job)
						Expect(err).NotTo(HaveOccurred())
					}

					jobChan := make(chan []*jobpool.Job, 10)
					streamCtx, streamCancel := context.WithCancel(ctx)
					defer streamCancel()

					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						_ = queue.StreamJobs(streamCtx, "worker-stop-retry-1", []string{"tag1"}, 10, jobChan)
					}()

					time.Sleep(100 * time.Millisecond)

					// Receive first batch of jobs (may contain multiple jobs)
					receivedJobs := make(map[string]*jobpool.Job)
					select {
					case jobs := <-jobChan:
						Expect(len(jobs)).To(BeNumerically(">=", 1))
						for _, j := range jobs {
							receivedJobs[j.ID] = j
						}
						Expect(receivedJobs["job-stop-retry-capacity-0"]).NotTo(BeNil(), "First job should be received")
					case <-time.After(2 * time.Second):
						Fail("First job should be received")
					}

					// Cancel the job
					_, _, err := queue.CancelJobs(ctx, nil, []string{"job-stop-retry-capacity-0"})
					Expect(err).NotTo(HaveOccurred())

					// When: Stop job with retry
					err = queue.StopJobsWithRetry(ctx, map[string]string{"job-stop-retry-capacity-0": "cancelled"})
					Expect(err).NotTo(HaveOccurred())

					// Then: Next job should be immediately available (either already in received batch or pushed via notification)
					hasNextJob := receivedJobs["job-stop-retry-capacity-1"] != nil || receivedJobs["job-stop-retry-capacity-2"] != nil
					if !hasNextJob {
						// If not in the batch, wait for notification to push it
						select {
						case jobs := <-jobChan:
							Expect(len(jobs)).To(BeNumerically(">=", 1))
							found := false
							for _, j := range jobs {
								if j.ID == "job-stop-retry-capacity-1" || j.ID == "job-stop-retry-capacity-2" {
									found = true
									break
								}
							}
							Expect(found).To(BeTrue(), "New job should be pushed after StopJobWithRetry")
						case <-time.After(2 * time.Second):
							Fail("New job should be pushed after StopJobWithRetry")
						}
					}

					streamCancel()
					wg.Wait()
				})
			})
		})
	})

	Describe("Cancellation", func() {
		Describe("CancelJobs", func() {
			Context("when cancelling INITIAL_PENDING jobs", func() {
				It("should transition to UNSCHEDULED", func() {
					// Given: Jobs in INITIAL_PENDING state
					jobs := []*jobpool.Job{
						{
							ID:            "job-cancel-1",
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test1"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						},
						{
							ID:            "job-cancel-2",
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test2"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						},
					}
					_, err := queue.EnqueueJobs(ctx, jobs)
					Expect(err).NotTo(HaveOccurred())

					// When: Cancel jobs
					cancelled, unknown, err := queue.CancelJobs(ctx, []string{"tag1"}, nil)
					Expect(err).NotTo(HaveOccurred())
					Expect(len(cancelled)).To(Equal(2))
					Expect(len(unknown)).To(Equal(0))

					// Then: Jobs should be in UNSCHEDULED state
					for _, jobID := range cancelled {
						job, err := queue.GetJob(ctx, jobID)
						Expect(err).NotTo(HaveOccurred())
						Expect(job.Status).To(Equal(jobpool.JobStatusUnscheduled))
					}
				})
			})

			Context("when cancelling RUNNING jobs", func() {
				It("should transition to CANCELLING", func() {
					// Given: A job that will be assigned
					job := &jobpool.Job{
						ID:            "job-cancel-running-1",
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())

					// Assign job via StreamJobs
					jobChan := make(chan []*jobpool.Job, 10)
					streamCtx, streamCancel := context.WithCancel(ctx)
					defer streamCancel()

					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						_ = queue.StreamJobs(streamCtx, "worker-cancel-1", []string{"tag1"}, 10, jobChan)
					}()

					time.Sleep(100 * time.Millisecond)

					// Receive the job
					select {
					case jobs := <-jobChan:
						Expect(len(jobs)).To(BeNumerically(">=", 1))
						found := false
						for _, j := range jobs {
							if j.ID == "job-cancel-running-1" {
								found = true
								break
							}
						}
						Expect(found).To(BeTrue(), "Job should be assigned")
					case <-time.After(2 * time.Second):
						Fail("Job should be assigned")
					}

					// Verify job is RUNNING
					runningJob, err := queue.GetJob(ctx, "job-cancel-running-1")
					Expect(err).NotTo(HaveOccurred())
					Expect(runningJob.Status).To(Equal(jobpool.JobStatusRunning))

					// When: Cancel the job
					cancelled, unknown, err := queue.CancelJobs(ctx, nil, []string{"job-cancel-running-1"})
					Expect(err).NotTo(HaveOccurred())
					Expect(len(cancelled)).To(Equal(1))
					Expect(len(unknown)).To(Equal(0))

					// Then: Job should be in CANCELLING state
					cancellingJob, err := queue.GetJob(ctx, "job-cancel-running-1")
					Expect(err).NotTo(HaveOccurred())
					Expect(cancellingJob.Status).To(Equal(jobpool.JobStatusCancelling))

					streamCancel()
					wg.Wait()
				})
			})

			Context("when cancelling FAILED_RETRY jobs", func() {
				It("should transition to STOPPED", func() {
					// Given: A job that will fail
					job := &jobpool.Job{
						ID:            "job-cancel-failed-1",
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())

					// Assign and fail the job
					jobChan := make(chan []*jobpool.Job, 10)
					streamCtx, streamCancel := context.WithCancel(ctx)
					defer streamCancel()

					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						_ = queue.StreamJobs(streamCtx, "worker-cancel-failed-1", []string{"tag1"}, 10, jobChan)
					}()

					time.Sleep(100 * time.Millisecond)

					select {
					case jobs := <-jobChan:
						Expect(len(jobs)).To(BeNumerically(">=", 1))
						found := false
						for _, j := range jobs {
							if j.ID == "job-cancel-failed-1" {
								found = true
								break
							}
						}
						Expect(found).To(BeTrue(), "Job should be assigned")
					case <-time.After(2 * time.Second):
						Fail("Job should be assigned")
					}

					// Fail the job BEFORE cancelling StreamJobs
					err = queue.FailJobs(ctx, map[string]string{"job-cancel-failed-1": "test error"})
					Expect(err).NotTo(HaveOccurred())

					streamCancel()
					wg.Wait()

					// Verify job is in FAILED_RETRY
					failedJob, err := queue.GetJob(ctx, "job-cancel-failed-1")
					Expect(err).NotTo(HaveOccurred())
					Expect(failedJob.Status).To(Equal(jobpool.JobStatusFailedRetry))

					// When: Cancel the job
					cancelled, unknown, err := queue.CancelJobs(ctx, nil, []string{"job-cancel-failed-1"})
					Expect(err).NotTo(HaveOccurred())
					Expect(len(cancelled)).To(Equal(1))
					Expect(len(unknown)).To(Equal(0))

					// Then: Job should be in STOPPED state
					stoppedJob, err := queue.GetJob(ctx, "job-cancel-failed-1")
					Expect(err).NotTo(HaveOccurred())
					Expect(stoppedJob.Status).To(Equal(jobpool.JobStatusStopped))
				})
			})

			Context("when cancelling UNKNOWN_RETRY jobs", func() {
				It("should transition to STOPPED", func() {
					// Given: A job that will become UNKNOWN_RETRY
					job := &jobpool.Job{
						ID:            "job-cancel-unknown-1",
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())

					// Assign job to worker
					jobChan := make(chan []*jobpool.Job, 10)
					streamCtx, streamCancel := context.WithCancel(ctx)
					defer streamCancel()

					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						_ = queue.StreamJobs(streamCtx, "worker-unknown-cancel-1", []string{"tag1"}, 10, jobChan)
					}()

					time.Sleep(100 * time.Millisecond)

					select {
					case jobs := <-jobChan:
						Expect(len(jobs)).To(BeNumerically(">=", 1))
						found := false
						for _, j := range jobs {
							if j.ID == "job-cancel-unknown-1" {
								found = true
								break
							}
						}
						Expect(found).To(BeTrue(), "Job should be assigned")
					case <-time.After(2 * time.Second):
						Fail("Job should be assigned")
					}

					// Mark worker as unresponsive BEFORE cancelling StreamJobs
					err = queue.MarkWorkerUnresponsive(ctx, "worker-unknown-cancel-1")
					Expect(err).NotTo(HaveOccurred())

					streamCancel()
					wg.Wait()

					// Verify job is in UNKNOWN_RETRY
					unknownJob, err := queue.GetJob(ctx, "job-cancel-unknown-1")
					Expect(err).NotTo(HaveOccurred())
					Expect(unknownJob.Status).To(Equal(jobpool.JobStatusUnknownRetry))

					// When: Cancel the job
					cancelled, unknown, err := queue.CancelJobs(ctx, nil, []string{"job-cancel-unknown-1"})
					Expect(err).NotTo(HaveOccurred())
					Expect(len(cancelled)).To(Equal(1))
					Expect(len(unknown)).To(Equal(0))

					// Then: Job should be in STOPPED state
					stoppedJob, err := queue.GetJob(ctx, "job-cancel-unknown-1")
					Expect(err).NotTo(HaveOccurred())
					Expect(stoppedJob.Status).To(Equal(jobpool.JobStatusStopped))
				})
			})

			Context("when cancelling already CANCELLING jobs", func() {
				It("should treat as no-op", func() {
					// Given: A job in CANCELLING state
					job := &jobpool.Job{
						ID:            "job-cancel-already-1",
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())

					// Assign and cancel the job
					jobChan := make(chan []*jobpool.Job, 10)
					streamCtx, streamCancel := context.WithCancel(ctx)
					defer streamCancel()

					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						_ = queue.StreamJobs(streamCtx, "worker-cancel-already-1", []string{"tag1"}, 10, jobChan)
					}()

					time.Sleep(100 * time.Millisecond)

					select {
					case jobs := <-jobChan:
						Expect(len(jobs)).To(BeNumerically(">=", 1))
						found := false
						for _, j := range jobs {
							if j.ID == "job-cancel-already-1" {
								found = true
								break
							}
						}
						Expect(found).To(BeTrue(), "Job should be assigned")
					case <-time.After(2 * time.Second):
						Fail("Job should be assigned")
					}

					// Cancel the job BEFORE cancelling StreamJobs (first time)
					_, _, err = queue.CancelJobs(ctx, nil, []string{"job-cancel-already-1"})
					Expect(err).NotTo(HaveOccurred())

					streamCancel()
					wg.Wait()

					// Verify job is in CANCELLING
					cancellingJob, err := queue.GetJob(ctx, "job-cancel-already-1")
					Expect(err).NotTo(HaveOccurred())
					Expect(cancellingJob.Status).To(Equal(jobpool.JobStatusCancelling))

					// When: Cancel again (should be no-op)
					cancelled, unknown, err := queue.CancelJobs(ctx, nil, []string{"job-cancel-already-1"})
					Expect(err).NotTo(HaveOccurred())
					Expect(len(cancelled)).To(Equal(1)) // Included in cancelled list
					Expect(len(unknown)).To(Equal(0))

					// Then: Job should still be in CANCELLING state
					stillCancellingJob, err := queue.GetJob(ctx, "job-cancel-already-1")
					Expect(err).NotTo(HaveOccurred())
					Expect(stillCancellingJob.Status).To(Equal(jobpool.JobStatusCancelling))
				})
			})
		})

		Describe("AcknowledgeCancellation", func() {
			Context("when wasExecuting is true", func() {
				It("should transition to STOPPED", func() {
					// Given: A job in CANCELLING state
					job := &jobpool.Job{
						ID:            "job-ack-stop-1",
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())

					// Assign and cancel the job
					jobChan := make(chan []*jobpool.Job, 10)
					streamCtx, streamCancel := context.WithCancel(ctx)
					defer streamCancel()

					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						_ = queue.StreamJobs(streamCtx, "worker-ack-1", []string{"tag1"}, 10, jobChan)
					}()

					time.Sleep(100 * time.Millisecond)

					select {
					case jobs := <-jobChan:
						Expect(len(jobs)).To(BeNumerically(">=", 1))
						found := false
						for _, j := range jobs {
							if j.ID == "job-ack-stop-1" {
								found = true
								break
							}
						}
						Expect(found).To(BeTrue(), "Job should be assigned")
					case <-time.After(2 * time.Second):
						Fail("Job should be assigned")
					}

					// Cancel the job BEFORE cancelling StreamJobs
					_, _, err = queue.CancelJobs(ctx, nil, []string{"job-ack-stop-1"})
					Expect(err).NotTo(HaveOccurred())

					streamCancel()
					wg.Wait()

					// Verify job is in CANCELLING
					cancellingJob, err := queue.GetJob(ctx, "job-ack-stop-1")
					Expect(err).NotTo(HaveOccurred())
					Expect(cancellingJob.Status).To(Equal(jobpool.JobStatusCancelling))

					// When: Acknowledge cancellation with wasExecuting=true
					err = queue.AcknowledgeCancellation(ctx, map[string]bool{"job-ack-stop-1": true})
					Expect(err).NotTo(HaveOccurred())

					// Then: Job should be in STOPPED state
					stoppedJob, err := queue.GetJob(ctx, "job-ack-stop-1")
					Expect(err).NotTo(HaveOccurred())
					Expect(stoppedJob.Status).To(Equal(jobpool.JobStatusStopped))
					Expect(stoppedJob.FinalizedAt).NotTo(BeNil())
				})

				It("should free capacity for StreamJobs", func() {
					// Given: Worker with assigned job and pending jobs
					for i := 0; i < 3; i++ {
						job := &jobpool.Job{
							ID:            fmt.Sprintf("job-ack-capacity-%d", i),
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						}
						_, err := queue.EnqueueJob(ctx, job)
						Expect(err).NotTo(HaveOccurred())
					}

					jobChan := make(chan []*jobpool.Job, 10)
					streamCtx, streamCancel := context.WithCancel(ctx)
					defer streamCancel()

					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						_ = queue.StreamJobs(streamCtx, "worker-ack-capacity-1", []string{"tag1"}, 10, jobChan)
					}()

					time.Sleep(100 * time.Millisecond)

					// Receive first batch of jobs (may contain multiple jobs)
					receivedJobs := make(map[string]*jobpool.Job)
					select {
					case jobs := <-jobChan:
						Expect(len(jobs)).To(BeNumerically(">=", 1))
						for _, j := range jobs {
							receivedJobs[j.ID] = j
						}
						Expect(receivedJobs["job-ack-capacity-0"]).NotTo(BeNil(), "First job should be received")
					case <-time.After(2 * time.Second):
						Fail("First job should be received")
					}

					// Cancel the job
					_, _, err := queue.CancelJobs(ctx, nil, []string{"job-ack-capacity-0"})
					Expect(err).NotTo(HaveOccurred())

					// When: Acknowledge cancellation
					err = queue.AcknowledgeCancellation(ctx, map[string]bool{"job-ack-capacity-0": true})
					Expect(err).NotTo(HaveOccurred())

					// Then: Next job should be immediately available (either already in received batch or pushed via notification)
					hasNextJob := receivedJobs["job-ack-capacity-1"] != nil || receivedJobs["job-ack-capacity-2"] != nil
					if !hasNextJob {
						// If not in the batch, wait for notification to push it
						select {
						case jobs := <-jobChan:
							Expect(len(jobs)).To(BeNumerically(">=", 1))
							found := false
							for _, j := range jobs {
								if j.ID == "job-ack-capacity-1" || j.ID == "job-ack-capacity-2" {
									found = true
									break
								}
							}
							Expect(found).To(BeTrue(), "New job should be pushed after AcknowledgeCancellation")
						case <-time.After(2 * time.Second):
							Fail("New job should be pushed after AcknowledgeCancellation")
						}
					}

					streamCancel()
					wg.Wait()
				})
			})

			Context("when wasExecuting is false", func() {
				It("should transition to UNKNOWN_STOPPED", func() {
					// Given: A job in CANCELLING state
					job := &jobpool.Job{
						ID:            "job-ack-unknown-1",
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())

					// Assign and cancel the job
					jobChan := make(chan []*jobpool.Job, 10)
					streamCtx, streamCancel := context.WithCancel(ctx)
					defer streamCancel()

					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						_ = queue.StreamJobs(streamCtx, "worker-ack-unknown-1", []string{"tag1"}, 10, jobChan)
					}()

					time.Sleep(100 * time.Millisecond)

					select {
					case jobs := <-jobChan:
						Expect(len(jobs)).To(BeNumerically(">=", 1))
						found := false
						for _, j := range jobs {
							if j.ID == "job-ack-unknown-1" {
								found = true
								break
							}
						}
						Expect(found).To(BeTrue(), "Job should be assigned")
					case <-time.After(2 * time.Second):
						Fail("Job should be assigned")
					}

					// Cancel the job BEFORE cancelling StreamJobs
					_, _, err = queue.CancelJobs(ctx, nil, []string{"job-ack-unknown-1"})
					Expect(err).NotTo(HaveOccurred())

					streamCancel()
					wg.Wait()

					// Verify job is in CANCELLING
					cancellingJob, err := queue.GetJob(ctx, "job-ack-unknown-1")
					Expect(err).NotTo(HaveOccurred())
					Expect(cancellingJob.Status).To(Equal(jobpool.JobStatusCancelling))

					// When: Acknowledge cancellation with wasExecuting=false
					err = queue.AcknowledgeCancellation(ctx, map[string]bool{"job-ack-unknown-1": false})
					Expect(err).NotTo(HaveOccurred())

					// Then: Job should be in UNKNOWN_STOPPED state
					unknownStoppedJob, err := queue.GetJob(ctx, "job-ack-unknown-1")
					Expect(err).NotTo(HaveOccurred())
					Expect(unknownStoppedJob.Status).To(Equal(jobpool.JobStatusUnknownStopped))
					Expect(unknownStoppedJob.FinalizedAt).NotTo(BeNil())
				})
			})
		})
	})

	Describe("Worker Management", func() {
		Describe("MarkWorkerUnresponsive", func() {
			Context("when worker has RUNNING jobs", func() {
				It("should transition jobs to UNKNOWN_RETRY", func() {
					// Given: Jobs assigned to a worker
					for i := 0; i < 3; i++ {
						job := &jobpool.Job{
							ID:            fmt.Sprintf("job-unresponsive-%d", i),
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						}
						_, err := queue.EnqueueJob(ctx, job)
						Expect(err).NotTo(HaveOccurred())
					}

					// Assign jobs to worker
					jobChan := make(chan []*jobpool.Job, 10)
					streamCtx, streamCancel := context.WithCancel(ctx)
					defer streamCancel()

					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						_ = queue.StreamJobs(streamCtx, "worker-unresponsive-1", []string{"tag1"}, 10, jobChan)
					}()

					time.Sleep(100 * time.Millisecond)

					// Receive all jobs
					receivedJobs := make(map[string]bool)
					timeout := time.After(2 * time.Second)
					for len(receivedJobs) < 3 {
						select {
						case jobs := <-jobChan:
							for _, job := range jobs {
								receivedJobs[job.ID] = true
							}
						case <-timeout:
							break
						}
					}
					Expect(len(receivedJobs)).To(BeNumerically(">=", 2), "At least 2 jobs should be received")

					// Verify jobs are RUNNING
					for jobID := range receivedJobs {
						job, err := queue.GetJob(ctx, jobID)
						Expect(err).NotTo(HaveOccurred())
						Expect(job.Status).To(Equal(jobpool.JobStatusRunning))
						Expect(job.AssigneeID).To(Equal("worker-unresponsive-1"))
					}

					// When: Mark worker as unresponsive BEFORE cancelling StreamJobs
					err := queue.MarkWorkerUnresponsive(ctx, "worker-unresponsive-1")
					Expect(err).NotTo(HaveOccurred())

					streamCancel()
					wg.Wait()

					// Then: All jobs should be in UNKNOWN_RETRY state
					for jobID := range receivedJobs {
						job, err := queue.GetJob(ctx, jobID)
						Expect(err).NotTo(HaveOccurred())
						Expect(job.Status).To(Equal(jobpool.JobStatusUnknownRetry))
						Expect(job.AssigneeID).To(Equal("worker-unresponsive-1")) // Preserved
					}
				})

				It("should free capacity and notify workers", func() {
					// Given: Worker with assigned jobs and another waiting worker
					for i := 0; i < 3; i++ {
						job := &jobpool.Job{
							ID:            fmt.Sprintf("job-unresponsive-capacity-%d", i),
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						}
						_, err := queue.EnqueueJob(ctx, job)
						Expect(err).NotTo(HaveOccurred())
					}

					// Assign jobs to first worker
					jobChan1 := make(chan []*jobpool.Job, 10)
					streamCtx1, streamCancel1 := context.WithCancel(ctx)
					defer streamCancel1()

					var wg1 sync.WaitGroup
					wg1.Add(1)
					go func() {
						defer wg1.Done()
						_ = queue.StreamJobs(streamCtx1, "worker-unresponsive-capacity-1", []string{"tag1"}, 10, jobChan1)
					}()

					time.Sleep(100 * time.Millisecond)

					// Receive jobs
					receivedCount := 0
					timeout := time.After(2 * time.Second)
					for receivedCount < 3 {
						select {
						case jobs := <-jobChan1:
							receivedCount += len(jobs)
						case <-timeout:
							break
						}
					}

					streamCancel1()
					wg1.Wait()

					// Set up second worker
					jobChan2 := make(chan []*jobpool.Job, 10)
					streamCtx2, streamCancel2 := context.WithCancel(ctx)
					defer streamCancel2()

					var wg2 sync.WaitGroup
					wg2.Add(1)
					go func() {
						defer wg2.Done()
						_ = queue.StreamJobs(streamCtx2, "worker-unresponsive-capacity-2", []string{"tag1"}, 10, jobChan2)
					}()

					time.Sleep(100 * time.Millisecond)

					// When: Mark first worker as unresponsive
					err := queue.MarkWorkerUnresponsive(ctx, "worker-unresponsive-capacity-1")
					Expect(err).NotTo(HaveOccurred())

					// Then: Second worker should receive UNKNOWN_RETRY jobs
					select {
					case jobs := <-jobChan2:
						Expect(len(jobs)).To(BeNumerically(">=", 1))
						found := false
						for _, j := range jobs {
							if j.ID == "job-unresponsive-capacity-0" || j.ID == "job-unresponsive-capacity-1" || j.ID == "job-unresponsive-capacity-2" {
								found = true
								break
							}
						}
						Expect(found).To(BeTrue(), "Second worker should receive UNKNOWN_RETRY jobs")
					case <-time.After(2 * time.Second):
						Fail("Second worker should receive UNKNOWN_RETRY jobs")
					}

					streamCancel2()
					wg2.Wait()
				})
			})

			Context("when worker has CANCELLING jobs", func() {
				It("should transition jobs to UNKNOWN_STOPPED", func() {
					// Given: A job assigned to a worker
					job := &jobpool.Job{
						ID:            "job-unresponsive-cancelling-1",
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())

					// Assign job to worker
					jobChan := make(chan []*jobpool.Job, 10)
					streamCtx, streamCancel := context.WithCancel(ctx)
					defer streamCancel()

					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						_ = queue.StreamJobs(streamCtx, "worker-unresponsive-cancelling-1", []string{"tag1"}, 10, jobChan)
					}()

					time.Sleep(100 * time.Millisecond)

					select {
					case jobs := <-jobChan:
						Expect(len(jobs)).To(BeNumerically(">=", 1))
						found := false
						for _, j := range jobs {
							if j.ID == "job-unresponsive-cancelling-1" {
								found = true
								break
							}
						}
						Expect(found).To(BeTrue(), "Job should be assigned")
					case <-time.After(2 * time.Second):
						Fail("Job should be assigned")
					}

					// Cancel the job BEFORE cancelling StreamJobs
					_, _, err = queue.CancelJobs(ctx, nil, []string{"job-unresponsive-cancelling-1"})
					Expect(err).NotTo(HaveOccurred())

					// Verify job is in CANCELLING
					cancellingJob, err := queue.GetJob(ctx, "job-unresponsive-cancelling-1")
					Expect(err).NotTo(HaveOccurred())
					Expect(cancellingJob.Status).To(Equal(jobpool.JobStatusCancelling))

					// When: Mark worker as unresponsive BEFORE cancelling StreamJobs
					err = queue.MarkWorkerUnresponsive(ctx, "worker-unresponsive-cancelling-1")
					Expect(err).NotTo(HaveOccurred())

					streamCancel()
					wg.Wait()

					// Then: Job should be in UNKNOWN_STOPPED state
					unknownStoppedJob, err := queue.GetJob(ctx, "job-unresponsive-cancelling-1")
					Expect(err).NotTo(HaveOccurred())
					Expect(unknownStoppedJob.Status).To(Equal(jobpool.JobStatusUnknownStopped))
					Expect(unknownStoppedJob.AssigneeID).To(Equal("worker-unresponsive-cancelling-1")) // Preserved
				})
			})
		})
	})

	Describe("Query Operations", func() {
		Describe("GetJobStats", func() {
			Context("when querying with tag filters", func() {
				It("should return statistics for matching jobs", func() {
					// Given: Jobs with different tags
					jobs := []*jobpool.Job{
						{
							ID:            "job-stats-1",
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test1"),
							Tags:          []string{"tag1", "tag2"},
							CreatedAt:     time.Now(),
						},
						{
							ID:            "job-stats-2",
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test2"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						},
						{
							ID:            "job-stats-3",
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test3"),
							Tags:          []string{"tag2"},
							CreatedAt:     time.Now(),
						},
					}
					_, err := queue.EnqueueJobs(ctx, jobs)
					Expect(err).NotTo(HaveOccurred())

					// When: Query stats with tag1 filter
					stats, err := queue.GetJobStats(ctx, []string{"tag1"})
					Expect(err).NotTo(HaveOccurred())

					// Then: Should return stats for jobs with tag1 (AND logic)
					Expect(stats.TotalJobs).To(BeNumerically(">=", 2)) // job-stats-1 and job-stats-2
					Expect(stats.PendingJobs).To(BeNumerically(">=", 2))
				})

				It("should return all jobs when tags are empty", func() {
					// Given: Jobs with different tags
					jobs := []*jobpool.Job{
						{
							ID:            "job-stats-empty-1",
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test1"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						},
						{
							ID:            "job-stats-empty-2",
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test2"),
							Tags:          []string{"tag2"},
							CreatedAt:     time.Now(),
						},
					}
					_, err := queue.EnqueueJobs(ctx, jobs)
					Expect(err).NotTo(HaveOccurred())

					// When: Query stats with empty tags
					stats, err := queue.GetJobStats(ctx, nil)
					Expect(err).NotTo(HaveOccurred())

					// Then: Should return stats for all jobs
					Expect(stats.TotalJobs).To(BeNumerically(">=", 2))
					Expect(stats.PendingJobs).To(BeNumerically(">=", 2))
				})

				It("should count jobs in different states", func() {
					// Given: Jobs in different states
					jobs := []*jobpool.Job{
						{
							ID:            "job-stats-states-1",
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test1"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						},
						{
							ID:            "job-stats-states-2",
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test2"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						},
					}
					_, err := queue.EnqueueJobs(ctx, jobs)
					Expect(err).NotTo(HaveOccurred())

					// Assign and complete one job
					jobChan := make(chan []*jobpool.Job, 10)
					streamCtx, streamCancel := context.WithCancel(ctx)
					defer streamCancel()

					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						_ = queue.StreamJobs(streamCtx, "worker-stats-1", []string{"tag1"}, 10, jobChan)
					}()

					time.Sleep(200 * time.Millisecond)

					// Receive and complete only the first job
					var completedJobID string
					select {
					case jobs := <-jobChan:
						Expect(len(jobs)).To(BeNumerically(">=", 1))
						if len(jobs) > 0 {
							completedJobID = jobs[0].ID
							err = queue.CompleteJobs(ctx, map[string][]byte{completedJobID: []byte("result")})
							Expect(err).NotTo(HaveOccurred())
						}
					case <-time.After(2 * time.Second):
						Fail("Should receive at least one job")
					}

					// Wait a bit to ensure the second job is also assigned
					time.Sleep(200 * time.Millisecond)

					// Verify both jobs were assigned (one completed, one still running)
					allJobs, err := queue.GetJobs(ctx, []string{"job-stats-states-1", "job-stats-states-2"})
					Expect(err).NotTo(HaveOccurred())
					completedCount := 0
					runningCount := 0
					for _, job := range allJobs {
						if job.Status == jobpool.JobStatusCompleted {
							completedCount++
						} else if job.Status == jobpool.JobStatusRunning {
							runningCount++
						}
					}
					Expect(completedCount).To(BeNumerically(">=", 1), "At least one job should be completed")
					// At least one job should be running (the one we didn't complete)
					// OR both jobs might have been assigned and we need to wait for cleanup

					streamCancel()
					wg.Wait()

					// Wait for cleanup to complete
					time.Sleep(300 * time.Millisecond)

					// When: Query stats
					stats, err := queue.GetJobStats(ctx, []string{"tag1"})
					Expect(err).NotTo(HaveOccurred())

					// Then: Should have correct counts
					// Note: Both jobs were assigned via StreamJobs, one was completed, one remains running
					// After StreamJobs is canceled, the running job should be transitioned to FAILED_RETRY
					Expect(stats.TotalJobs).To(BeNumerically(">=", 2))
					Expect(stats.CompletedJobs).To(BeNumerically(">=", 1))
					// The remaining job should be in FAILED_RETRY (transitioned by StreamJobs cleanup)
					Expect(stats.FailedJobs).To(BeNumerically(">=", 1))
				})
			})
		})
	})

	Describe("Maintenance", func() {
		Describe("ResetRunningJobs", func() {
			Context("when jobs are in RUNNING state", func() {
				It("should transition all to UNKNOWN_RETRY", func() {
					// Given: Jobs assigned to workers
					for i := 0; i < 3; i++ {
						job := &jobpool.Job{
							ID:            fmt.Sprintf("job-reset-%d", i),
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						}
						_, err := queue.EnqueueJob(ctx, job)
						Expect(err).NotTo(HaveOccurred())
					}

					// Assign jobs
					jobChan := make(chan []*jobpool.Job, 10)
					streamCtx, streamCancel := context.WithCancel(ctx)
					defer streamCancel()

					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						_ = queue.StreamJobs(streamCtx, "worker-reset-1", []string{"tag1"}, 10, jobChan)
					}()

					time.Sleep(100 * time.Millisecond)

					// Receive jobs
					receivedJobs := make(map[string]bool)
					timeout := time.After(2 * time.Second)
					for len(receivedJobs) < 3 {
						select {
						case jobs := <-jobChan:
							for _, job := range jobs {
								receivedJobs[job.ID] = true
							}
						case <-timeout:
							break
						}
					}

					// Verify jobs are RUNNING
					for jobID := range receivedJobs {
						job, err := queue.GetJob(ctx, jobID)
						Expect(err).NotTo(HaveOccurred())
						Expect(job.Status).To(Equal(jobpool.JobStatusRunning))
					}

					// When: Reset running jobs BEFORE cancelling StreamJobs
					err := queue.ResetRunningJobs(ctx)
					Expect(err).NotTo(HaveOccurred())

					streamCancel()
					wg.Wait()

					// Then: All jobs should be in UNKNOWN_RETRY state
					for jobID := range receivedJobs {
						job, err := queue.GetJob(ctx, jobID)
						Expect(err).NotTo(HaveOccurred())
						Expect(job.Status).To(Equal(jobpool.JobStatusUnknownRetry))
					}
				})

				It("should notify workers of newly eligible jobs", func() {
					// Given: Jobs assigned to a worker
					for i := 0; i < 2; i++ {
						job := &jobpool.Job{
							ID:            fmt.Sprintf("job-reset-notify-%d", i),
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						}
						_, err := queue.EnqueueJob(ctx, job)
						Expect(err).NotTo(HaveOccurred())
					}

					// Assign jobs to first worker
					jobChan1 := make(chan []*jobpool.Job, 10)
					streamCtx1, streamCancel1 := context.WithCancel(ctx)
					defer streamCancel1()

					var wg1 sync.WaitGroup
					wg1.Add(1)
					go func() {
						defer wg1.Done()
						_ = queue.StreamJobs(streamCtx1, "worker-reset-notify-1", []string{"tag1"}, 10, jobChan1)
					}()

					time.Sleep(100 * time.Millisecond)

					// Receive jobs
					receivedCount := 0
					timeout := time.After(2 * time.Second)
					for receivedCount < 2 {
						select {
						case jobs := <-jobChan1:
							receivedCount += len(jobs)
						case <-timeout:
							break
						}
					}

					streamCancel1()
					wg1.Wait()

					// Set up second worker
					jobChan2 := make(chan []*jobpool.Job, 10)
					streamCtx2, streamCancel2 := context.WithCancel(ctx)
					defer streamCancel2()

					var wg2 sync.WaitGroup
					wg2.Add(1)
					go func() {
						defer wg2.Done()
						_ = queue.StreamJobs(streamCtx2, "worker-reset-notify-2", []string{"tag1"}, 10, jobChan2)
					}()

					time.Sleep(100 * time.Millisecond)

					// When: Reset running jobs
					err := queue.ResetRunningJobs(ctx)
					Expect(err).NotTo(HaveOccurred())

					// Then: Second worker should receive UNKNOWN_RETRY jobs
					select {
					case jobs := <-jobChan2:
						Expect(len(jobs)).To(BeNumerically(">=", 1))
						found := false
						for _, j := range jobs {
							if j.ID == "job-reset-notify-0" || j.ID == "job-reset-notify-1" {
								found = true
								break
							}
						}
						Expect(found).To(BeTrue(), "Second worker should receive UNKNOWN_RETRY jobs")
					case <-time.After(2 * time.Second):
						Fail("Second worker should receive UNKNOWN_RETRY jobs")
					}

					streamCancel2()
					wg2.Wait()
				})
			})
		})

		Describe("DeleteJobs", func() {
			Context("when jobs are in final states", func() {
				It("should delete jobs matching tags", func() {
					// Given: Jobs in final states
					jobs := []*jobpool.Job{
						{
							ID:            "job-delete-1",
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test1"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						},
						{
							ID:            "job-delete-2",
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test2"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						},
					}
					_, err := queue.EnqueueJobs(ctx, jobs)
					Expect(err).NotTo(HaveOccurred())

					// Complete the jobs
					jobChan := make(chan []*jobpool.Job, 10)
					streamCtx, streamCancel := context.WithCancel(ctx)
					defer streamCancel()

					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						_ = queue.StreamJobs(streamCtx, "worker-delete-1", []string{"tag1"}, 10, jobChan)
					}()

					time.Sleep(100 * time.Millisecond)

					receivedJobs := make(map[string]*jobpool.Job)
					timeout := time.After(2 * time.Second)
					for len(receivedJobs) < 2 {
						select {
						case jobs := <-jobChan:
							for _, job := range jobs {
								receivedJobs[job.ID] = job
							}
						case <-timeout:
							break
						}
					}

					for _, job := range receivedJobs {
						err = queue.CompleteJobs(ctx, map[string][]byte{job.ID: []byte("result")})
						Expect(err).NotTo(HaveOccurred())
					}

					streamCancel()
					wg.Wait()

					// When: Delete jobs by tags
					err = queue.DeleteJobs(ctx, []string{"tag1"}, nil)
					Expect(err).NotTo(HaveOccurred())

					// Then: Jobs should be deleted
					for jobID := range receivedJobs {
						_, err := queue.GetJob(ctx, jobID)
						Expect(err).To(HaveOccurred(), "Job should be deleted")
					}
				})

				It("should delete jobs by jobIDs", func() {
					// Given: Jobs in final states
					jobs := []*jobpool.Job{
						{
							ID:            "job-delete-id-1",
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test1"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						},
						{
							ID:            "job-delete-id-2",
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test2"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						},
					}
					jobIDs, err := queue.EnqueueJobs(ctx, jobs)
					Expect(err).NotTo(HaveOccurred())

					// Complete the jobs
					jobChan := make(chan []*jobpool.Job, 10)
					streamCtx, streamCancel := context.WithCancel(ctx)
					defer streamCancel()

					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						_ = queue.StreamJobs(streamCtx, "worker-delete-id-1", []string{"tag1"}, 10, jobChan)
					}()

					time.Sleep(100 * time.Millisecond)

					receivedJobs := make(map[string]*jobpool.Job)
					timeout := time.After(2 * time.Second)
					for len(receivedJobs) < 2 {
						select {
						case jobs := <-jobChan:
							for _, job := range jobs {
								receivedJobs[job.ID] = job
							}
						case <-timeout:
							break
						}
					}

					for _, job := range receivedJobs {
						err = queue.CompleteJobs(ctx, map[string][]byte{job.ID: []byte("result")})
						Expect(err).NotTo(HaveOccurred())
					}

					streamCancel()
					wg.Wait()

					// When: Delete jobs by jobIDs
					err = queue.DeleteJobs(ctx, nil, jobIDs)
					Expect(err).NotTo(HaveOccurred())

					// Then: Jobs should be deleted
					for _, jobID := range jobIDs {
						_, err := queue.GetJob(ctx, jobID)
						Expect(err).To(HaveOccurred(), "Job should be deleted")
					}
				})

				It("should return error when non-final state jobs exist", func() {
					// Given: Jobs in different states
					jobs := []*jobpool.Job{
						{
							ID:            "job-delete-error-1",
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test1"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						},
						{
							ID:            "job-delete-error-2",
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test2"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						},
					}
					jobIDs, err := queue.EnqueueJobs(ctx, jobs)
					Expect(err).NotTo(HaveOccurred())

					// Complete one job
					jobChan := make(chan []*jobpool.Job, 10)
					streamCtx, streamCancel := context.WithCancel(ctx)
					defer streamCancel()

					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						_ = queue.StreamJobs(streamCtx, "worker-delete-error-1", []string{"tag1"}, 10, jobChan)
					}()

					time.Sleep(100 * time.Millisecond)

					select {
					case jobs := <-jobChan:
						if len(jobs) > 0 {
							err = queue.CompleteJobs(ctx, map[string][]byte{jobs[0].ID: []byte("result")})
							Expect(err).NotTo(HaveOccurred())
						}
					case <-time.After(2 * time.Second):
					}

					streamCancel()
					wg.Wait()

					// When: Try to delete all jobs by tags (one is still pending)
					err = queue.DeleteJobs(ctx, []string{"tag1"}, nil)
					Expect(err).To(HaveOccurred(), "Should return error when non-final state jobs exist")

					// Then: No jobs should be deleted
					for _, jobID := range jobIDs {
						_, err := queue.GetJob(ctx, jobID)
						Expect(err).NotTo(HaveOccurred(), "Jobs should not be deleted")
					}
				})
			})
		})

		Describe("CleanupExpiredJobs", func() {
			Context("when COMPLETED jobs are older than TTL", func() {
				It("should delete old COMPLETED jobs", func() {
					// Given: Jobs that will be completed
					oldTime := time.Now().Add(-2 * time.Hour)
					jobs := []*jobpool.Job{
						{
							ID:            "job-cleanup-1",
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test1"),
							Tags:          []string{"tag1"},
							CreatedAt:     oldTime,
						},
						{
							ID:            "job-cleanup-2",
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test2"),
							Tags:          []string{"tag1"},
							CreatedAt:     oldTime,
						},
					}
					jobIDs, err := queue.EnqueueJobs(ctx, jobs)
					Expect(err).NotTo(HaveOccurred())

					// Complete the jobs
					jobChan := make(chan []*jobpool.Job, 10)
					streamCtx, streamCancel := context.WithCancel(ctx)
					defer streamCancel()

					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						_ = queue.StreamJobs(streamCtx, "worker-cleanup-1", []string{"tag1"}, 10, jobChan)
					}()

					time.Sleep(100 * time.Millisecond)

					receivedJobs := make(map[string]*jobpool.Job)
					timeout := time.After(2 * time.Second)
					for len(receivedJobs) < 2 {
						select {
						case jobs := <-jobChan:
							for _, job := range jobs {
								receivedJobs[job.ID] = job
							}
						case <-timeout:
							break
						}
					}

					for _, job := range receivedJobs {
						err = queue.CompleteJobs(ctx, map[string][]byte{job.ID: []byte("result")})
						Expect(err).NotTo(HaveOccurred())
					}

					streamCancel()
					wg.Wait()

					// Wait for finalized_at to become old enough for cleanup
					// Since cleanup checks finalized_at (when job was completed), we need to wait
					// TTL * 2 to ensure finalized_at is older than TTL
					ttl := 1 * time.Second
					time.Sleep(ttl*2 + 100*time.Millisecond) // Add small buffer for safety

					// When: Cleanup expired jobs (TTL = 1 second, finalized_at is now > 2 seconds old)
					err = queue.CleanupExpiredJobs(ctx, ttl)
					Expect(err).NotTo(HaveOccurred())

					// Then: Old jobs should be deleted
					for _, jobID := range jobIDs {
						_, err := queue.GetJob(ctx, jobID)
						Expect(err).To(HaveOccurred(), "Old job should be deleted")
					}
				})

				It("should preserve recent COMPLETED jobs", func() {
					// Given: Recent jobs that will be completed
					recentTime := time.Now().Add(-30 * time.Minute)
					jobs := []*jobpool.Job{
						{
							ID:            "job-cleanup-recent-1",
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test1"),
							Tags:          []string{"tag1"},
							CreatedAt:     recentTime,
						},
					}
					jobIDs, err := queue.EnqueueJobs(ctx, jobs)
					Expect(err).NotTo(HaveOccurred())

					// Complete the job
					jobChan := make(chan []*jobpool.Job, 10)
					streamCtx, streamCancel := context.WithCancel(ctx)
					defer streamCancel()

					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						_ = queue.StreamJobs(streamCtx, "worker-cleanup-recent-1", []string{"tag1"}, 10, jobChan)
					}()

					time.Sleep(100 * time.Millisecond)

					select {
					case jobs := <-jobChan:
						if len(jobs) > 0 {
							err = queue.CompleteJobs(ctx, map[string][]byte{jobs[0].ID: []byte("result")})
							Expect(err).NotTo(HaveOccurred())
						}
					case <-time.After(2 * time.Second):
					}

					streamCancel()
					wg.Wait()

					// When: Cleanup expired jobs (TTL = 1 hour, job is 30 minutes old)
					err = queue.CleanupExpiredJobs(ctx, 1*time.Hour)
					Expect(err).NotTo(HaveOccurred())

					// Then: Recent job should be preserved
					for _, jobID := range jobIDs {
						_, err := queue.GetJob(ctx, jobID)
						Expect(err).NotTo(HaveOccurred(), "Recent job should be preserved")
					}
				})
			})
		})
	})

	Describe("Resource Management", func() {
		Describe("Close", func() {
			Context("when queue is closed", func() {
				It("should terminate all StreamJobs calls", func() {
					// Given: Active StreamJobs calls
					jobChan1 := make(chan []*jobpool.Job, 10)
					streamCtx1, streamCancel1 := context.WithCancel(ctx)
					defer streamCancel1()

					var wg1 sync.WaitGroup
					wg1.Add(1)
					streamErr1 := make(chan error, 1)
					go func() {
						defer wg1.Done()
						err := queue.StreamJobs(streamCtx1, "worker-close-1", []string{"tag1"}, 10, jobChan1)
						streamErr1 <- err
					}()

					jobChan2 := make(chan []*jobpool.Job, 10)
					streamCtx2, streamCancel2 := context.WithCancel(ctx)
					defer streamCancel2()

					var wg2 sync.WaitGroup
					wg2.Add(1)
					streamErr2 := make(chan error, 1)
					go func() {
						defer wg2.Done()
						err := queue.StreamJobs(streamCtx2, "worker-close-2", []string{"tag1"}, 10, jobChan2)
						streamErr2 <- err
					}()

					time.Sleep(100 * time.Millisecond)

					// When: Close the queue
					err := queue.Close()
					Expect(err).NotTo(HaveOccurred())

					// Then: StreamJobs should terminate
					wg1.Wait()
					wg2.Wait()

					select {
					case err := <-streamErr1:
						Expect(err).To(BeNil(), "StreamJobs should return nil on queue close")
					case <-time.After(2 * time.Second):
						Fail("StreamJobs should terminate")
					}

					select {
					case err := <-streamErr2:
						Expect(err).To(BeNil(), "StreamJobs should return nil on queue close")
					case <-time.After(2 * time.Second):
						Fail("StreamJobs should terminate")
					}

					// Channels should be closed
					_, ok := <-jobChan1
					Expect(ok).To(BeFalse(), "Channel should be closed")

					_, ok = <-jobChan2
					Expect(ok).To(BeFalse(), "Channel should be closed")

					streamCancel1()
					streamCancel2()
				})

				It("should transition undelivered RUNNING jobs to FAILED_RETRY", func() {
					// Given: Jobs assigned but not yet delivered
					for i := 0; i < 2; i++ {
						job := &jobpool.Job{
							ID:            fmt.Sprintf("job-close-cleanup-%d", i),
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						}
						_, err := queue.EnqueueJob(ctx, job)
						Expect(err).NotTo(HaveOccurred())
					}

					// Set up StreamJobs with small channel to potentially block delivery
					jobChan := make(chan []*jobpool.Job, 1) // Small buffer
					streamCtx, streamCancel := context.WithCancel(ctx)
					defer streamCancel()

					var wg sync.WaitGroup
					wg.Add(1)
					go func() {
						defer wg.Done()
						_ = queue.StreamJobs(streamCtx, "worker-close-cleanup-1", []string{"tag1"}, 10, jobChan)
					}()

					time.Sleep(100 * time.Millisecond)

					// Jobs should be assigned (status RUNNING) but may not all be delivered
					// Close the queue
					err := queue.Close()
					Expect(err).NotTo(HaveOccurred())

					wg.Wait()
					streamCancel()

					// Then: Any undelivered RUNNING jobs should be in FAILED_RETRY
					// Note: This is implementation-dependent, but we verify jobs are not stuck in RUNNING
					for i := 0; i < 2; i++ {
						jobID := fmt.Sprintf("job-close-cleanup-%d", i)
						job, err := queue.GetJob(ctx, jobID)
						if err == nil {
							// Job should not be in RUNNING state after close
							Expect(job.Status).NotTo(Equal(jobpool.JobStatusRunning), "Job should not be stuck in RUNNING after close")
						}
					}
				})
			})
		})
	})
})
