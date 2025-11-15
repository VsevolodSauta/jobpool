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

var _ = Describe("StreamJobs Notification Mechanism", func() {
	var (
		backend jobpool.Backend
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

		backend, err = jobpool.NewSQLiteBackend(tmpFile.Name())
		Expect(err).NotTo(HaveOccurred())

		queue = jobpool.NewPoolQueue(backend)
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
					Status:        jobpool.JobStatusPending,
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

		Context("when failed jobs transition to PENDING for retry", func() {
			It("should notify waiting StreamJobs workers after FailJob", func() {
				// Given: A job that will fail and a waiting worker
				job := &jobpool.Job{
					ID:            "job-fail-retry-1",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Dequeue and assign the job
				jobs, err := backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(jobs)).To(Equal(1))

				// Set up StreamJobs worker
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

				// When: Job fails (FailJob transitions to PENDING)
				err = queue.FailJob(ctx, "job-fail-retry-1", "test error")
				Expect(err).NotTo(HaveOccurred())

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
				// Job should be either PENDING (if not yet dequeued) or RUNNING (if already dequeued for retry)
				Expect(retryJob.Status == jobpool.JobStatusPending || retryJob.Status == jobpool.JobStatusRunning).To(BeTrue(), "Failed job should be in PENDING or RUNNING state")

				streamCancel()
				wg.Wait()
			})
		})

		Context("when jobs in UNKNOWN_RETRY state become eligible", func() {
			It("should push UNKNOWN_RETRY jobs to StreamJobs workers", func() {
				// Given: A job in UNKNOWN_RETRY state
				job := &jobpool.Job{
					ID:            "job-unknown-retry-1",
					Status:        jobpool.JobStatusUnknownRetry,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
					AssigneeID:    "old-worker",
				}
				_, err := backend.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.UpdateJobStatus(ctx, job.ID, jobpool.JobStatusUnknownRetry, nil, "worker unresponsive")
				Expect(err).NotTo(HaveOccurred())

				// Set up StreamJobs worker
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

		Context("when job status is updated to PENDING", func() {
			It("should notify waiting StreamJobs workers after UpdateJobStatus to PENDING", func() {
				// Given: A job in STOPPED state and a waiting worker
				job := &jobpool.Job{
					ID:            "job-update-pending-1",
					Status:        jobpool.JobStatusStopped,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := backend.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.UpdateJobStatus(ctx, job.ID, jobpool.JobStatusStopped, nil, "stopped")
				Expect(err).NotTo(HaveOccurred())

				// Set up StreamJobs worker
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

				// When: Job status is updated to PENDING
				err = queue.UpdateJobStatus(ctx, job.ID, jobpool.JobStatusPending, nil, "")
				Expect(err).NotTo(HaveOccurred())

				// Then: Job should be pushed to worker via StreamJobs
				select {
				case jobs := <-jobChan:
					Expect(len(jobs)).To(BeNumerically(">=", 1))
					found := false
					for _, j := range jobs {
						if j.ID == "job-update-pending-1" {
							found = true
							break
						}
					}
					Expect(found).To(BeTrue(), "Job updated to PENDING should be pushed to worker")
				case <-time.After(2 * time.Second):
					Fail("Job updated to PENDING should be pushed via StreamJobs")
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
						Status:        jobpool.JobStatusPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Assign first job to worker
				jobs, err := backend.DequeueJobs(ctx, "worker-complete-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(jobs)).To(Equal(1))
				Expect(jobs[0].ID).To(Equal("job-complete-0"))

				// Set up StreamJobs for the same worker
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

				// When: Complete the assigned job
				err = queue.CompleteJob(ctx, "job-complete-0", []byte("result"))
				Expect(err).NotTo(HaveOccurred())

				// Then: Next job should be immediately pushed (CompleteJob triggers notification)
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
						Status:        jobpool.JobStatusPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Assign first job to worker
				jobs, err := backend.DequeueJobs(ctx, "worker-stop-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(jobs)).To(Equal(1))
				Expect(jobs[0].ID).To(Equal("job-stop-0"))

				// Set up StreamJobs for the same worker
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

				// When: Stop the assigned job
				err = queue.StopJob(ctx, "job-stop-0", "cancelled")
				Expect(err).NotTo(HaveOccurred())

				// Then: Next job should be immediately pushed (StopJob triggers notification)
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
						Status:        jobpool.JobStatusPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Assign first job to worker
				jobs, err := backend.DequeueJobs(ctx, "worker-fail-capacity-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(jobs)).To(Equal(1))
				Expect(jobs[0].ID).To(Equal("job-fail-capacity-0"))

				// Set up StreamJobs for the same worker
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

				// When: Fail the assigned job (FailJob frees capacity and transitions to PENDING)
				err = queue.FailJob(ctx, "job-fail-capacity-0", "test error")
				Expect(err).NotTo(HaveOccurred())

				// Then: Next job should be immediately pushed (FailJob frees capacity and triggers notification)
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

				// Verify failed job is in PENDING state (ready for retry)
				failedJob, err := queue.GetJob(ctx, "job-fail-capacity-0")
				Expect(err).NotTo(HaveOccurred())
				Expect(failedJob.Status).To(Equal(jobpool.JobStatusPending), "Failed job should be in PENDING state for retry")

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
						Status:        jobpool.JobStatusPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Assign first job to worker
				jobs, err := backend.DequeueJobs(ctx, "worker-unknown-stopped-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(jobs)).To(Equal(1))
				Expect(jobs[0].ID).To(Equal("job-unknown-stopped-0"))

				// Set up StreamJobs for the same worker
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

				// When: Mark job as UNKNOWN_STOPPED
				err = queue.MarkJobUnknownStopped(ctx, "job-unknown-stopped-0", "worker unresponsive")
				Expect(err).NotTo(HaveOccurred())

				// Then: Next job should be immediately pushed (MarkJobUnknownStopped triggers notification)
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

				// Verify unknown stopped job status
				unknownStoppedJob, err := queue.GetJob(ctx, "job-unknown-stopped-0")
				Expect(err).NotTo(HaveOccurred())
				Expect(unknownStoppedJob.Status).To(Equal(jobpool.JobStatusUnknownStopped), "Job should be UNKNOWN_STOPPED")

				streamCancel()
				wg.Wait()
			})
		})

		Context("when UpdateJobStatus transitions job to terminal state", func() {
			It("should immediately notify StreamJobs after UpdateJobStatus to COMPLETED", func() {
				// Given: Worker with assigned job and pending jobs waiting
				// Enqueue 3 jobs
				for i := 0; i < 3; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-update-complete-%d", i),
						Status:        jobpool.JobStatusPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Assign first job to worker
				jobs, err := backend.DequeueJobs(ctx, "worker-update-complete-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(jobs)).To(Equal(1))
				Expect(jobs[0].ID).To(Equal("job-update-complete-0"))

				// Set up StreamJobs for the same worker
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

				// When: Update job status to COMPLETED via UpdateJobStatus
				err = queue.UpdateJobStatus(ctx, "job-update-complete-0", jobpool.JobStatusCompleted, []byte("result"), "")
				Expect(err).NotTo(HaveOccurred())

				// Then: Next job should be immediately pushed (UpdateJobStatus to COMPLETED triggers notification)
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
					Expect(found).To(BeTrue(), "New job should be immediately pushed after UpdateJobStatus to COMPLETED")
				case <-time.After(2 * time.Second):
					Fail("New job should be immediately pushed after UpdateJobStatus to COMPLETED")
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
						Status:        jobpool.JobStatusPending,
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
					err := queue.CompleteJob(ctx, jobToComplete, []byte("result"))
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
})
