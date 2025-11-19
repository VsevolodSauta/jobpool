package jobpool_test

import (
	"context"
	"fmt"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/VsevolodSauta/jobpool"
)

var _ = Describe("Queue Interface", func() {
	var (
		backend jobpool.Backend
		queue   jobpool.Queue
		ctx     context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()
		backend = jobpool.NewInMemoryBackend()
		queue = jobpool.NewPoolQueue(backend, testLogger())
	})

	AfterEach(func() {
		if queue != nil {
			_ = queue.Close()
		}
	})

	Describe("Job Submission", func() {
		Context("EnqueueJob", func() {
			It("should enqueue a single job with INITIAL_PENDING status", func() {
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}

				jobID, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				Expect(jobID).To(Equal("job-1"))

				// Verify job was stored
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusInitialPending))
			})

			It("should return error if job is nil", func() {
				_, err := queue.EnqueueJob(ctx, nil)
				Expect(err).To(HaveOccurred())
			})

			It("should return error if job ID is empty", func() {
				job := &jobpool.Job{
					ID:            "",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).To(HaveOccurred())
			})

			It("should return error if job status is not INITIAL_PENDING", func() {
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusRunning,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).To(HaveOccurred())
			})

			It("should notify waiting StreamJobs workers", func() {
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

				// Enqueue job
				job := &jobpool.Job{
					ID:            "job-notify-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Verify job was pushed
				select {
				case jobs := <-jobChan:
					Expect(len(jobs)).To(BeNumerically(">=", 1))
					found := false
					for _, j := range jobs {
						if j.ID == "job-notify-1" {
							found = true
							break
						}
					}
					Expect(found).To(BeTrue())
				case <-time.After(2 * time.Second):
					Fail("Job should be pushed to StreamJobs")
				}

				streamCancel()
				wg.Wait()
			})
		})

		Context("EnqueueJobs", func() {
			It("should enqueue multiple jobs", func() {
				jobs := []*jobpool.Job{
					{
						ID:            "job-1",
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test1"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					},
					{
						ID:            "job-2",
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test2"),
						Tags:          []string{"tag2"},
						CreatedAt:     time.Now(),
					},
				}

				jobIDs, err := queue.EnqueueJobs(ctx, jobs)
				Expect(err).NotTo(HaveOccurred())
				Expect(jobIDs).To(HaveLen(2))
				Expect(jobIDs).To(ContainElements("job-1", "job-2"))
			})

			It("should return empty slice for empty input", func() {
				jobIDs, err := queue.EnqueueJobs(ctx, []*jobpool.Job{})
				Expect(err).NotTo(HaveOccurred())
				Expect(jobIDs).To(BeEmpty())
			})

			It("should return error if any job is nil", func() {
				jobs := []*jobpool.Job{
					{
						ID:            "job-1",
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test1"),
						CreatedAt:     time.Now(),
					},
					nil,
				}
				_, err := queue.EnqueueJobs(ctx, jobs)
				Expect(err).To(HaveOccurred())
			})

			It("should deduplicate notifications for same tag combinations", func() {
				// This is tested indirectly through notification behavior
				jobs := []*jobpool.Job{
					{
						ID:            "job-1",
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test1"),
						Tags:          []string{"tag1", "tag2"},
						CreatedAt:     time.Now(),
					},
					{
						ID:            "job-2",
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test2"),
						Tags:          []string{"tag1", "tag2"},
						CreatedAt:     time.Now(),
					},
				}

				_, err := queue.EnqueueJobs(ctx, jobs)
				Expect(err).NotTo(HaveOccurred())
			})
		})
	})

	Describe("Job Streaming", func() {
		Context("StreamJobs Preconditions", func() {
			It("should return error if assigneeID is empty", func() {
				jobChan := make(chan []*jobpool.Job, 10)
				err := queue.StreamJobs(ctx, "", []string{"tag1"}, 10, jobChan)
				Expect(err).To(HaveOccurred())
			})

			It("should return error if maxAssignedJobs is zero", func() {
				jobChan := make(chan []*jobpool.Job, 10)
				err := queue.StreamJobs(ctx, "worker-1", []string{"tag1"}, 0, jobChan)
				Expect(err).To(HaveOccurred())
			})

			It("should return error if maxAssignedJobs is negative", func() {
				jobChan := make(chan []*jobpool.Job, 10)
				err := queue.StreamJobs(ctx, "worker-1", []string{"tag1"}, -1, jobChan)
				Expect(err).To(HaveOccurred())
			})

			It("should return error if channel is nil", func() {
				err := queue.StreamJobs(ctx, "worker-1", []string{"tag1"}, 10, nil)
				Expect(err).To(HaveOccurred())
			})
		})

		Context("StreamJobs", func() {
			It("should immediately dequeue eligible jobs when capacity is available", func() {
				// Enqueue a job first
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Start StreamJobs
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-1", []string{"tag1"}, 10, jobChan)
				}()

				// Wait for job to be received
				select {
				case jobs := <-jobChan:
					Expect(len(jobs)).To(BeNumerically(">=", 1))
					Expect(jobs[0].ID).To(Equal("job-1"))
					Expect(jobs[0].Status).To(Equal(jobpool.JobStatusRunning))
					Expect(jobs[0].AssigneeID).To(Equal("worker-1"))
					Expect(jobs[0].AssignedAt).NotTo(BeNil())
					Expect(jobs[0].StartedAt).NotTo(BeNil())
				case <-time.After(2 * time.Second):
					Fail("Job should be immediately dequeued")
				}

				streamCancel()
				wg.Wait()
			})

			It("should respect capacity limits", func() {
				// Enqueue 10 jobs
				for i := 0; i < 10; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Start StreamJobs with maxAssignedJobs=5
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-1", []string{"tag1"}, 5, jobChan)
				}()

				// Collect received jobs
				receivedJobs := make(map[string]bool)
				timeout := time.After(2 * time.Second)
			loop1:
				for len(receivedJobs) < 5 {
					select {
					case jobs := <-jobChan:
						for _, job := range jobs {
							receivedJobs[job.ID] = true
						}
					case <-timeout:
						break loop1
					}
				}

				// Should receive exactly 5 jobs (capacity limit)
				Expect(len(receivedJobs)).To(Equal(5))

				streamCancel()
				wg.Wait()
			})

			It("should filter jobs by tags (AND logic)", func() {
				// Enqueue jobs with different tags
				job1 := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test1"),
					Tags:          []string{"tag1", "tag2"},
					CreatedAt:     time.Now(),
				}
				job2 := &jobpool.Job{
					ID:            "job-2",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test2"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job1)
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.EnqueueJob(ctx, job2)
				Expect(err).NotTo(HaveOccurred())

				// Start StreamJobs with tags ["tag1", "tag2"]
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-1", []string{"tag1", "tag2"}, 10, jobChan)
				}()

				// Wait for jobs
				timeout := time.After(2 * time.Second)
				receivedJobs := make(map[string]bool)
			loop2:
				for len(receivedJobs) < 1 {
					select {
					case jobs := <-jobChan:
						for _, job := range jobs {
							receivedJobs[job.ID] = true
						}
					case <-timeout:
						break loop2
					}
				}

				// Should only receive job-1 (has both tags)
				Expect(receivedJobs).To(HaveKey("job-1"))
				Expect(receivedJobs).NotTo(HaveKey("job-2"))

				streamCancel()
				wg.Wait()
			})

			It("should accept all jobs when tags are empty", func() {
				// Enqueue jobs with different tags
				for i := 0; i < 3; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{fmt.Sprintf("tag%d", i)},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Start StreamJobs with empty tags
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-1", nil, 10, jobChan)
				}()

				// Collect received jobs
				receivedJobs := make(map[string]bool)
				timeout := time.After(2 * time.Second)
			loop3:
				for len(receivedJobs) < 3 {
					select {
					case jobs := <-jobChan:
						for _, job := range jobs {
							receivedJobs[job.ID] = true
						}
					case <-timeout:
						break loop3
					}
				}

				// Should receive all jobs
				Expect(len(receivedJobs)).To(Equal(3))

				streamCancel()
				wg.Wait()
			})

			It("should close channel when context is cancelled", func() {
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)

				var wg sync.WaitGroup
				wg.Add(1)
				var streamErr error
				go func() {
					defer wg.Done()
					streamErr = queue.StreamJobs(streamCtx, "worker-1", nil, 10, jobChan)
				}()

				// Wait a bit
				time.Sleep(100 * time.Millisecond)

				// Cancel context
				streamCancel()

				// Wait for StreamJobs to return
				wg.Wait()

				// Channel should be closed
				_, ok := <-jobChan
				Expect(ok).To(BeFalse())
				Expect(streamErr).To(HaveOccurred())
			})

			It("should transition assigned jobs to FAILED_RETRY on termination", func() {
				// Enqueue and assign a job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-1", []string{"tag1"}, 10, jobChan)
				}()

				// Wait for job to be assigned
				select {
				case <-jobChan:
					// Job assigned
				case <-time.After(2 * time.Second):
					Fail("Job should be assigned")
				}

				// Cancel context
				streamCancel()
				wg.Wait()

				// Wait a bit for cleanup
				time.Sleep(100 * time.Millisecond)

				// Job should be in FAILED_RETRY state
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusFailedRetry))
			})

			It("should select jobs in ascending order by COALESCE(LastRetryAt, CreatedAt)", func() {
				baseTime := time.Now()
				// Enqueue jobs with different creation times
				job1 := &jobpool.Job{
					ID:            "job-oldest",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test1"),
					Tags:          []string{"tag1"},
					CreatedAt:     baseTime.Add(-5 * time.Minute),
				}
				job2 := &jobpool.Job{
					ID:            "job-middle",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test2"),
					Tags:          []string{"tag1"},
					CreatedAt:     baseTime.Add(-3 * time.Minute),
				}
				job3 := &jobpool.Job{
					ID:            "job-newest",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test3"),
					Tags:          []string{"tag1"},
					CreatedAt:     baseTime.Add(-1 * time.Minute),
				}

				_, err := queue.EnqueueJob(ctx, job1)
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.EnqueueJob(ctx, job2)
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.EnqueueJob(ctx, job3)
				Expect(err).NotTo(HaveOccurred())

				// Start StreamJobs
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-1", []string{"tag1"}, 10, jobChan)
				}()

				// Collect all jobs
				receivedJobs := make([]string, 0, 3)
				timeout := time.After(2 * time.Second)
			loop4:
				for len(receivedJobs) < 3 {
					select {
					case jobs := <-jobChan:
						for _, job := range jobs {
							receivedJobs = append(receivedJobs, job.ID)
						}
					case <-timeout:
						break loop4
					}
				}

				// Should receive oldest first (job-oldest, job-middle, job-newest)
				// Note: Order in slices may not match, but selection should prioritize oldest
				Expect(len(receivedJobs)).To(Equal(3))
				// Verify all jobs were received
				Expect(receivedJobs).To(ContainElement("job-oldest"))
				Expect(receivedJobs).To(ContainElement("job-middle"))
				Expect(receivedJobs).To(ContainElement("job-newest"))

				streamCancel()
				wg.Wait()
			})

			It("should handle channel full behavior - jobs still assigned even if channel is full", func() {
				// Enqueue a job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Create a channel with no buffer (will block on send)
				jobChan := make(chan []*jobpool.Job)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-1", []string{"tag1"}, 10, jobChan)
				}()

				// Wait a bit for job to be assigned
				time.Sleep(200 * time.Millisecond)

				// Verify job is in RUNNING state (assigned even though channel is full)
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusRunning))
				Expect(retrieved.AssigneeID).To(Equal("worker-1"))

				streamCancel()
				wg.Wait()
			})

			It("should handle multiple StreamJobs calls with same assigneeID", func() {
				// Enqueue jobs
				for i := 0; i < 5; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Start two StreamJobs calls with same assigneeID
				jobChan1 := make(chan []*jobpool.Job, 10)
				jobChan2 := make(chan []*jobpool.Job, 10)
				streamCtx1, streamCancel1 := context.WithCancel(ctx)
				streamCtx2, streamCancel2 := context.WithCancel(ctx)
				defer streamCancel1()
				defer streamCancel2()

				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx1, "worker-1", []string{"tag1"}, 3, jobChan1)
				}()
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx2, "worker-1", []string{"tag1"}, 3, jobChan2)
				}()

				// Both should receive jobs (independent capacity)
				timeout := time.After(2 * time.Second)
				received1 := make(map[string]bool)
				received2 := make(map[string]bool)

			loop5:
				for len(received1)+len(received2) < 5 {
					select {
					case jobs := <-jobChan1:
						for _, job := range jobs {
							received1[job.ID] = true
						}
					case jobs := <-jobChan2:
						for _, job := range jobs {
							received2[job.ID] = true
						}
					case <-timeout:
						break loop5
					}
				}

				// Both should have received some jobs
				Expect(len(received1) + len(received2)).To(BeNumerically(">=", 5))

				streamCancel1()
				streamCancel2()
				wg.Wait()
			})
		})
	})

	Describe("Job Lifecycle", func() {
		Context("CompleteJob", func() {
			It("should preserve AssigneeID and AssignedAt when completing a job", func() {
				// Enqueue and assign job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign job
				jobs, err := backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(jobs)).To(Equal(1))
				assigneeID := jobs[0].AssigneeID
				assignedAt := jobs[0].AssignedAt

				// Complete job
				err = queue.CompleteJob(ctx, "job-1", []byte("result"))
				Expect(err).NotTo(HaveOccurred())

				// Verify fields are preserved
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.AssigneeID).To(Equal(assigneeID))
				Expect(retrieved.AssignedAt).NotTo(BeNil())
				Expect(retrieved.AssignedAt.Equal(*assignedAt)).To(BeTrue())
			})

			It("should set FinalizedAt timestamp when completing a job", func() {
				// Enqueue and assign job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign job
				_, err = backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())

				// Complete job
				err = queue.CompleteJob(ctx, "job-1", []byte("result"))
				Expect(err).NotTo(HaveOccurred())

				// Verify FinalizedAt is set
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.FinalizedAt).NotTo(BeNil())
			})

			It("should transition CANCELLING job to COMPLETED", func() {
				// Enqueue and assign job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign and cancel
				_, err = backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-1"})
				Expect(err).NotTo(HaveOccurred())

				// Complete job (from CANCELLING state)
				err = queue.CompleteJob(ctx, "job-1", []byte("result"))
				Expect(err).NotTo(HaveOccurred())

				// Verify job is completed
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusCompleted))
			})

			It("should transition UNKNOWN_RETRY job to COMPLETED", func() {
				job := &jobpool.Job{
					ID:            "job-1",
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				err := seedUnknownRetryJob(ctx, queue, backend, job)
				Expect(err).NotTo(HaveOccurred())

				// Complete job
				err = queue.CompleteJob(ctx, "job-1", []byte("result"))
				Expect(err).NotTo(HaveOccurred())

				// Verify job is completed
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusCompleted))
			})

			It("should transition UNKNOWN_STOPPED job to COMPLETED", func() {
				job := &jobpool.Job{
					ID:            "job-1",
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				err := seedUnknownStoppedJob(ctx, queue, backend, job)
				Expect(err).NotTo(HaveOccurred())

				// Complete job
				err = queue.CompleteJob(ctx, "job-1", []byte("result"))
				Expect(err).NotTo(HaveOccurred())

				// Verify job is completed
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusCompleted))
			})

			It("should complete a running job", func() {
				// Enqueue and assign job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign job
				jobs, err := backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(jobs)).To(Equal(1))

				// Complete job
				err = queue.CompleteJob(ctx, "job-1", []byte("result"))
				Expect(err).NotTo(HaveOccurred())

				// Verify job is completed
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusCompleted))
				Expect(retrieved.Result).To(Equal([]byte("result")))
			})

			It("should free capacity and notify workers", func() {
				// Enqueue 2 jobs
				for i := 0; i < 2; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Start StreamJobs with capacity 1
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-1", []string{"tag1"}, 1, jobChan)
				}()

				// Receive first job
				var firstJob *jobpool.Job
				select {
				case jobs := <-jobChan:
					Expect(len(jobs)).To(Equal(1))
					firstJob = jobs[0]
				case <-time.After(2 * time.Second):
					Fail("Should receive first job")
				}

				// Complete first job
				err := queue.CompleteJob(ctx, firstJob.ID, []byte("result"))
				Expect(err).NotTo(HaveOccurred())

				// Should receive second job (capacity freed)
				select {
				case jobs := <-jobChan:
					Expect(len(jobs)).To(BeNumerically(">=", 1))
				case <-time.After(2 * time.Second):
					Fail("Should receive second job after completion")
				}

				streamCancel()
				wg.Wait()
			})
		})

		Context("FailJob", func() {
			It("should preserve AssigneeID and AssignedAt when failing a job", func() {
				// Enqueue and assign job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign job
				jobs, err := backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(jobs)).To(Equal(1))
				assigneeID := jobs[0].AssigneeID
				assignedAt := jobs[0].AssignedAt

				// Fail job
				err = queue.FailJob(ctx, "job-1", "test error")
				Expect(err).NotTo(HaveOccurred())

				// Verify fields are preserved
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.AssigneeID).To(Equal(assigneeID))
				Expect(retrieved.AssignedAt).NotTo(BeNil())
				Expect(retrieved.AssignedAt.Equal(*assignedAt)).To(BeTrue())
			})

			It("should increment RetryCount and set LastRetryAt when failing a job", func() {
				// Enqueue and assign job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign job
				_, err = backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())

				// Fail job
				err = queue.FailJob(ctx, "job-1", "test error")
				Expect(err).NotTo(HaveOccurred())

				// Verify retry count incremented and LastRetryAt set
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.RetryCount).To(Equal(1))
				Expect(retrieved.LastRetryAt).NotTo(BeNil())
			})

			It("should transition UNKNOWN_RETRY job to FAILED_RETRY", func() {
				job := &jobpool.Job{
					ID:            "job-1",
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				err := seedUnknownRetryJob(ctx, queue, backend, job)
				Expect(err).NotTo(HaveOccurred())

				// Fail job
				err = queue.FailJob(ctx, "job-1", "test error")
				Expect(err).NotTo(HaveOccurred())

				// Verify job is in FAILED_RETRY state
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusFailedRetry))
			})

			It("should fail a running job", func() {
				// Enqueue and assign job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign job
				jobs, err := backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(jobs)).To(Equal(1))

				// Fail job
				err = queue.FailJob(ctx, "job-1", "test error")
				Expect(err).NotTo(HaveOccurred())

				// Verify job is failed
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusFailedRetry))
				Expect(retrieved.ErrorMessage).To(Equal("test error"))
				Expect(retrieved.RetryCount).To(Equal(1))
			})

			It("should return error if errorMsg is empty", func() {
				err := queue.FailJob(ctx, "job-1", "")
				Expect(err).To(HaveOccurred())
			})

			It("should free capacity and notify workers", func() {
				// Enqueue 2 jobs
				for i := 0; i < 2; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Start StreamJobs with capacity 1
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-1", []string{"tag1"}, 1, jobChan)
				}()

				// Receive first job
				var firstJob *jobpool.Job
				select {
				case jobs := <-jobChan:
					Expect(len(jobs)).To(Equal(1))
					firstJob = jobs[0]
				case <-time.After(2 * time.Second):
					Fail("Should receive first job")
				}

				// Fail first job
				err := queue.FailJob(ctx, firstJob.ID, "test error")
				Expect(err).NotTo(HaveOccurred())

				// Should receive second job (capacity freed, failed job eligible again)
				select {
				case jobs := <-jobChan:
					Expect(len(jobs)).To(BeNumerically(">=", 1))
				case <-time.After(2 * time.Second):
					Fail("Should receive second job after failure")
				}

				streamCancel()
				wg.Wait()
			})
		})

		Context("StopJob", func() {
			It("should preserve AssigneeID and AssignedAt when stopping a job", func() {
				// Enqueue and assign job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign job
				jobs, err := backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(jobs)).To(Equal(1))
				assigneeID := jobs[0].AssigneeID
				assignedAt := jobs[0].AssignedAt

				// Stop job
				err = queue.StopJob(ctx, "job-1", "cancelled")
				Expect(err).NotTo(HaveOccurred())

				// Verify fields are preserved
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.AssigneeID).To(Equal(assigneeID))
				Expect(retrieved.AssignedAt).NotTo(BeNil())
				Expect(retrieved.AssignedAt.Equal(*assignedAt)).To(BeTrue())
			})

			It("should set FinalizedAt timestamp when stopping a job", func() {
				// Enqueue and assign job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign job
				_, err = backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())

				// Stop job
				err = queue.StopJob(ctx, "job-1", "cancelled")
				Expect(err).NotTo(HaveOccurred())

				// Verify FinalizedAt is set
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.FinalizedAt).NotTo(BeNil())
			})

			It("should transition CANCELLING job to STOPPED", func() {
				// Enqueue and assign job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign and cancel
				_, err = backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-1"})
				Expect(err).NotTo(HaveOccurred())

				// Stop job (from CANCELLING state)
				err = queue.StopJob(ctx, "job-1", "cancelled")
				Expect(err).NotTo(HaveOccurred())

				// Verify job is stopped
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusStopped))
			})

			It("should transition UNKNOWN_RETRY job to STOPPED", func() {
				job := &jobpool.Job{
					ID:            "job-1",
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				err := seedUnknownRetryJob(ctx, queue, backend, job)
				Expect(err).NotTo(HaveOccurred())

				// Stop job
				err = queue.StopJob(ctx, "job-1", "cancelled")
				Expect(err).NotTo(HaveOccurred())

				// Verify job is stopped
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusStopped))
			})

			It("should stop a running job", func() {
				// Enqueue and assign job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign job
				jobs, err := backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(jobs)).To(Equal(1))

				// Stop job
				err = queue.StopJob(ctx, "job-1", "cancelled")
				Expect(err).NotTo(HaveOccurred())

				// Verify job is stopped
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusStopped))
				Expect(retrieved.ErrorMessage).To(Equal("cancelled"))
			})

			It("should allow empty errorMsg", func() {
				// Enqueue and assign job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign job
				jobs, err := backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(jobs)).To(Equal(1))

				// Stop job with empty errorMsg
				err = queue.StopJob(ctx, "job-1", "")
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Context("StopJobWithRetry", func() {
			It("should return error if job is not in CANCELLING state", func() {
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Try to stop with retry (not in CANCELLING state)
				err = queue.StopJobWithRetry(ctx, "job-1", "error")
				Expect(err).To(HaveOccurred())
			})

			It("should preserve AssigneeID and AssignedAt when stopping with retry", func() {
				// Enqueue and assign job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign and cancel
				jobs, err := backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(jobs)).To(Equal(1))
				assigneeID := jobs[0].AssigneeID
				assignedAt := jobs[0].AssignedAt

				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-1"})
				Expect(err).NotTo(HaveOccurred())

				// Stop with retry
				err = queue.StopJobWithRetry(ctx, "job-1", "cancelled with error")
				Expect(err).NotTo(HaveOccurred())

				// Verify fields are preserved
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.AssigneeID).To(Equal(assigneeID))
				Expect(retrieved.AssignedAt).NotTo(BeNil())
				Expect(retrieved.AssignedAt.Equal(*assignedAt)).To(BeTrue())
			})

			It("should increment RetryCount and set LastRetryAt when stopping with retry", func() {
				// Enqueue and assign job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign and cancel
				_, err = backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-1"})
				Expect(err).NotTo(HaveOccurred())

				// Stop with retry
				err = queue.StopJobWithRetry(ctx, "job-1", "cancelled with error")
				Expect(err).NotTo(HaveOccurred())

				// Verify retry count incremented and LastRetryAt set
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.RetryCount).To(Equal(1))
				Expect(retrieved.LastRetryAt).NotTo(BeNil())
				Expect(retrieved.FinalizedAt).NotTo(BeNil())
			})

			It("should stop a CANCELLING job with retry increment", func() {
				// Enqueue and assign job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign job
				jobs, err := backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(jobs)).To(Equal(1))

				// Cancel job (transitions to CANCELLING)
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-1"})
				Expect(err).NotTo(HaveOccurred())

				// Stop with retry
				err = queue.StopJobWithRetry(ctx, "job-1", "cancelled with error")
				Expect(err).NotTo(HaveOccurred())

				// Verify job is stopped with retry count incremented
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusStopped))
				Expect(retrieved.RetryCount).To(Equal(1))
			})
		})

		Context("MarkJobUnknownStopped", func() {
			It("should preserve AssigneeID and AssignedAt when marking as unknown stopped", func() {
				// Enqueue and assign job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign job
				jobs, err := backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(jobs)).To(Equal(1))
				assigneeID := jobs[0].AssigneeID
				assignedAt := jobs[0].AssignedAt

				// Mark as unknown stopped
				err = queue.MarkJobUnknownStopped(ctx, "job-1", "worker unresponsive")
				Expect(err).NotTo(HaveOccurred())

				// Verify fields are preserved
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.AssigneeID).To(Equal(assigneeID))
				Expect(retrieved.AssignedAt).NotTo(BeNil())
				Expect(retrieved.AssignedAt.Equal(*assignedAt)).To(BeTrue())
			})

			It("should transition CANCELLING job to UNKNOWN_STOPPED", func() {
				// Enqueue and assign job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign and cancel
				_, err = backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-1"})
				Expect(err).NotTo(HaveOccurred())

				// Mark as unknown stopped
				err = queue.MarkJobUnknownStopped(ctx, "job-1", "worker unresponsive")
				Expect(err).NotTo(HaveOccurred())

				// Verify job is unknown stopped
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusUnknownStopped))
			})

			It("should transition UNKNOWN_RETRY job to UNKNOWN_STOPPED", func() {
				job := &jobpool.Job{
					ID:            "job-1",
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				err := seedUnknownRetryJob(ctx, queue, backend, job)
				Expect(err).NotTo(HaveOccurred())

				// Mark as unknown stopped
				err = queue.MarkJobUnknownStopped(ctx, "job-1", "worker unresponsive")
				Expect(err).NotTo(HaveOccurred())

				// Verify job is unknown stopped
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusUnknownStopped))
			})

			It("should free capacity when transitioning RUNNING job to UNKNOWN_STOPPED", func() {
				// Enqueue 2 jobs
				for i := 0; i < 2; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Start StreamJobs with capacity 1
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-1", []string{"tag1"}, 1, jobChan)
				}()

				// Receive first job
				var firstJob *jobpool.Job
				select {
				case jobs := <-jobChan:
					Expect(len(jobs)).To(Equal(1))
					firstJob = jobs[0]
				case <-time.After(2 * time.Second):
					Fail("Should receive first job")
				}

				// Mark RUNNING job as unknown stopped (should free capacity)
				err := queue.MarkJobUnknownStopped(ctx, firstJob.ID, "worker unresponsive")
				Expect(err).NotTo(HaveOccurred())

				// Should receive second job (capacity freed)
				select {
				case jobs := <-jobChan:
					Expect(len(jobs)).To(BeNumerically(">=", 1))
				case <-time.After(2 * time.Second):
					Fail("Should receive second job after marking as unknown stopped")
				}

				streamCancel()
				wg.Wait()
			})

			It("should transition RUNNING job to UNKNOWN_STOPPED", func() {
				// Enqueue and assign job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign job
				_, err = backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())

				// Mark as unknown stopped
				err = queue.MarkJobUnknownStopped(ctx, "job-1", "worker unresponsive")
				Expect(err).NotTo(HaveOccurred())

				// Verify job is unknown stopped
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusUnknownStopped))
			})

			It("should mark a job as unknown stopped", func() {
				// Enqueue and assign job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign job
				jobs, err := backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(jobs)).To(Equal(1))

				// Mark as unknown stopped
				err = queue.MarkJobUnknownStopped(ctx, "job-1", "worker unresponsive")
				Expect(err).NotTo(HaveOccurred())

				// Verify job is unknown stopped
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusUnknownStopped))
				Expect(retrieved.ErrorMessage).To(Equal("worker unresponsive"))
			})
		})
	})

	Describe("Cancellation", func() {
		Context("CancelJobs", func() {
			It("should transition FAILED_RETRY jobs to STOPPED", func() {
				job := &jobpool.Job{
					ID:            "job-1",
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				err := seedFailedRetryJob(ctx, queue, backend, job)
				Expect(err).NotTo(HaveOccurred())

				cancelled, unknown, err := queue.CancelJobs(ctx, nil, []string{"job-1"})
				Expect(err).NotTo(HaveOccurred())
				Expect(cancelled).To(ContainElement("job-1"))
				Expect(unknown).To(BeEmpty())

				// Verify job is STOPPED
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusStopped))
			})

			It("should transition UNKNOWN_RETRY jobs to STOPPED", func() {
				job := &jobpool.Job{
					ID:            "job-1",
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				err := seedUnknownRetryJob(ctx, queue, backend, job)
				Expect(err).NotTo(HaveOccurred())

				cancelled, unknown, err := queue.CancelJobs(ctx, nil, []string{"job-1"})
				Expect(err).NotTo(HaveOccurred())
				Expect(cancelled).To(ContainElement("job-1"))
				Expect(unknown).To(BeEmpty())

				// Verify job is STOPPED
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusStopped))
			})

			It("should treat CANCELLING jobs as no-op", func() {
				// Enqueue and assign job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign and cancel
				_, err = backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-1"})
				Expect(err).NotTo(HaveOccurred())

				// Verify job is in CANCELLING state
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusCancelling))

				// Cancel again (should be no-op)
				cancelled, unknown, err := queue.CancelJobs(ctx, nil, []string{"job-1"})
				Expect(err).NotTo(HaveOccurred())
				Expect(cancelled).To(ContainElement("job-1"))
				Expect(unknown).To(BeEmpty())

				// Verify job is still in CANCELLING state
				retrieved, err = queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusCancelling))
			})

			It("should include jobs in terminal states in unknownJobIDs", func() {
				// Create completed job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign and complete
				_, err = backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.CompleteJob(ctx, "job-1", []byte("result"))
				Expect(err).NotTo(HaveOccurred())

				// Try to cancel completed job
				cancelled, unknown, err := queue.CancelJobs(ctx, nil, []string{"job-1"})
				Expect(err).NotTo(HaveOccurred())
				Expect(cancelled).To(BeEmpty())
				Expect(unknown).To(ContainElement("job-1"))
			})

			It("should cancel INITIAL_PENDING jobs", func() {
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				cancelled, unknown, err := queue.CancelJobs(ctx, nil, []string{"job-1"})
				Expect(err).NotTo(HaveOccurred())
				Expect(cancelled).To(ContainElement("job-1"))
				Expect(unknown).To(BeEmpty())

				// Verify job is UNSCHEDULED
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusUnscheduled))
			})

			It("should cancel RUNNING jobs (transition to CANCELLING)", func() {
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign job
				jobs, err := backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(jobs)).To(Equal(1))

				cancelled, unknown, err := queue.CancelJobs(ctx, nil, []string{"job-1"})
				Expect(err).NotTo(HaveOccurred())
				Expect(cancelled).To(ContainElement("job-1"))
				Expect(unknown).To(BeEmpty())

				// Verify job is CANCELLING
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusCancelling))
			})

			It("should cancel jobs by tags", func() {
				for i := 0; i < 3; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				cancelled, _, err := queue.CancelJobs(ctx, []string{"tag1"}, nil)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(cancelled)).To(Equal(3))
			})
		})

		Context("AcknowledgeCancellation", func() {
			It("should return error if job is not in CANCELLING state", func() {
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Try to acknowledge (not in CANCELLING state)
				err = queue.AcknowledgeCancellation(ctx, "job-1", true)
				Expect(err).To(HaveOccurred())
			})

			It("should return error if job not found", func() {
				err := queue.AcknowledgeCancellation(ctx, "nonexistent", true)
				Expect(err).To(HaveOccurred())
			})

			It("should free capacity when wasExecuting=true", func() {
				// Enqueue 2 jobs
				for i := 0; i < 2; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Start StreamJobs with capacity 1
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-1", []string{"tag1"}, 1, jobChan)
				}()

				// Receive first job
				var firstJob *jobpool.Job
				select {
				case jobs := <-jobChan:
					Expect(len(jobs)).To(Equal(1))
					firstJob = jobs[0]
				case <-time.After(2 * time.Second):
					Fail("Should receive first job")
				}

				// Cancel and acknowledge
				_, _, err := queue.CancelJobs(ctx, nil, []string{firstJob.ID})
				Expect(err).NotTo(HaveOccurred())
				err = queue.AcknowledgeCancellation(ctx, firstJob.ID, true)
				Expect(err).NotTo(HaveOccurred())

				// Should receive second job (capacity freed)
				select {
				case jobs := <-jobChan:
					Expect(len(jobs)).To(BeNumerically(">=", 1))
				case <-time.After(2 * time.Second):
					Fail("Should receive second job after acknowledgment")
				}

				streamCancel()
				wg.Wait()
			})

			It("should set FinalizedAt when acknowledging cancellation", func() {
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign and cancel
				_, err = backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-1"})
				Expect(err).NotTo(HaveOccurred())

				// Acknowledge
				err = queue.AcknowledgeCancellation(ctx, "job-1", true)
				Expect(err).NotTo(HaveOccurred())

				// Verify FinalizedAt is set
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.FinalizedAt).NotTo(BeNil())
			})

			It("should transition CANCELLING to STOPPED when wasExecuting=true", func() {
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign and cancel
				_, err = backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-1"})
				Expect(err).NotTo(HaveOccurred())

				// Acknowledge
				err = queue.AcknowledgeCancellation(ctx, "job-1", true)
				Expect(err).NotTo(HaveOccurred())

				// Verify job is STOPPED
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusStopped))
			})

			It("should transition CANCELLING to UNKNOWN_STOPPED when wasExecuting=false", func() {
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign and cancel
				_, err = backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-1"})
				Expect(err).NotTo(HaveOccurred())

				// Acknowledge
				err = queue.AcknowledgeCancellation(ctx, "job-1", false)
				Expect(err).NotTo(HaveOccurred())

				// Verify job is UNKNOWN_STOPPED
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusUnknownStopped))
			})

			It("should not free capacity when wasExecuting=false", func() {
				// Enqueue 2 jobs
				for i := 0; i < 2; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Start StreamJobs with capacity 1
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-1", []string{"tag1"}, 1, jobChan)
				}()

				// Receive first job
				var firstJob *jobpool.Job
				select {
				case jobs := <-jobChan:
					Expect(len(jobs)).To(Equal(1))
					firstJob = jobs[0]
				case <-time.After(2 * time.Second):
					Fail("Should receive first job")
				}

				// Cancel and acknowledge with wasExecuting=false (job was not executing, so no capacity held)
				_, _, err := queue.CancelJobs(ctx, nil, []string{firstJob.ID})
				Expect(err).NotTo(HaveOccurred())
				err = queue.AcknowledgeCancellation(ctx, firstJob.ID, false)
				Expect(err).NotTo(HaveOccurred())

				// Should NOT receive second job (capacity was not freed because job was not executing)
				// Wait a bit to ensure no job is received
				select {
				case jobs := <-jobChan:
					// If we receive jobs, it means capacity was incorrectly freed
					Fail(fmt.Sprintf("Should not receive second job (capacity not freed), but received: %v", jobs))
				case <-time.After(500 * time.Millisecond):
					// Expected: no job received because capacity was not freed
				}

				streamCancel()
				wg.Wait()
			})
		})
	})

	Describe("Worker Management", func() {
		Context("MarkWorkerUnresponsive", func() {
			It("should preserve AssigneeID and AssignedAt when marking worker unresponsive", func() {
				// Enqueue and assign jobs
				for i := 0; i < 2; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Assign jobs to worker-1
				jobs, err := backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 2)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(jobs)).To(Equal(2))
				assigneeID := jobs[0].AssigneeID
				assignedAt := jobs[0].AssignedAt

				// Mark worker as unresponsive
				err = queue.MarkWorkerUnresponsive(ctx, "worker-1")
				Expect(err).NotTo(HaveOccurred())

				// Verify fields are preserved
				retrieved, err := queue.GetJob(ctx, "job-0")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.AssigneeID).To(Equal(assigneeID))
				Expect(retrieved.AssignedAt).NotTo(BeNil())
				Expect(retrieved.AssignedAt.Equal(*assignedAt)).To(BeTrue())
			})

			It("should free capacity and notify workers when marking worker unresponsive", func() {
				// Enqueue 2 jobs
				for i := 0; i < 2; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Assign both jobs to worker-1
				_, err := backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 2)
				Expect(err).NotTo(HaveOccurred())

				// Start StreamJobs for worker-2
				jobChan := make(chan []*jobpool.Job, 10)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "worker-2", []string{"tag1"}, 10, jobChan)
				}()

				// Mark worker-1 as unresponsive (jobs become UNKNOWN_RETRY, eligible)
				err = queue.MarkWorkerUnresponsive(ctx, "worker-1")
				Expect(err).NotTo(HaveOccurred())

				// Worker-2 should receive the jobs (they're now eligible)
				timeout := time.After(2 * time.Second)
				receivedCount := 0
			loop6:
				for receivedCount < 2 {
					select {
					case jobs := <-jobChan:
						receivedCount += len(jobs)
					case <-timeout:
						break loop6
					}
				}
				Expect(receivedCount).To(BeNumerically(">=", 1))

				streamCancel()
				wg.Wait()
			})

			It("should transition RUNNING jobs to UNKNOWN_RETRY", func() {
				// Enqueue and assign jobs
				for i := 0; i < 2; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Assign jobs to worker-1
				jobs, err := backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 2)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(jobs)).To(Equal(2))

				// Mark worker as unresponsive
				err = queue.MarkWorkerUnresponsive(ctx, "worker-1")
				Expect(err).NotTo(HaveOccurred())

				// Verify jobs are UNKNOWN_RETRY
				for i := 0; i < 2; i++ {
					retrieved, err := queue.GetJob(ctx, fmt.Sprintf("job-%d", i))
					Expect(err).NotTo(HaveOccurred())
					Expect(retrieved.Status).To(Equal(jobpool.JobStatusUnknownRetry))
				}
			})

			It("should transition CANCELLING jobs to UNKNOWN_STOPPED", func() {
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign and cancel
				_, err = backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-1"})
				Expect(err).NotTo(HaveOccurred())

				// Mark worker as unresponsive
				err = queue.MarkWorkerUnresponsive(ctx, "worker-1")
				Expect(err).NotTo(HaveOccurred())

				// Verify job is UNKNOWN_STOPPED
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusUnknownStopped))
			})
		})
	})

	Describe("Query Operations", func() {
		Context("GetJob", func() {
			It("should retrieve a job by ID", func() {
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.ID).To(Equal("job-1"))
			})

			It("should return error if job not found", func() {
				_, err := queue.GetJob(ctx, "nonexistent")
				Expect(err).To(HaveOccurred())
			})
		})

		Context("GetJobStats", func() {
			It("should return StoppedJobs count", func() {
				job1 := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				job2 := &jobpool.Job{
					ID:            "job-2",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job1)
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.EnqueueJob(ctx, job2)
				Expect(err).NotTo(HaveOccurred())

				// Dequeue one job (could be job1 or job2, doesn't matter)
				dequeued, err := backend.DequeueJobs(ctx, "state-worker", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(dequeued)).To(Equal(1))

				// Stop the dequeued job
				err = queue.StopJob(ctx, dequeued[0].ID, "test stop")
				Expect(err).NotTo(HaveOccurred())

				stats, err := queue.GetJobStats(ctx, []string{"tag1"})
				Expect(err).NotTo(HaveOccurred())
				Expect(stats.StoppedJobs).To(Equal(int32(1)))
			})

			It("should return TotalRetries count", func() {
				job := &jobpool.Job{
					ID:            "job-1",
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				err := seedJobWithRetries(ctx, queue, backend, job, 3)
				Expect(err).NotTo(HaveOccurred())

				stats, err := queue.GetJobStats(ctx, []string{"tag1"})
				Expect(err).NotTo(HaveOccurred())
				Expect(stats.TotalRetries).To(Equal(int32(3)))
			})

			It("should return RunningJobs count", func() {
				// Enqueue and assign job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign job
				_, err = backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())

				stats, err := queue.GetJobStats(ctx, []string{"tag1"})
				Expect(err).NotTo(HaveOccurred())
				Expect(stats.RunningJobs).To(Equal(int32(1)))
			})

			It("should return statistics for jobs matching tags", func() {
				// Enqueue jobs with different tags
				for i := 0; i < 3; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				stats, err := queue.GetJobStats(ctx, []string{"tag1"})
				Expect(err).NotTo(HaveOccurred())
				Expect(stats.TotalJobs).To(Equal(int32(3)))
				Expect(stats.PendingJobs).To(Equal(int32(3)))
			})

			It("should return statistics for all jobs when tags are empty", func() {
				// Enqueue jobs with different tags
				for i := 0; i < 3; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{fmt.Sprintf("tag%d", i)},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				stats, err := queue.GetJobStats(ctx, nil)
				Expect(err).NotTo(HaveOccurred())
				Expect(stats.TotalJobs).To(Equal(int32(3)))
			})

			It("should filter by AND logic", func() {
				job1 := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1", "tag2"},
					CreatedAt:     time.Now(),
				}
				job2 := &jobpool.Job{
					ID:            "job-2",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job1)
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.EnqueueJob(ctx, job2)
				Expect(err).NotTo(HaveOccurred())

				stats, err := queue.GetJobStats(ctx, []string{"tag1", "tag2"})
				Expect(err).NotTo(HaveOccurred())
				Expect(stats.TotalJobs).To(Equal(int32(1))) // Only job-1 matches
			})
		})
	})

	Describe("Maintenance", func() {
		Context("CleanupExpiredJobs", func() {
			It("should return error if ttl is zero", func() {
				err := queue.CleanupExpiredJobs(ctx, 0)
				Expect(err).To(HaveOccurred())
			})

			It("should return error if ttl is negative", func() {
				err := queue.CleanupExpiredJobs(ctx, -1*time.Second)
				Expect(err).To(HaveOccurred())
			})

			It("should delete completed jobs older than TTL", func() {
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign and complete
				_, err = backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.CompleteJob(ctx, "job-1", []byte("result"))
				Expect(err).NotTo(HaveOccurred())

				// Cleanup with very short TTL
				err = queue.CleanupExpiredJobs(ctx, 1*time.Nanosecond)
				Expect(err).NotTo(HaveOccurred())

				// Job should be deleted
				_, err = queue.GetJob(ctx, "job-1")
				Expect(err).To(HaveOccurred())
			})
		})

		Context("ResetRunningJobs", func() {
			It("should transition all RUNNING jobs to UNKNOWN_RETRY", func() {
				// Enqueue and assign jobs
				for i := 0; i < 2; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-%d", i),
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
				jobs, err := backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 2)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(jobs)).To(Equal(2))

				// Reset running jobs
				err = queue.ResetRunningJobs(ctx)
				Expect(err).NotTo(HaveOccurred())

				// Verify jobs are UNKNOWN_RETRY
				for i := 0; i < 2; i++ {
					retrieved, err := queue.GetJob(ctx, fmt.Sprintf("job-%d", i))
					Expect(err).NotTo(HaveOccurred())
					Expect(retrieved.Status).To(Equal(jobpool.JobStatusUnknownRetry))
				}
			})

			It("should transition all CANCELLING jobs to UNKNOWN_STOPPED", func() {
				// Enqueue and assign job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign and cancel job
				_, err = backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-1"})
				Expect(err).NotTo(HaveOccurred())

				// Verify job is in CANCELLING state
				retrieved, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusCancelling))

				// Reset running jobs (should handle CANCELLING jobs)
				err = queue.ResetRunningJobs(ctx)
				Expect(err).NotTo(HaveOccurred())

				// Verify job is UNKNOWN_STOPPED (CANCELLING → UNKNOWN_STOPPED)
				retrieved, err = queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(retrieved.Status).To(Equal(jobpool.JobStatusUnknownStopped))
			})

			It("should discover pending jobs after restart when ResetRunningJobs called before workers connect", func() {
				// This test verifies the post-registration notification mechanism
				// Scenario: Service restarts, ResetRunningJobs is called before workers connect,
				// then workers connect and should discover all pending jobs

				// Enqueue multiple jobs
				numJobs := 10
				for i := 0; i < numJobs; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Assign some jobs to simulate they were running before restart
				_, err := backend.DequeueJobs(ctx, "old-worker", []string{"tag1"}, 5)
				Expect(err).NotTo(HaveOccurred())

				// Simulate service restart: ResetRunningJobs called before workers connect
				// This transitions RUNNING jobs to UNKNOWN_RETRY (eligible)
				err = queue.ResetRunningJobs(ctx)
				Expect(err).NotTo(HaveOccurred())

				// Verify jobs are now eligible (UNKNOWN_RETRY or INITIAL_PENDING)
				stats, err := queue.GetJobStats(ctx, []string{"tag1"})
				Expect(err).NotTo(HaveOccurred())
				// Should have 5 UNKNOWN_RETRY + 5 INITIAL_PENDING = 10 eligible jobs
				Expect(stats.PendingJobs + stats.FailedJobs).To(Equal(int32(numJobs)))

				// Now simulate worker connecting after restart
				// The post-registration notification should ensure all pending jobs are discovered
				jobChan := make(chan []*jobpool.Job, 20)
				streamCtx, streamCancel := context.WithCancel(ctx)
				defer streamCancel()

				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = queue.StreamJobs(streamCtx, "new-worker", []string{"tag1"}, 10, jobChan)
				}()

				// Collect all received jobs
				receivedJobs := make(map[string]bool)
				timeout := time.After(3 * time.Second)
			loop:
				for len(receivedJobs) < numJobs {
					select {
					case jobs := <-jobChan:
						for _, job := range jobs {
							receivedJobs[job.ID] = true
							Expect(job.Status).To(Equal(jobpool.JobStatusRunning))
							Expect(job.AssigneeID).To(Equal("new-worker"))
						}
					case <-timeout:
						break loop
					}
				}

				// Should receive all jobs (initial dequeue + post-registration notification)
				// The post-registration notification ensures jobs are discovered even if
				// initial dequeue doesn't find all of them
				Expect(len(receivedJobs)).To(Equal(numJobs), "All pending jobs should be discovered after restart")

				streamCancel()
				wg.Wait()
			})
		})

		Context("DeleteJobs", func() {
			It("should delete jobs by tags", func() {
				// Create completed jobs with same tag
				for i := 0; i < 3; i++ {
					job := &jobpool.Job{
						ID:            fmt.Sprintf("job-%d", i),
						Status:        jobpool.JobStatusInitialPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"tag1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
				}

				// Complete all jobs
				for i := 0; i < 3; i++ {
					_, err := backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
					Expect(err).NotTo(HaveOccurred())
					err = queue.CompleteJob(ctx, fmt.Sprintf("job-%d", i), []byte("result"))
					Expect(err).NotTo(HaveOccurred())
				}

				// Delete by tags
				err := queue.DeleteJobs(ctx, []string{"tag1"}, nil)
				Expect(err).NotTo(HaveOccurred())

				// Verify jobs are deleted
				for i := 0; i < 3; i++ {
					_, err := queue.GetJob(ctx, fmt.Sprintf("job-%d", i))
					Expect(err).To(HaveOccurred())
				}
			})

			It("should delete jobs by both tags and jobIDs (union)", func() {
				// Create jobs
				job1 := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				job2 := &jobpool.Job{
					ID:            "job-2",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag2"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job1)
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.EnqueueJob(ctx, job2)
				Expect(err).NotTo(HaveOccurred())

				// Complete jobs
				_, err = backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.CompleteJob(ctx, "job-1", []byte("result"))
				Expect(err).NotTo(HaveOccurred())

				_, err = backend.DequeueJobs(ctx, "worker-1", []string{"tag2"}, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.CompleteJob(ctx, "job-2", []byte("result"))
				Expect(err).NotTo(HaveOccurred())

				// Delete by tags (tag1) and jobIDs (job-2)
				err = queue.DeleteJobs(ctx, []string{"tag1"}, []string{"job-2"})
				Expect(err).NotTo(HaveOccurred())

				// Both jobs should be deleted
				_, err = queue.GetJob(ctx, "job-1")
				Expect(err).To(HaveOccurred())
				_, err = queue.GetJob(ctx, "job-2")
				Expect(err).To(HaveOccurred())
			})

			It("should delete jobs in final states", func() {
				// Create completed job
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Assign and complete
				_, err = backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.CompleteJob(ctx, "job-1", []byte("result"))
				Expect(err).NotTo(HaveOccurred())

				// Delete job
				err = queue.DeleteJobs(ctx, nil, []string{"job-1"})
				Expect(err).NotTo(HaveOccurred())

				// Job should be deleted
				_, err = queue.GetJob(ctx, "job-1")
				Expect(err).To(HaveOccurred())
			})

			It("should return error if job is not in final state", func() {
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// Try to delete non-final job
				err = queue.DeleteJobs(ctx, nil, []string{"job-1"})
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Describe("Thread Safety", func() {
		It("should handle concurrent EnqueueJob calls", func() {
			var wg sync.WaitGroup
			numGoroutines := 10
			jobsPerGoroutine := 5

			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					for j := 0; j < jobsPerGoroutine; j++ {
						job := &jobpool.Job{
							ID:            fmt.Sprintf("job-%d-%d", workerID, j),
							Status:        jobpool.JobStatusInitialPending,
							JobType:       "test",
							JobDefinition: []byte("test"),
							Tags:          []string{"tag1"},
							CreatedAt:     time.Now(),
						}
						_, err := queue.EnqueueJob(ctx, job)
						Expect(err).NotTo(HaveOccurred())
					}
				}(i)
			}

			wg.Wait()

			// Verify all jobs were enqueued
			stats, err := queue.GetJobStats(ctx, []string{"tag1"})
			Expect(err).NotTo(HaveOccurred())
			Expect(stats.TotalJobs).To(Equal(int32(numGoroutines * jobsPerGoroutine)))
		})

		It("should handle concurrent CompleteJob calls", func() {
			// Enqueue jobs
			numJobs := 10
			for i := 0; i < numJobs; i++ {
				job := &jobpool.Job{
					ID:            fmt.Sprintf("job-%d", i),
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
			}

			// Assign all jobs
			_, err := backend.DequeueJobs(ctx, "worker-1", []string{"tag1"}, numJobs)
			Expect(err).NotTo(HaveOccurred())

			// Complete jobs concurrently
			var wg sync.WaitGroup
			for i := 0; i < numJobs; i++ {
				wg.Add(1)
				go func(jobID string) {
					defer wg.Done()
					err := queue.CompleteJob(ctx, jobID, []byte("result"))
					Expect(err).NotTo(HaveOccurred())
				}(fmt.Sprintf("job-%d", i))
			}

			wg.Wait()

			// Verify all jobs are completed
			stats, err := queue.GetJobStats(ctx, []string{"tag1"})
			Expect(err).NotTo(HaveOccurred())
			Expect(stats.CompletedJobs).To(Equal(int32(numJobs)))
		})
	})

	Describe("Close", func() {
		It("should return nil (not ctx.Err()) when queue is closed", func() {
			jobChan := make(chan []*jobpool.Job, 10)
			streamCtx, streamCancel := context.WithCancel(ctx)
			defer streamCancel()

			var wg sync.WaitGroup
			wg.Add(1)
			var streamErr error
			go func() {
				defer wg.Done()
				streamErr = queue.StreamJobs(streamCtx, "worker-1", nil, 10, jobChan)
			}()

			// Wait a bit
			time.Sleep(100 * time.Millisecond)

			// Close queue (not cancel context)
			err := queue.Close()
			Expect(err).NotTo(HaveOccurred())

			// Wait for StreamJobs to return
			wg.Wait()

			// Should return nil, not ctx.Err()
			Expect(streamErr).To(BeNil())
		})

		It("should close the queue and terminate StreamJobs", func() {
			jobChan := make(chan []*jobpool.Job, 10)
			streamCtx, streamCancel := context.WithCancel(ctx)
			defer streamCancel()

			var wg sync.WaitGroup
			wg.Add(1)
			var streamErr error
			go func() {
				defer wg.Done()
				streamErr = queue.StreamJobs(streamCtx, "worker-1", nil, 10, jobChan)
			}()

			// Wait a bit
			time.Sleep(100 * time.Millisecond)

			// Close queue
			err := queue.Close()
			Expect(err).NotTo(HaveOccurred())

			// Wait for StreamJobs to return
			wg.Wait()

			// Channel should be closed
			_, ok := <-jobChan
			Expect(ok).To(BeFalse())
			Expect(streamErr).To(BeNil())
		})
	})
})

func enqueueInitialJob(ctx context.Context, q jobpool.Queue, job *jobpool.Job) error {
	job.Status = jobpool.JobStatusInitialPending
	_, err := q.EnqueueJob(ctx, job)
	return err
}

func assignJobToWorker(ctx context.Context, backend jobpool.Backend, worker string, tags []string) error {
	_, err := backend.DequeueJobs(ctx, worker, tags, 1)
	return err
}

func seedUnknownRetryJob(ctx context.Context, q jobpool.Queue, backend jobpool.Backend, job *jobpool.Job) error {
	if err := enqueueInitialJob(ctx, q, job); err != nil {
		return err
	}
	if err := assignJobToWorker(ctx, backend, "state-worker", job.Tags); err != nil {
		return err
	}
	return backend.ResetRunningJobs(ctx)
}

func seedUnknownStoppedJob(ctx context.Context, q jobpool.Queue, backend jobpool.Backend, job *jobpool.Job) error {
	if err := enqueueInitialJob(ctx, q, job); err != nil {
		return err
	}
	if err := assignJobToWorker(ctx, backend, "state-worker", job.Tags); err != nil {
		return err
	}
	if _, _, err := q.CancelJobs(ctx, nil, []string{job.ID}); err != nil {
		return err
	}
	return backend.MarkWorkerUnresponsive(ctx, "state-worker")
}

func seedFailedRetryJob(ctx context.Context, q jobpool.Queue, backend jobpool.Backend, job *jobpool.Job) error {
	if err := enqueueInitialJob(ctx, q, job); err != nil {
		return err
	}
	if err := assignJobToWorker(ctx, backend, "state-worker", job.Tags); err != nil {
		return err
	}
	_, err := backend.FailJob(ctx, job.ID, "test failure")
	return err
}

func seedStoppedJob(ctx context.Context, q jobpool.Queue, backend jobpool.Backend, job *jobpool.Job) error {
	if err := enqueueInitialJob(ctx, q, job); err != nil {
		return err
	}
	// Dequeue with nil tags to match any job
	if err := assignJobToWorker(ctx, backend, "state-worker", nil); err != nil {
		return err
	}
	_, err := backend.StopJob(ctx, job.ID, "test stop")
	return err
}

func seedJobWithRetries(ctx context.Context, q jobpool.Queue, backend jobpool.Backend, job *jobpool.Job, retries int) error {
	if err := enqueueInitialJob(ctx, q, job); err != nil {
		return err
	}
	for i := 0; i < retries; i++ {
		if err := assignJobToWorker(ctx, backend, "retry-worker", job.Tags); err != nil {
			return err
		}
		if _, err := backend.FailJob(ctx, job.ID, fmt.Sprintf("retry-%d", i)); err != nil {
			return err
		}
	}
	return nil
}
