package jobpool_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VsevolodSauta/jobpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// testLogger creates a logger for tests (discards output)
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelError, // Only show errors in tests
	}))
}

// BackendTestSuite runs a comprehensive test suite against a Backend implementation
func BackendTestSuite(backendFactory func() (jobpool.Backend, func())) {
	var backend jobpool.Backend
	var cleanup func()
	var ctx context.Context

	BeforeEach(func() {
		backend, cleanup = backendFactory()
		ctx = context.Background()
	})

	AfterEach(func() {
		if cleanup != nil {
			cleanup()
		}
	})

	Describe("EnqueueJob", func() {
		It("should enqueue a job successfully", func() {
			job := &jobpool.Job{
				ID:            "job-1",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("test data"),
				Tags:          []string{"tag1", "tag2"},
				CreatedAt:     time.Now(),
			}

			jobID, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			Expect(jobID).To(Equal("job-1"))
		})

		It("should retrieve enqueued job", func() {
			job := &jobpool.Job{
				ID:            "job-1",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("test data"),
				Tags:          []string{"tag1"},
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			retrieved, err := backend.GetJob(ctx, "job-1")
			Expect(err).NotTo(HaveOccurred())
			Expect(retrieved.ID).To(Equal("job-1"))
			Expect(retrieved.Status).To(Equal(jobpool.JobStatusInitialPending))
			Expect(retrieved.JobType).To(Equal("test"))
			Expect(retrieved.JobDefinition).To(Equal([]byte("test data")))
			Expect(retrieved.Tags).To(ContainElement("tag1"))
		})

		It("should return error for duplicate job ID", func() {
			job := &jobpool.Job{
				ID:            "job-1",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("test data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			// Try to enqueue same job again
			_, err = backend.EnqueueJob(ctx, job)
			Expect(err).To(HaveOccurred())
		})

		It("should return error for nil job", func() {
			_, err := backend.EnqueueJob(ctx, nil)
			Expect(err).To(HaveOccurred())
		})

		It("should return error for empty job ID", func() {
			job := &jobpool.Job{
				ID:            "",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("test data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).To(HaveOccurred())
		})

		It("should return error for invalid status", func() {
			job := &jobpool.Job{
				ID:            "job-1",
				Status:        jobpool.JobStatusRunning, // Invalid - must be INITIAL_PENDING
				JobType:       "test",
				JobDefinition: []byte("test data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).To(HaveOccurred())
		})

		It("should handle context cancellation", func() {
			cancelCtx, cancel := context.WithCancel(ctx)
			cancel() // Cancel immediately

			job := &jobpool.Job{
				ID:            "job-1",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("test data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(cancelCtx, job)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(context.Canceled))
		})

		It("should make job available for DequeueJobs immediately", func() {
			job := &jobpool.Job{
				ID:            "job-1",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("test data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			// Job should be immediately available for dequeue
			jobs, err := backend.DequeueJobs(ctx, "assignee-1", nil, 10)
			Expect(err).NotTo(HaveOccurred())
			Expect(jobs).To(HaveLen(1))
			Expect(jobs[0].ID).To(Equal("job-1"))
		})

		It("should make job appear in GetJobStats immediately", func() {
			job := &jobpool.Job{
				ID:            "job-1",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("test data"),
				Tags:          []string{"tag1"},
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			stats, err := backend.GetJobStats(ctx, []string{"tag1"})
			Expect(err).NotTo(HaveOccurred())
			Expect(stats.TotalJobs).To(Equal(int32(1)))
			Expect(stats.PendingJobs).To(Equal(int32(1)))
		})

		It("should preserve all job fields", func() {
			createdAt := time.Now().Add(-1 * time.Hour)
			job := &jobpool.Job{
				ID:            "job-1",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test-type",
				JobDefinition: []byte("test data"),
				Tags:          []string{"tag1", "tag2", "tag3"},
				CreatedAt:     createdAt,
				RetryCount:    0,
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			retrieved, err := backend.GetJob(ctx, "job-1")
			Expect(err).NotTo(HaveOccurred())
			Expect(retrieved.ID).To(Equal("job-1"))
			Expect(retrieved.JobType).To(Equal("test-type"))
			Expect(retrieved.JobDefinition).To(Equal([]byte("test data")))
			Expect(retrieved.Tags).To(HaveLen(3))
			Expect(retrieved.Tags).To(ContainElement("tag1"))
			Expect(retrieved.Tags).To(ContainElement("tag2"))
			Expect(retrieved.Tags).To(ContainElement("tag3"))
			Expect(retrieved.CreatedAt.Unix()).To(Equal(createdAt.Unix()))
			Expect(retrieved.RetryCount).To(Equal(0))
		})
	})

	Describe("EnqueueJobs", func() {
		It("should enqueue multiple jobs", func() {
			jobs := []*jobpool.Job{
				{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("data1"),
					CreatedAt:     time.Now(),
				},
				{
					ID:            "job-2",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("data2"),
					CreatedAt:     time.Now(),
				},
			}

			jobIDs, err := backend.EnqueueJobs(ctx, jobs)
			Expect(err).NotTo(HaveOccurred())
			Expect(jobIDs).To(HaveLen(2))
			Expect(jobIDs).To(ContainElement("job-1"))
			Expect(jobIDs).To(ContainElement("job-2"))
		})

		It("should handle empty job slice", func() {
			jobIDs, err := backend.EnqueueJobs(ctx, []*jobpool.Job{})
			Expect(err).NotTo(HaveOccurred())
			Expect(jobIDs).To(BeEmpty())
		})

		It("should return error when batch contains nil job", func() {
			jobs := []*jobpool.Job{
				{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("data1"),
					CreatedAt:     time.Now(),
				},
				nil,
			}

			_, err := backend.EnqueueJobs(ctx, jobs)
			Expect(err).To(HaveOccurred())
		})

		It("should return error for duplicate IDs within batch", func() {
			now := time.Now()
			jobs := []*jobpool.Job{
				{
					ID:            "job-dup",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("data1"),
					CreatedAt:     now,
				},
				{
					ID:            "job-dup",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("data2"),
					CreatedAt:     now,
				},
			}

			_, err := backend.EnqueueJobs(ctx, jobs)
			Expect(err).To(HaveOccurred())
		})

		It("should return error if any job already exists", func() {
			existing := &jobpool.Job{
				ID:            "job-existing",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("existing"),
				CreatedAt:     time.Now(),
			}
			_, err := backend.EnqueueJob(ctx, existing)
			Expect(err).NotTo(HaveOccurred())

			jobs := []*jobpool.Job{
				existing,
				{
					ID:            "job-new",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("new"),
					CreatedAt:     time.Now(),
				},
			}

			_, err = backend.EnqueueJobs(ctx, jobs)
			Expect(err).To(HaveOccurred())

			// Ensure no new job was created (atomicity)
			_, err = backend.GetJob(ctx, "job-new")
			Expect(err).To(HaveOccurred())
		})

		It("should return IDs in same order as input", func() {
			jobs := []*jobpool.Job{
				{
					ID:            "job-first",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("data1"),
					CreatedAt:     time.Now(),
				},
				{
					ID:            "job-second",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("data2"),
					CreatedAt:     time.Now(),
				},
			}

			jobIDs, err := backend.EnqueueJobs(ctx, jobs)
			Expect(err).NotTo(HaveOccurred())
			Expect(jobIDs).To(Equal([]string{"job-first", "job-second"}))
		})

		It("should handle context cancellation", func() {
			cancelCtx, cancel := context.WithCancel(ctx)
			cancel()

			jobs := []*jobpool.Job{
				{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("data1"),
					CreatedAt:     time.Now(),
				},
			}

			_, err := backend.EnqueueJobs(cancelCtx, jobs)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(context.Canceled))
		})

		It("should be more efficient than multiple EnqueueJob calls (batch)", func() {
			// Observable effect: all jobs should be available immediately
			jobs := []*jobpool.Job{
				{
					ID:            "job-batch-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("data1"),
					CreatedAt:     time.Now(),
				},
				{
					ID:            "job-batch-2",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("data2"),
					CreatedAt:     time.Now(),
				},
			}

			jobIDs, err := backend.EnqueueJobs(ctx, jobs)
			Expect(err).NotTo(HaveOccurred())
			Expect(jobIDs).To(HaveLen(2))

			stats, err := backend.GetJobStats(ctx, []string{})
			Expect(err).NotTo(HaveOccurred())
			Expect(stats.TotalJobs).To(BeNumerically(">=", 2))
		})

		It("should make all jobs immediately available for DequeueJobs", func() {
			jobs := []*jobpool.Job{
				{
					ID:            "job-batch-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("data1"),
					CreatedAt:     time.Now(),
				},
				{
					ID:            "job-batch-2",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("data2"),
					CreatedAt:     time.Now(),
				},
			}

			_, err := backend.EnqueueJobs(ctx, jobs)
			Expect(err).NotTo(HaveOccurred())

			dequeued, err := backend.DequeueJobs(ctx, "assignee-1", nil, 10)
			Expect(err).NotTo(HaveOccurred())
			Expect(dequeued).To(HaveLen(2))
			Expect([]string{dequeued[0].ID, dequeued[1].ID}).To(ContainElements("job-batch-1", "job-batch-2"))
		})
	})

	Describe("DequeueJobs", func() {
		It("should dequeue pending jobs", func() {
			job := &jobpool.Job{
				ID:            "job-1",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("test data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			jobs, err := backend.DequeueJobs(ctx, "assignee-1", nil, 10)
			Expect(err).NotTo(HaveOccurred())
			Expect(jobs).To(HaveLen(1))
			Expect(jobs[0].ID).To(Equal("job-1"))
			Expect(jobs[0].Status).To(Equal(jobpool.JobStatusRunning))
			Expect(jobs[0].AssigneeID).To(Equal("assignee-1"))
			Expect(jobs[0].AssignedAt).NotTo(BeNil())
			Expect(jobs[0].Tags).To(BeEmpty()) // Job had no tags
		})

		It("should filter by tags", func() {
			job1 := &jobpool.Job{
				ID:            "job-1",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data1"),
				Tags:          []string{"tag1"},
				CreatedAt:     time.Now(),
			}
			job2 := &jobpool.Job{
				ID:            "job-2",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data2"),
				Tags:          []string{"tag2"},
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job1)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.EnqueueJob(ctx, job2)
			Expect(err).NotTo(HaveOccurred())

			jobs, err := backend.DequeueJobs(ctx, "assignee-1", []string{"tag1"}, 10)
			Expect(err).NotTo(HaveOccurred())
			Expect(jobs).To(HaveLen(1))
			Expect(jobs[0].ID).To(Equal("job-1"))
			Expect(jobs[0].Tags).To(ContainElement("tag1")) // Tags should be populated in returned jobs
		})

		It("should respect limit", func() {
			for i := 0; i < 5; i++ {
				job := &jobpool.Job{
					ID:            "job-" + string(rune('1'+i)),
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("data"),
					CreatedAt:     time.Now(),
				}
				_, err := backend.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
			}

			jobs, err := backend.DequeueJobs(ctx, "assignee-1", nil, 3)
			Expect(err).NotTo(HaveOccurred())
			Expect(jobs).To(HaveLen(3))
		})

		It("should return error for empty assigneeID", func() {
			_, err := backend.DequeueJobs(ctx, "", nil, 1)
			Expect(err).To(HaveOccurred())
		})

		It("should return error for non-positive limit", func() {
			job := &jobpool.Job{
				ID:            "job-limit",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}
			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 0)
			Expect(err).To(HaveOccurred())
		})

		It("should handle context cancellation", func() {
			job := &jobpool.Job{
				ID:            "job-cancel",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}
			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			cancelCtx, cancel := context.WithCancel(ctx)
			cancel()

			_, err = backend.DequeueJobs(cancelCtx, "assignee-1", nil, 1)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(context.Canceled))
		})

		It("should treat empty tags as no filtering", func() {
			job := &jobpool.Job{
				ID:            "job-no-tags",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				Tags:          []string{"tag1"},
				CreatedAt:     time.Now(),
			}
			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			jobs, err := backend.DequeueJobs(ctx, "assignee-1", []string{}, 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(jobs).To(HaveLen(1))
			Expect(jobs[0].ID).To(Equal("job-no-tags"))
		})

		It("should only select jobs with ALL provided tags", func() {
			job := &jobpool.Job{
				ID:            "job-tags",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				Tags:          []string{"tag1", "tag2"},
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			jobs, err := backend.DequeueJobs(ctx, "assignee-1", []string{"tag1", "tag2"}, 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(jobs).To(HaveLen(1))
			Expect(jobs[0].ID).To(Equal("job-tags"))
		})

		It("should not assign jobs in non-eligible states", func() {
			job := &jobpool.Job{
				ID:            "job-non-eligible",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			// Assign job (now RUNNING)
			jobs, err := backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(jobs).To(HaveLen(1))

			// Attempt to assign again (should not be reassigned)
			jobs, err = backend.DequeueJobs(ctx, "assignee-2", nil, 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(jobs).To(BeEmpty())

			// Complete job so it becomes terminal
			_, err = backend.CompleteJob(ctx, "job-non-eligible", []byte("done"))
			Expect(err).NotTo(HaveOccurred())

			// Completed job should not be assigned again
			jobs, err = backend.DequeueJobs(ctx, "assignee-3", nil, 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(jobs).To(BeEmpty())
		})

		It("should set StartedAt when assigning job", func() {
			job := &jobpool.Job{
				ID:            "job-started-at",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			jobs, err := backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(jobs).To(HaveLen(1))
			Expect(jobs[0].StartedAt).NotTo(BeNil())
		})

		It("should allow FAILED_RETRY jobs to be scheduled", func() {
			job := &jobpool.Job{
				ID:            "job-failed-retry",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			// Move job to FAILED_RETRY
			jobs, err := backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(jobs).To(HaveLen(1))

			_, err = backend.FailJob(ctx, "job-failed-retry", "error")
			Expect(err).NotTo(HaveOccurred())

			// Should be eligible again
			requeued, err := backend.DequeueJobs(ctx, "assignee-2", nil, 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(requeued).To(HaveLen(1))
			Expect(requeued[0].ID).To(Equal("job-failed-retry"))
			Expect(requeued[0].AssigneeID).To(Equal("assignee-2"))
		})

		It("should allow UNKNOWN_RETRY jobs to be scheduled", func() {
			job := &jobpool.Job{
				ID:            "job-unknown-retry",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())

			// Simulate worker unresponsive
			err = backend.MarkWorkerUnresponsive(ctx, "assignee-1")
			Expect(err).NotTo(HaveOccurred())

			requeued, err := backend.DequeueJobs(ctx, "assignee-2", nil, 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(requeued).To(HaveLen(1))
			Expect(requeued[0].ID).To(Equal("job-unknown-retry"))
			Expect(requeued[0].Status).To(Equal(jobpool.JobStatusRunning))
		})

		It("should prevent same job from being assigned twice concurrently", func() {
			job := &jobpool.Job{
				ID:            "job-concurrent",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			done := make(chan string, 2)

			for i := 0; i < 2; i++ {
				assignee := fmt.Sprintf("worker-%d", i)
				go func(a string) {
					jobs, err := backend.DequeueJobs(ctx, a, nil, 1)
					if err == nil && len(jobs) > 0 {
						done <- jobs[0].AssigneeID
					} else {
						done <- ""
					}
				}(assignee)
			}

			assignee1 := <-done
			assignee2 := <-done

			// Exactly one assignee should have received the job
			Expect((assignee1 == "") != (assignee2 == "")).To(BeTrue())
		})

		It("should return empty slice when no eligible jobs", func() {
			jobs, err := backend.DequeueJobs(ctx, "assignee-1", nil, 10)
			Expect(err).NotTo(HaveOccurred())
			Expect(jobs).To(BeEmpty())
		})
	})

	Describe("CompleteJob", func() {
		It("should complete a job and return freed assignee ID", func() {
			job := &jobpool.Job{
				ID:            "job-1",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("test data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			jobs, err := backend.DequeueJobs(ctx, "assignee-1", nil, 10)
			Expect(err).NotTo(HaveOccurred())
			Expect(jobs).To(HaveLen(1))

			freedAssigneeIDs, err := backend.CompleteJob(ctx, "job-1", []byte("result"))
			Expect(err).NotTo(HaveOccurred())
			Expect(freedAssigneeIDs).To(Equal(map[string]int{"assignee-1": 1}))

			completed, err := backend.GetJob(ctx, "job-1")
			Expect(err).NotTo(HaveOccurred())
			Expect(completed.Status).To(Equal(jobpool.JobStatusCompleted))
			Expect(completed.Result).To(Equal([]byte("result")))
			// AssigneeID is preserved for historical tracking (not cleared)
			Expect(completed.AssigneeID).To(Equal("assignee-1"))
		})

		It("should return error if job was not assigned", func() {
			job := &jobpool.Job{
				ID:            "job-1",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("test data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			_, err = backend.CompleteJob(ctx, "job-1", []byte("result"))
			Expect(err).To(HaveOccurred()) // Job must be in RUNNING state to complete
		})

		It("should complete a job from CANCELLING state", func() {
			job := &jobpool.Job{
				ID:            "job-cancelling",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())

			cancelled, unknown, err := backend.CancelJobs(ctx, nil, []string{"job-cancelling"})
			Expect(err).NotTo(HaveOccurred())
			Expect(cancelled).To(ContainElement("job-cancelling"))
			Expect(unknown).To(BeEmpty())

			freedAssigneeIDs, err := backend.CompleteJob(ctx, "job-cancelling", []byte("done"))
			Expect(err).NotTo(HaveOccurred())
			Expect(freedAssigneeIDs).To(Equal(map[string]int{"assignee-1": 1}))

			completed, err := backend.GetJob(ctx, "job-cancelling")
			Expect(err).NotTo(HaveOccurred())
			Expect(completed.Status).To(Equal(jobpool.JobStatusCompleted))
			Expect(completed.Result).To(Equal([]byte("done")))
		})

		It("should complete a job from UNKNOWN_RETRY state", func() {
			job := &jobpool.Job{
				ID:            "job-unknown-retry-complete",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())

			err = backend.MarkWorkerUnresponsive(ctx, "assignee-1")
			Expect(err).NotTo(HaveOccurred())

			freedAssigneeIDs, err := backend.CompleteJob(ctx, "job-unknown-retry-complete", []byte("done"))
			Expect(err).NotTo(HaveOccurred())
			Expect(freedAssigneeIDs).To(Equal(map[string]int{}))

			completed, err := backend.GetJob(ctx, "job-unknown-retry-complete")
			Expect(err).NotTo(HaveOccurred())
			Expect(completed.Status).To(Equal(jobpool.JobStatusCompleted))
		})

		It("should return error for job not found", func() {
			_, err := backend.CompleteJob(ctx, "does-not-exist", []byte("result"))
			Expect(err).To(HaveOccurred())
		})

		It("should return error for invalid state", func() {
			job := &jobpool.Job{
				ID:            "job-invalid-complete",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			_, err = backend.CompleteJob(ctx, "job-invalid-complete", []byte("result"))
			Expect(err).To(HaveOccurred())
		})

		It("should propagate context cancellation", func() {
			job := &jobpool.Job{
				ID:            "job-complete-cancel",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())

			cancelCtx, cancel := context.WithCancel(ctx)
			cancel()

			_, err = backend.CompleteJob(cancelCtx, "job-complete-cancel", []byte("result"))
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(context.Canceled))
		})
	})

	Describe("FailJob", func() {
		It("should fail a job and return freed assignee ID", func() {
			job := &jobpool.Job{
				ID:            "job-1",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("test data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			jobs, err := backend.DequeueJobs(ctx, "assignee-1", nil, 10)
			Expect(err).NotTo(HaveOccurred())
			Expect(jobs).To(HaveLen(1))

			freedAssigneeIDs, err := backend.FailJob(ctx, "job-1", "error message")
			Expect(err).NotTo(HaveOccurred())
			Expect(freedAssigneeIDs).To(Equal(map[string]int{"assignee-1": 1}))

			failed, err := backend.GetJob(ctx, "job-1")
			Expect(err).NotTo(HaveOccurred())
			Expect(failed.Status).To(Equal(jobpool.JobStatusFailedRetry))
			Expect(failed.ErrorMessage).To(Equal("error message"))
			Expect(failed.RetryCount).To(Equal(1))
			// AssigneeID is preserved for historical tracking (not cleared)
			Expect(failed.AssigneeID).To(Equal("assignee-1"))
		})

		It("should fail job from UNKNOWN_RETRY state", func() {
			job := &jobpool.Job{
				ID:            "job-fail-unknown",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())

			err = backend.MarkWorkerUnresponsive(ctx, "assignee-1")
			Expect(err).NotTo(HaveOccurred())

			freedAssigneeIDs, err := backend.FailJob(ctx, "job-fail-unknown", "error")
			Expect(err).NotTo(HaveOccurred())
			Expect(freedAssigneeIDs).To(Equal(map[string]int{}))

			failed, err := backend.GetJob(ctx, "job-fail-unknown")
			Expect(err).NotTo(HaveOccurred())
			Expect(failed.Status).To(Equal(jobpool.JobStatusFailedRetry))
			Expect(failed.RetryCount).To(Equal(1))
		})

		It("should return error for empty error message", func() {
			job := &jobpool.Job{
				ID:            "job-fail-empty-msg",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())

			_, err = backend.FailJob(ctx, "job-fail-empty-msg", "")
			Expect(err).To(HaveOccurred())
		})

		It("should return error for invalid state", func() {
			job := &jobpool.Job{
				ID:            "job-fail-invalid",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			_, err = backend.FailJob(ctx, "job-fail-invalid", "error")
			Expect(err).To(HaveOccurred())
		})

		It("should return error when job not found", func() {
			_, err := backend.FailJob(ctx, "missing-job", "error")
			Expect(err).To(HaveOccurred())
		})

		It("should propagate context cancellation", func() {
			job := &jobpool.Job{
				ID:            "job-fail-cancel",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())

			cancelCtx, cancel := context.WithCancel(ctx)
			cancel()

			_, err = backend.FailJob(cancelCtx, "job-fail-cancel", "error")
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(context.Canceled))
		})
	})

	Describe("StopJob", func() {
		It("should stop a job and return freed assignee ID", func() {
			job := &jobpool.Job{
				ID:            "job-1",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("test data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			jobs, err := backend.DequeueJobs(ctx, "assignee-1", nil, 10)
			Expect(err).NotTo(HaveOccurred())
			Expect(jobs).To(HaveLen(1))

			freedAssigneeIDs, err := backend.StopJob(ctx, "job-1", "stopped")
			Expect(err).NotTo(HaveOccurred())
			Expect(freedAssigneeIDs).To(Equal(map[string]int{"assignee-1": 1}))

			stopped, err := backend.GetJob(ctx, "job-1")
			Expect(err).NotTo(HaveOccurred())
			Expect(stopped.Status).To(Equal(jobpool.JobStatusStopped))
			Expect(stopped.ErrorMessage).To(Equal("stopped"))
			// AssigneeID is preserved for historical tracking (per spec: queue.md line 38)
			Expect(stopped.AssigneeID).To(Equal("assignee-1"))
		})

		It("should stop job from CANCELLING state", func() {
			job := &jobpool.Job{
				ID:            "job-stop-cancelling",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())

			cancelled, _, err := backend.CancelJobs(ctx, nil, []string{"job-stop-cancelling"})
			Expect(err).NotTo(HaveOccurred())
			Expect(cancelled).To(ContainElement("job-stop-cancelling"))

			freedAssigneeIDs, err := backend.StopJob(ctx, "job-stop-cancelling", "canceled")
			Expect(err).NotTo(HaveOccurred())
			Expect(freedAssigneeIDs).To(Equal(map[string]int{"assignee-1": 1}))

			stopped, err := backend.GetJob(ctx, "job-stop-cancelling")
			Expect(err).NotTo(HaveOccurred())
			Expect(stopped.Status).To(Equal(jobpool.JobStatusStopped))
			Expect(stopped.ErrorMessage).To(Equal("canceled"))
		})

		It("should stop job from UNKNOWN_RETRY state", func() {
			job := &jobpool.Job{
				ID:            "job-stop-unknown",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())

			err = backend.MarkWorkerUnresponsive(ctx, "assignee-1")
			Expect(err).NotTo(HaveOccurred())

			freedAssigneeIDs, err := backend.StopJob(ctx, "job-stop-unknown", "stopped")
			Expect(err).NotTo(HaveOccurred())
			Expect(freedAssigneeIDs).To(Equal(map[string]int{}))

			stopped, err := backend.GetJob(ctx, "job-stop-unknown")
			Expect(err).NotTo(HaveOccurred())
			Expect(stopped.Status).To(Equal(jobpool.JobStatusStopped))
		})

		It("should allow stopping with empty message", func() {
			job := &jobpool.Job{
				ID:            "job-stop-empty-msg",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())

			_, err = backend.StopJob(ctx, "job-stop-empty-msg", "")
			Expect(err).NotTo(HaveOccurred())

			stopped, err := backend.GetJob(ctx, "job-stop-empty-msg")
			Expect(err).NotTo(HaveOccurred())
			Expect(stopped.Status).To(Equal(jobpool.JobStatusStopped))
			Expect(stopped.ErrorMessage).To(Equal(""))
		})

		It("should return error for invalid state", func() {
			job := &jobpool.Job{
				ID:            "job-stop-invalid",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			_, err = backend.StopJob(ctx, "job-stop-invalid", "error")
			Expect(err).To(HaveOccurred())
		})

		It("should propagate context cancellation", func() {
			job := &jobpool.Job{
				ID:            "job-stop-cancel",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())

			cancelCtx, cancel := context.WithCancel(ctx)
			cancel()

			_, err = backend.StopJob(cancelCtx, "job-stop-cancel", "error")
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(context.Canceled))
		})
	})

	Describe("StopJobWithRetry", func() {
		It("should stop job from CANCELLING with retry increment", func() {
			job := &jobpool.Job{
				ID:            "job-stop-retry",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())

			cancelled, _, err := backend.CancelJobs(ctx, nil, []string{"job-stop-retry"})
			Expect(err).NotTo(HaveOccurred())
			Expect(cancelled).To(ContainElement("job-stop-retry"))

			freedAssigneeIDs, err := backend.StopJobWithRetry(ctx, "job-stop-retry", "failed during cancel")
			Expect(err).NotTo(HaveOccurred())
			Expect(freedAssigneeIDs).To(Equal(map[string]int{"assignee-1": 1}))

			stopped, err := backend.GetJob(ctx, "job-stop-retry")
			Expect(err).NotTo(HaveOccurred())
			Expect(stopped.Status).To(Equal(jobpool.JobStatusStopped))
			Expect(stopped.RetryCount).To(Equal(1))
			Expect(stopped.LastRetryAt).NotTo(BeNil())
		})

		It("should return error when job not in CANCELLING state", func() {
			job := &jobpool.Job{
				ID:            "job-stop-retry-invalid",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			_, err = backend.StopJobWithRetry(ctx, "job-stop-retry-invalid", "error")
			Expect(err).To(HaveOccurred())
		})

		It("should return error when job not found", func() {
			_, err := backend.StopJobWithRetry(ctx, "missing-job", "error")
			Expect(err).To(HaveOccurred())
		})

		It("should propagate context cancellation", func() {
			job := &jobpool.Job{
				ID:            "job-stop-retry-cancel",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())
			_, _, err = backend.CancelJobs(ctx, nil, []string{"job-stop-retry-cancel"})
			Expect(err).NotTo(HaveOccurred())

			cancelCtx, cancel := context.WithCancel(ctx)
			cancel()

			_, err = backend.StopJobWithRetry(cancelCtx, "job-stop-retry-cancel", "error")
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(context.Canceled))
		})
	})

	Describe("MarkJobUnknownStopped", func() {
		It("should mark job from CANCELLING as UNKNOWN_STOPPED", func() {
			job := &jobpool.Job{
				ID:            "job-unknown-stop",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())
			_, _, err = backend.CancelJobs(ctx, nil, []string{"job-unknown-stop"})
			Expect(err).NotTo(HaveOccurred())

			freedAssigneeIDs, err := backend.MarkJobUnknownStopped(ctx, "job-unknown-stop", "timeout")
			Expect(err).NotTo(HaveOccurred())
			Expect(freedAssigneeIDs).To(Equal(map[string]int{"assignee-1": 1}))

			jobState, err := backend.GetJob(ctx, "job-unknown-stop")
			Expect(err).NotTo(HaveOccurred())
			Expect(jobState.Status).To(Equal(jobpool.JobStatusUnknownStopped))
			Expect(jobState.ErrorMessage).To(Equal("timeout"))
		})

		It("should handle jobs from UNKNOWN_RETRY state", func() {
			job := &jobpool.Job{
				ID:            "job-unknown-stop-retry",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())
			err = backend.MarkWorkerUnresponsive(ctx, "assignee-1")
			Expect(err).NotTo(HaveOccurred())

			freedAssigneeIDs, err := backend.MarkJobUnknownStopped(ctx, "job-unknown-stop-retry", "worker missing")
			Expect(err).NotTo(HaveOccurred())
			Expect(freedAssigneeIDs).To(Equal(map[string]int{}))

			jobState, err := backend.GetJob(ctx, "job-unknown-stop-retry")
			Expect(err).NotTo(HaveOccurred())
			Expect(jobState.Status).To(Equal(jobpool.JobStatusUnknownStopped))
		})

		It("should handle jobs from RUNNING state (worker not connected)", func() {
			job := &jobpool.Job{
				ID:            "job-unknown-stop-running",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())

			freedAssigneeIDs, err := backend.MarkJobUnknownStopped(ctx, "job-unknown-stop-running", "worker gone")
			Expect(err).NotTo(HaveOccurred())
			Expect(freedAssigneeIDs).To(Equal(map[string]int{"assignee-1": 1}))

			jobState, err := backend.GetJob(ctx, "job-unknown-stop-running")
			Expect(err).NotTo(HaveOccurred())
			Expect(jobState.Status).To(Equal(jobpool.JobStatusUnknownStopped))
		})

		It("should return error for missing job", func() {
			_, err := backend.MarkJobUnknownStopped(ctx, "missing-job", "error")
			Expect(err).To(HaveOccurred())
		})

		It("should propagate context cancellation", func() {
			job := &jobpool.Job{
				ID:            "job-unknown-stop-cancel",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())

			cancelCtx, cancel := context.WithCancel(ctx)
			cancel()

			_, err = backend.MarkJobUnknownStopped(cancelCtx, "job-unknown-stop-cancel", "error")
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(context.Canceled))
		})
	})

	Describe("MarkWorkerUnresponsive", func() {
		It("should transition RUNNING to UNKNOWN_RETRY and CANCELLING to UNKNOWN_STOPPED", func() {
			jobRunning := &jobpool.Job{
				ID:            "job-worker-running",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}
			jobCancelling := &jobpool.Job{
				ID:            "job-worker-cancelling",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, jobRunning)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.EnqueueJob(ctx, jobCancelling)
			Expect(err).NotTo(HaveOccurred())

			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 2)
			Expect(err).NotTo(HaveOccurred())

			_, _, err = backend.CancelJobs(ctx, nil, []string{"job-worker-cancelling"})
			Expect(err).NotTo(HaveOccurred())

			err = backend.MarkWorkerUnresponsive(ctx, "assignee-1")
			Expect(err).NotTo(HaveOccurred())

			stateRunning, err := backend.GetJob(ctx, "job-worker-running")
			Expect(err).NotTo(HaveOccurred())
			Expect(stateRunning.Status).To(Equal(jobpool.JobStatusUnknownRetry))

			stateCancelling, err := backend.GetJob(ctx, "job-worker-cancelling")
			Expect(err).NotTo(HaveOccurred())
			Expect(stateCancelling.Status).To(Equal(jobpool.JobStatusUnknownStopped))
		})

		It("should be no-op when worker has no jobs", func() {
			err := backend.MarkWorkerUnresponsive(ctx, "unused-worker")
			Expect(err).NotTo(HaveOccurred())
		})

		It("should make freed jobs available for new workers", func() {
			for i := 0; i < 2; i++ {
				job := &jobpool.Job{
					ID:            fmt.Sprintf("job-worker-free-%d", i),
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("data"),
					CreatedAt:     time.Now(),
				}
				_, err := backend.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
			}

			_, err := backend.DequeueJobs(ctx, "worker-old", nil, 2)
			Expect(err).NotTo(HaveOccurred())

			err = backend.MarkWorkerUnresponsive(ctx, "worker-old")
			Expect(err).NotTo(HaveOccurred())

			requeued, err := backend.DequeueJobs(ctx, "worker-new", nil, 2)
			Expect(err).NotTo(HaveOccurred())
			Expect(requeued).To(HaveLen(2))
		})

		It("should propagate context cancellation", func() {
			cancelCtx, cancel := context.WithCancel(ctx)
			cancel()

			err := backend.MarkWorkerUnresponsive(cancelCtx, "worker")
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(context.Canceled))
		})
	})

	Describe("ResetRunningJobs", func() {
		It("should reset all running jobs to UNKNOWN_RETRY", func() {
			for i := 0; i < 3; i++ {
				job := &jobpool.Job{
					ID:            fmt.Sprintf("job-reset-%d", i),
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("data"),
					CreatedAt:     time.Now(),
				}
				_, err := backend.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
			}

			_, err := backend.DequeueJobs(ctx, "reset-worker", nil, 3)
			Expect(err).NotTo(HaveOccurred())

			err = backend.ResetRunningJobs(ctx)
			Expect(err).NotTo(HaveOccurred())

			for i := 0; i < 3; i++ {
				job, err := backend.GetJob(ctx, fmt.Sprintf("job-reset-%d", i))
				Expect(err).NotTo(HaveOccurred())
				Expect(job.Status).To(Equal(jobpool.JobStatusUnknownRetry))
				Expect(job.AssigneeID).To(Equal("reset-worker"))
			}
		})

		It("should be no-op when no running jobs", func() {
			err := backend.ResetRunningJobs(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should allow other workers to claim reset jobs", func() {
			for i := 0; i < 2; i++ {
				job := &jobpool.Job{
					ID:            fmt.Sprintf("job-reset-claim-%d", i),
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("data"),
					CreatedAt:     time.Now(),
				}
				_, err := backend.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
			}

			_, err := backend.DequeueJobs(ctx, "worker-reset-old", nil, 2)
			Expect(err).NotTo(HaveOccurred())

			err = backend.ResetRunningJobs(ctx)
			Expect(err).NotTo(HaveOccurred())

			requeued, err := backend.DequeueJobs(ctx, "worker-reset-new", nil, 2)
			Expect(err).NotTo(HaveOccurred())
			Expect(requeued).To(HaveLen(2))
		})

		It("should honor context cancellation", func() {
			cancelCtx, cancel := context.WithCancel(ctx)
			cancel()

			err := backend.ResetRunningJobs(cancelCtx)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(context.Canceled))
		})

		It("should transition CANCELLING jobs to UNKNOWN_STOPPED", func() {
			// Enqueue and assign job
			job := &jobpool.Job{
				ID:            "job-cancelling-reset",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}
			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			// Assign and cancel job
			_, err = backend.DequeueJobs(ctx, "reset-worker", nil, 1)
			Expect(err).NotTo(HaveOccurred())
			_, _, err = backend.CancelJobs(ctx, nil, []string{"job-cancelling-reset"})
			Expect(err).NotTo(HaveOccurred())

			// Verify job is in CANCELLING state
			retrieved, err := backend.GetJob(ctx, "job-cancelling-reset")
			Expect(err).NotTo(HaveOccurred())
			Expect(retrieved.Status).To(Equal(jobpool.JobStatusCancelling))

			// Reset running jobs (should handle CANCELLING jobs)
			err = backend.ResetRunningJobs(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Verify job is UNKNOWN_STOPPED (CANCELLING → UNKNOWN_STOPPED)
			retrieved, err = backend.GetJob(ctx, "job-cancelling-reset")
			Expect(err).NotTo(HaveOccurred())
			Expect(retrieved.Status).To(Equal(jobpool.JobStatusUnknownStopped))
			Expect(retrieved.AssigneeID).To(Equal("reset-worker"))
		})
	})

	Describe("CleanupExpiredJobs", func() {
		It("should remove completed jobs older than TTL", func() {
			jobOld := &jobpool.Job{
				ID:            "job-cleanup-old",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now().Add(-2 * time.Hour),
			}
			jobNew := &jobpool.Job{
				ID:            "job-cleanup-new",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, jobOld)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.EnqueueJob(ctx, jobNew)
			Expect(err).NotTo(HaveOccurred())

			_, err = backend.DequeueJobs(ctx, "cleanup-worker", nil, 2)
			Expect(err).NotTo(HaveOccurred())
			// Complete old job first
			_, err = backend.CompleteJob(ctx, "job-cleanup-old", []byte("done"))
			Expect(err).NotTo(HaveOccurred())

			// Wait to ensure timestamps differ (now using nanosecond precision)
			// Use 100ms to ensure clear separation
			time.Sleep(100 * time.Millisecond)

			// Complete new job (its finalized_at will be ~100ms after old job's)
			_, err = backend.CompleteJob(ctx, "job-cleanup-new", []byte("done"))
			Expect(err).NotTo(HaveOccurred())

			// Wait a bit to ensure finalized_at is set for new job
			time.Sleep(50 * time.Millisecond)

			// TTL should be between old job's age (100ms+) and new job's age (50ms+)
			// Use 75ms TTL: old job (100ms+) will be deleted, new job (50ms+) will be kept
			err = backend.CleanupExpiredJobs(ctx, 75*time.Millisecond)
			Expect(err).NotTo(HaveOccurred())

			_, err = backend.GetJob(ctx, "job-cleanup-old")
			Expect(err).To(HaveOccurred())

			job, err := backend.GetJob(ctx, "job-cleanup-new")
			Expect(err).NotTo(HaveOccurred())
			Expect(job.ID).To(Equal("job-cleanup-new"))
		})

		It("should error when ttl <= 0", func() {
			err := backend.CleanupExpiredJobs(ctx, 0)
			Expect(err).To(HaveOccurred())
		})

		It("should only delete jobs with finalized_at set", func() {
			// Create a job that never gets finalized (stays in RUNNING)
			jobRunning := &jobpool.Job{
				ID:            "job-running-no-finalize",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now().Add(-2 * time.Hour), // Old job
			}
			_, err := backend.EnqueueJob(ctx, jobRunning)
			Expect(err).NotTo(HaveOccurred())

			// Dequeue but don't complete (no finalized_at)
			_, err = backend.DequeueJobs(ctx, "cleanup-worker", nil, 1)
			Expect(err).NotTo(HaveOccurred())

			// Create a completed job (has finalized_at)
			jobCompleted := &jobpool.Job{
				ID:            "job-completed-finalize",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now().Add(-2 * time.Hour), // Old job
			}
			_, err = backend.EnqueueJob(ctx, jobCompleted)
			Expect(err).NotTo(HaveOccurred())

			_, err = backend.DequeueJobs(ctx, "cleanup-worker", nil, 1)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.CompleteJob(ctx, "job-completed-finalize", []byte("done"))
			Expect(err).NotTo(HaveOccurred())

			// Wait to ensure finalized_at is set
			time.Sleep(10 * time.Millisecond)

			// Cleanup with very short TTL (should delete completed job)
			err = backend.CleanupExpiredJobs(ctx, 1*time.Millisecond)
			Expect(err).NotTo(HaveOccurred())

			// Running job should still exist (no finalized_at)
			_, err = backend.GetJob(ctx, "job-running-no-finalize")
			Expect(err).NotTo(HaveOccurred())

			// Completed job should be deleted (had finalized_at)
			_, err = backend.GetJob(ctx, "job-completed-finalize")
			Expect(err).To(HaveOccurred())
		})

		It("should propagate context cancellation", func() {
			cancelCtx, cancel := context.WithCancel(ctx)
			cancel()

			err := backend.CleanupExpiredJobs(cancelCtx, time.Second)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(context.Canceled))
		})
	})

	Describe("DeleteJobs", func() {
		It("should delete jobs by tags when all are in final state", func() {
			job := &jobpool.Job{
				ID:            "job-delete-tags",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				Tags:          []string{"delete", "final"},
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			_, err = backend.DequeueJobs(ctx, "delete-worker", nil, 1)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.CompleteJob(ctx, "job-delete-tags", []byte("done"))
			Expect(err).NotTo(HaveOccurred())

			err = backend.DeleteJobs(ctx, []string{"delete", "final"}, nil)
			Expect(err).NotTo(HaveOccurred())

			_, err = backend.GetJob(ctx, "job-delete-tags")
			Expect(err).To(HaveOccurred())
		})

		It("should delete jobs by IDs", func() {
			job := &jobpool.Job{
				ID:            "job-delete-id",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}
			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "delete-worker", nil, 1)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.StopJob(ctx, "job-delete-id", "stopped")
			Expect(err).NotTo(HaveOccurred())

			err = backend.DeleteJobs(ctx, nil, []string{"job-delete-id"})
			Expect(err).NotTo(HaveOccurred())

			_, err = backend.GetJob(ctx, "job-delete-id")
			Expect(err).To(HaveOccurred())
		})

		It("should delete jobs using both tags and IDs", func() {
			jobByTag := &jobpool.Job{
				ID:            "job-delete-union-tag",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				Tags:          []string{"delete-union"},
				CreatedAt:     time.Now(),
			}
			jobByID := &jobpool.Job{
				ID:            "job-delete-union-id",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, jobByTag)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.EnqueueJob(ctx, jobByID)
			Expect(err).NotTo(HaveOccurred())

			_, err = backend.DequeueJobs(ctx, "delete-union-worker", nil, 2)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.CompleteJob(ctx, "job-delete-union-tag", []byte("done"))
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.CompleteJob(ctx, "job-delete-union-id", []byte("done"))
			Expect(err).NotTo(HaveOccurred())

			err = backend.DeleteJobs(ctx, []string{"delete-union"}, []string{"job-delete-union-id"})
			Expect(err).NotTo(HaveOccurred())

			_, err = backend.GetJob(ctx, "job-delete-union-tag")
			Expect(err).To(HaveOccurred())
			_, err = backend.GetJob(ctx, "job-delete-union-id")
			Expect(err).To(HaveOccurred())
		})

		It("should be atomic when any referenced job is not in a final state", func() {
			jobFinal := &jobpool.Job{
				ID:            "job-delete-final",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				Tags:          []string{"delete-mixed"},
				CreatedAt:     time.Now(),
			}
			jobPending := &jobpool.Job{
				ID:            "job-delete-pending",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, jobFinal)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.EnqueueJob(ctx, jobPending)
			Expect(err).NotTo(HaveOccurred())

			_, err = backend.DequeueJobs(ctx, "delete-mixed-worker", nil, 2)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.CompleteJob(ctx, "job-delete-final", []byte("done"))
			Expect(err).NotTo(HaveOccurred())

			err = backend.DeleteJobs(ctx, []string{"delete-mixed"}, []string{"job-delete-pending"})
			Expect(err).To(HaveOccurred())

			_, err = backend.GetJob(ctx, "job-delete-final")
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.GetJob(ctx, "job-delete-pending")
			Expect(err).NotTo(HaveOccurred())
		})

		It("should fail if any job is not in final state", func() {
			job := &jobpool.Job{
				ID:            "job-delete-invalid",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			err = backend.DeleteJobs(ctx, nil, []string{"job-delete-invalid"})
			Expect(err).To(HaveOccurred())

			// Ensure job still exists
			_, err = backend.GetJob(ctx, "job-delete-invalid")
			Expect(err).NotTo(HaveOccurred())
		})

		It("should require tags or job IDs", func() {
			err := backend.DeleteJobs(ctx, nil, nil)
			Expect(err).To(HaveOccurred())
		})

		It("should propagate context cancellation", func() {
			cancelCtx, cancel := context.WithCancel(ctx)
			cancel()

			err := backend.DeleteJobs(cancelCtx, nil, []string{"job"})
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(context.Canceled))
		})
	})

	Describe("Close", func() {
		It("should close backend without error", func() {
			newBackend, newCleanup := backendFactory()
			defer newCleanup()

			Expect(newBackend.Close()).To(Succeed())
			// Closing twice should also be safe
			Expect(newBackend.Close()).To(Succeed())

			_, err := newBackend.EnqueueJob(ctx, &jobpool.Job{
				ID:            "job-after-close",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			})
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Concurrency safeguards", func() {
		It("should assign each job only once across concurrent workers", func() {
			const jobCount = 20
			for i := 0; i < jobCount; i++ {
				job := &jobpool.Job{
					ID:            fmt.Sprintf("job-concurrency-%d", i),
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("data"),
					CreatedAt:     time.Now(),
				}
				_, err := backend.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
			}

			var processed int32
			var wg sync.WaitGroup
			workerCount := 5
			wg.Add(workerCount)

			for w := 0; w < workerCount; w++ {
				workerID := fmt.Sprintf("concurrent-worker-%d", w)
				go func(id string) {
					defer GinkgoRecover()
					defer wg.Done()
					for {
						if atomic.LoadInt32(&processed) >= jobCount {
							return
						}

						jobs, err := backend.DequeueJobs(ctx, id, nil, 1)
						Expect(err).NotTo(HaveOccurred())

						if len(jobs) == 0 {
							time.Sleep(2 * time.Millisecond)
							continue
						}

						_, err = backend.CompleteJob(ctx, jobs[0].ID, []byte("done"))
						Expect(err).NotTo(HaveOccurred())
						atomic.AddInt32(&processed, 1)
					}
				}(workerID)
			}

			wg.Wait()
			Expect(processed).To(Equal(int32(jobCount)))
		})
	})

	Describe("UpdateJobStatus", func() {
		It("should update job status with valid transition", func() {
			job := &jobpool.Job{
				ID:            "job-update",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())

			freedAssigneeIDs, err := backend.UpdateJobStatus(ctx, "job-update", jobpool.JobStatusCompleted, []byte("result"), "")
			Expect(err).NotTo(HaveOccurred())
			Expect(freedAssigneeIDs).To(Equal(map[string]int{"assignee-1": 1}))

			updated, err := backend.GetJob(ctx, "job-update")
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Status).To(Equal(jobpool.JobStatusCompleted))
			Expect(updated.Result).To(Equal([]byte("result")))
			Expect(updated.FinalizedAt).NotTo(BeNil())
		})

		It("should update error message and preserve timestamps", func() {
			job := &jobpool.Job{
				ID:            "job-update-error",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())

			_, err = backend.UpdateJobStatus(ctx, "job-update-error", jobpool.JobStatusFailedRetry, nil, "error")
			Expect(err).NotTo(HaveOccurred())

			updated, err := backend.GetJob(ctx, "job-update-error")
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Status).To(Equal(jobpool.JobStatusFailedRetry))
			Expect(updated.ErrorMessage).To(Equal("error"))
		})

		It("should reject invalid transitions", func() {
			job := &jobpool.Job{
				ID:            "job-update-invalid",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			_, err = backend.UpdateJobStatus(ctx, "job-update-invalid", jobpool.JobStatusCompleted, nil, "")
			Expect(err).To(HaveOccurred())
		})

		It("should return error when job not found", func() {
			_, err := backend.UpdateJobStatus(ctx, "missing-job", jobpool.JobStatusCompleted, nil, "")
			Expect(err).To(HaveOccurred())
		})

		It("should honor context cancellation", func() {
			job := &jobpool.Job{
				ID:            "job-update-cancel",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			cancelCtx, cancel := context.WithCancel(ctx)
			cancel()

			_, err = backend.UpdateJobStatus(cancelCtx, "job-update-cancel", jobpool.JobStatusCompleted, nil, "")
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(context.Canceled))
		})
	})

	Describe("CancelJobs", func() {
		It("should cancel jobs by tags with AND logic", func() {
			job := &jobpool.Job{
				ID:            "job-cancel-tags",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				Tags:          []string{"tag1", "tag2"},
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			cancelled, unknown, err := backend.CancelJobs(ctx, []string{"tag1", "tag2"}, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(cancelled).To(ContainElement("job-cancel-tags"))
			Expect(unknown).To(BeEmpty())

			state, err := backend.GetJob(ctx, "job-cancel-tags")
			Expect(err).NotTo(HaveOccurred())
			Expect(state.Status).To(Equal(jobpool.JobStatusUnscheduled))
		})

		It("should cancel running job by ID", func() {
			job := &jobpool.Job{
				ID:            "job-cancel-running",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}
			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())

			cancelled, unknown, err := backend.CancelJobs(ctx, nil, []string{"job-cancel-running"})
			Expect(err).NotTo(HaveOccurred())
			Expect(cancelled).To(ContainElement("job-cancel-running"))
			Expect(unknown).To(BeEmpty())

			state, err := backend.GetJob(ctx, "job-cancel-running")
			Expect(err).NotTo(HaveOccurred())
			Expect(state.Status).To(Equal(jobpool.JobStatusCancelling))
		})

		It("should stop FAILED_RETRY and UNKNOWN_RETRY jobs", func() {
			jobFailed := &jobpool.Job{
				ID:            "job-cancel-failed",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}
			jobUnknown := &jobpool.Job{
				ID:            "job-cancel-unknown",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, jobFailed)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.EnqueueJob(ctx, jobUnknown)
			Expect(err).NotTo(HaveOccurred())

			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 2)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.FailJob(ctx, "job-cancel-failed", "error")
			Expect(err).NotTo(HaveOccurred())
			err = backend.MarkWorkerUnresponsive(ctx, "assignee-1")
			Expect(err).NotTo(HaveOccurred())

			cancelled, unknown, err := backend.CancelJobs(ctx, nil, []string{"job-cancel-failed", "job-cancel-unknown"})
			Expect(err).NotTo(HaveOccurred())
			Expect(cancelled).To(ContainElements("job-cancel-failed", "job-cancel-unknown"))
			Expect(unknown).To(BeEmpty())

			stateFailed, err := backend.GetJob(ctx, "job-cancel-failed")
			Expect(err).NotTo(HaveOccurred())
			Expect(stateFailed.Status).To(Equal(jobpool.JobStatusStopped))

			stateUnknown, err := backend.GetJob(ctx, "job-cancel-unknown")
			Expect(err).NotTo(HaveOccurred())
			Expect(stateUnknown.Status).To(Equal(jobpool.JobStatusStopped))
		})

		It("should return unknown IDs for missing jobs", func() {
			cancelled, unknown, err := backend.CancelJobs(ctx, nil, []string{"missing"})
			Expect(err).NotTo(HaveOccurred())
			Expect(cancelled).To(BeEmpty())
			Expect(unknown).To(ContainElement("missing"))
		})

		It("should cancel jobs using union of tags and job IDs", func() {
			jobByTag := &jobpool.Job{
				ID:            "job-cancel-union-tag",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				Tags:          []string{"union"},
				CreatedAt:     time.Now(),
			}
			jobByID := &jobpool.Job{
				ID:            "job-cancel-union-id",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, jobByTag)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.EnqueueJob(ctx, jobByID)
			Expect(err).NotTo(HaveOccurred())

			cancelled, unknown, err := backend.CancelJobs(ctx, []string{"union"}, []string{"job-cancel-union-id"})
			Expect(err).NotTo(HaveOccurred())
			Expect(cancelled).To(ContainElements("job-cancel-union-tag", "job-cancel-union-id"))
			Expect(unknown).To(BeEmpty())

			stateTag, err := backend.GetJob(ctx, "job-cancel-union-tag")
			Expect(err).NotTo(HaveOccurred())
			Expect(stateTag.Status).To(Equal(jobpool.JobStatusUnscheduled))

			stateID, err := backend.GetJob(ctx, "job-cancel-union-id")
			Expect(err).NotTo(HaveOccurred())
			Expect(stateID.Status).To(Equal(jobpool.JobStatusUnscheduled))
		})

		It("should return error if no tags or job IDs provided", func() {
			_, _, err := backend.CancelJobs(ctx, nil, nil)
			Expect(err).To(HaveOccurred())
		})

		It("should honor context cancellation", func() {
			job := &jobpool.Job{
				ID:            "job-cancel-context",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			cancelCtx, cancel := context.WithCancel(ctx)
			cancel()

			_, _, err = backend.CancelJobs(cancelCtx, nil, []string{"job-cancel-context"})
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(context.Canceled))
		})
	})

	Describe("AcknowledgeCancellation", func() {
		It("should transition CANCELLING job to STOPPED when executing", func() {
			job := &jobpool.Job{
				ID:            "job-ack-stop",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())
			_, _, err = backend.CancelJobs(ctx, nil, []string{"job-ack-stop"})
			Expect(err).NotTo(HaveOccurred())

			err = backend.AcknowledgeCancellation(ctx, "job-ack-stop", true)
			Expect(err).NotTo(HaveOccurred())

			state, err := backend.GetJob(ctx, "job-ack-stop")
			Expect(err).NotTo(HaveOccurred())
			Expect(state.Status).To(Equal(jobpool.JobStatusStopped))
		})

		It("should transition CANCELLING job to UNKNOWN_STOPPED when not executing", func() {
			job := &jobpool.Job{
				ID:            "job-ack-unknown",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())
			_, _, err = backend.CancelJobs(ctx, nil, []string{"job-ack-unknown"})
			Expect(err).NotTo(HaveOccurred())

			err = backend.AcknowledgeCancellation(ctx, "job-ack-unknown", false)
			Expect(err).NotTo(HaveOccurred())

			state, err := backend.GetJob(ctx, "job-ack-unknown")
			Expect(err).NotTo(HaveOccurred())
			Expect(state.Status).To(Equal(jobpool.JobStatusUnknownStopped))
		})

		It("should return error when job is not in CANCELLING state", func() {
			job := &jobpool.Job{
				ID:            "job-ack-invalid",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			err = backend.AcknowledgeCancellation(ctx, "job-ack-invalid", true)
			Expect(err).To(HaveOccurred())
		})

		It("should return error when job not found", func() {
			err := backend.AcknowledgeCancellation(ctx, "missing-job", true)
			Expect(err).To(HaveOccurred())
		})

		It("should propagate context cancellation", func() {
			job := &jobpool.Job{
				ID:            "job-ack-cancel",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())
			_, _, err = backend.CancelJobs(ctx, nil, []string{"job-ack-cancel"})
			Expect(err).NotTo(HaveOccurred())

			cancelCtx, cancel := context.WithCancel(ctx)
			cancel()

			err = backend.AcknowledgeCancellation(cancelCtx, "job-ack-cancel", true)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(context.Canceled))
		})
	})

	Describe("GetJobStats", func() {
		It("should return stats for jobs with matching tags", func() {
			job1 := &jobpool.Job{
				ID:            "job-1",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data1"),
				Tags:          []string{"tag1"},
				CreatedAt:     time.Now(),
			}
			job2 := &jobpool.Job{
				ID:            "job-2",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data2"),
				Tags:          []string{"tag1", "tag2"},
				CreatedAt:     time.Now(),
			}
			job3 := &jobpool.Job{
				ID:            "job-3",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data3"),
				Tags:          []string{"tag2"},
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job1)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.EnqueueJob(ctx, job2)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.EnqueueJob(ctx, job3)
			Expect(err).NotTo(HaveOccurred())

			stats, err := backend.GetJobStats(ctx, []string{"tag1", "tag2"})
			Expect(err).NotTo(HaveOccurred())
			Expect(stats.TotalJobs).To(Equal(int32(1))) // Only job-2 has both tags
			Expect(stats.PendingJobs).To(Equal(int32(1)))
		})

		It("should return stats for all jobs when tags empty", func() {
			pending := &jobpool.Job{
				ID:            "job-stats-pending",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("pending"),
				CreatedAt:     time.Now(),
			}
			running := &jobpool.Job{
				ID:            "job-stats-running",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("running"),
				CreatedAt:     time.Now(),
			}
			completed := &jobpool.Job{
				ID:            "job-stats-completed",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("completed"),
				CreatedAt:     time.Now(),
			}
			stopped := &jobpool.Job{
				ID:            "job-stats-stopped",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("stopped"),
				CreatedAt:     time.Now(),
			}
			failed := &jobpool.Job{
				ID:            "job-stats-failed",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("failed"),
				CreatedAt:     time.Now(),
			}

			for _, job := range []*jobpool.Job{pending, running, completed, stopped, failed} {
				_, err := backend.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
			}

			// Dequeue 4 jobs, leaving 1 pending
			dequeuedJobs, err := backend.DequeueJobs(ctx, "stats-worker", nil, 4)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(dequeuedJobs)).To(Equal(4)) // Exactly 4 jobs should be dequeued

			// Verify we have exactly 1 pending job remaining
			allJobs := []string{"job-stats-pending", "job-stats-running", "job-stats-completed", "job-stats-stopped", "job-stats-failed"}
			dequeuedIDs := make(map[string]bool)
			for _, job := range dequeuedJobs {
				dequeuedIDs[job.ID] = true
			}
			var pendingJobID string
			for _, jobID := range allJobs {
				if !dequeuedIDs[jobID] {
					pendingJobID = jobID
					break
				}
			}
			Expect(pendingJobID).NotTo(BeEmpty(), "One job should remain pending")

			// Complete/stop/fail 3 of the dequeued jobs, leaving 1 running
			// Use the actual dequeued jobs rather than assuming specific IDs
			if len(dequeuedJobs) >= 3 {
				_, err = backend.CompleteJob(ctx, dequeuedJobs[0].ID, []byte("done"))
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.StopJob(ctx, dequeuedJobs[1].ID, "stopped")
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.FailJob(ctx, dequeuedJobs[2].ID, "error")
				Expect(err).NotTo(HaveOccurred())
			}

			stats, err := backend.GetJobStats(ctx, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(stats.TotalJobs).To(Equal(int32(5)))
			// Verify the pending job still exists and is in INITIAL_PENDING state
			pendingJob, err := backend.GetJob(ctx, pendingJobID)
			Expect(err).NotTo(HaveOccurred(), "Pending job %s should still exist", pendingJobID)
			Expect(pendingJob.Status).To(Equal(jobpool.JobStatusInitialPending), "Pending job %s should be in INITIAL_PENDING state, got %s", pendingJobID, pendingJob.Status)
			Expect(stats.PendingJobs).To(Equal(int32(1)), "One job should remain pending: %s (actual pending: %d, running: %d, completed: %d, stopped: %d, failed: %d)", pendingJobID, stats.PendingJobs, stats.RunningJobs, stats.CompletedJobs, stats.StoppedJobs, stats.FailedJobs)
			Expect(stats.RunningJobs).To(Equal(int32(1)), "One job should remain running")
			Expect(stats.CompletedJobs).To(BeNumerically(">=", 1))
			Expect(stats.StoppedJobs).To(BeNumerically(">=", 1))
			Expect(stats.FailedJobs).To(BeNumerically(">=", 1))
			Expect(stats.TotalRetries).To(BeNumerically(">=", 1))
		})

		It("should propagate context cancellation", func() {
			cancelCtx, cancel := context.WithCancel(ctx)
			cancel()

			_, err := backend.GetJobStats(cancelCtx, []string{})
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(context.Canceled))
		})
	})

	Describe("GetJob", func() {
		It("should return complete job details", func() {
			job := &jobpool.Job{
				ID:            "job-get",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "custom",
				JobDefinition: []byte("definition"),
				Tags:          []string{"alpha", "beta"},
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			retrieved, err := backend.GetJob(ctx, "job-get")
			Expect(err).NotTo(HaveOccurred())
			Expect(retrieved.ID).To(Equal("job-get"))
			Expect(retrieved.JobType).To(Equal("custom"))
			Expect(retrieved.JobDefinition).To(Equal([]byte("definition")))
			Expect(retrieved.Tags).To(ContainElements("alpha", "beta"))
		})

		It("should return error when job not found", func() {
			_, err := backend.GetJob(ctx, "missing-job")
			Expect(err).To(HaveOccurred())
		})

		It("should propagate context cancellation", func() {
			cancelCtx, cancel := context.WithCancel(ctx)
			cancel()

			_, err := backend.GetJob(cancelCtx, "anything")
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(context.Canceled))
		})
	})

	Describe("Backend invariants", func() {
		It("should not allow job to return to INITIAL_PENDING after leaving it", func() {
			job := &jobpool.Job{
				ID:            "job-invariant-status",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "assignee-1", nil, 1)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.FailJob(ctx, "job-invariant-status", "error")
			Expect(err).NotTo(HaveOccurred())

			jobState, err := backend.GetJob(ctx, "job-invariant-status")
			Expect(err).NotTo(HaveOccurred())
			Expect(jobState.Status).To(Equal(jobpool.JobStatusFailedRetry))
		})

		It("should preserve AssigneeID for historical tracking", func() {
			job := &jobpool.Job{
				ID:            "job-invariant-assignee",
				Status:        jobpool.JobStatusInitialPending,
				JobType:       "test",
				JobDefinition: []byte("data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.DequeueJobs(ctx, "assignee-keep", nil, 1)
			Expect(err).NotTo(HaveOccurred())
			_, err = backend.CompleteJob(ctx, "job-invariant-assignee", []byte("done"))
			Expect(err).NotTo(HaveOccurred())

			state, err := backend.GetJob(ctx, "job-invariant-assignee")
			Expect(err).NotTo(HaveOccurred())
			Expect(state.AssigneeID).To(Equal("assignee-keep"))
		})
	})

	Describe("DequeueJobs ordering", func() {
		Context("when dequeuing jobs with different submission times", func() {
			It("should return jobs in ascending order by max(created_at, last_retry_at) (oldest first)", func() {
				// Create jobs with different creation times (oldest to newest)
				// Per spec (queue.md line 200): Jobs are selected in ascending order (oldest first)
				baseTime := time.Now()
				job1 := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test1"),
					CreatedAt:     baseTime.Add(-5 * time.Minute), // Oldest
				}
				job2 := &jobpool.Job{
					ID:            "job-2",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test2"),
					CreatedAt:     baseTime.Add(-3 * time.Minute), // Middle
				}
				job3 := &jobpool.Job{
					ID:            "job-3",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test3"),
					CreatedAt:     baseTime.Add(-1 * time.Minute), // Newest
				}

				_, err := backend.EnqueueJob(ctx, job1)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.EnqueueJob(ctx, job2)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.EnqueueJob(ctx, job3)
				Expect(err).NotTo(HaveOccurred())

				// Dequeue all jobs
				jobs, err := backend.DequeueJobs(ctx, "worker-1", nil, 10)
				Expect(err).NotTo(HaveOccurred())
				Expect(jobs).To(HaveLen(3))

				// Should be in ascending order: oldest first (job-1, job-2, job-3)
				// Per spec: "Jobs are selected in ascending order by COALESCE(LastRetryAt, CreatedAt) (oldest first)"
				Expect(jobs[0].ID).To(Equal("job-1"))
				Expect(jobs[1].ID).To(Equal("job-2"))
				Expect(jobs[2].ID).To(Equal("job-3"))
			})
		})

		Context("when dequeuing jobs with retry times", func() {
			It("should order by max(created_at, last_retry_at) in ascending order (oldest first)", func() {
				// Per spec (queue.md line 200): Jobs are selected in ascending order (oldest first)
				baseTime := time.Now()
				// Job 1: created early, failed recently (last_retry_at > created_at)
				job1 := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test1"),
					CreatedAt:     baseTime.Add(-10 * time.Minute),
				}
				// Job 2: created recently, never failed (created_at only)
				job2 := &jobpool.Job{
					ID:            "job-2",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test2"),
					CreatedAt:     baseTime.Add(-2 * time.Minute),
				}
				// Job 3: created early, failed very recently (last_retry_at > created_at, and > job2.created_at)
				job3 := &jobpool.Job{
					ID:            "job-3",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test3"),
					CreatedAt:     baseTime.Add(-8 * time.Minute),
				}

				_, err := backend.EnqueueJob(ctx, job1)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.EnqueueJob(ctx, job2)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.EnqueueJob(ctx, job3)
				Expect(err).NotTo(HaveOccurred())

				// Make job1 and job3 fail to set last_retry_at
				// First assign them
				_, err = backend.DequeueJobs(ctx, "worker-temp", nil, 10)
				Expect(err).NotTo(HaveOccurred())

				// Fail job1 with recent retry time
				_, err = backend.FailJob(ctx, "job-1", "error1")
				Expect(err).NotTo(HaveOccurred())
				// Fail job3 with very recent retry time (newer than job1's retry)
				_, err = backend.FailJob(ctx, "job-3", "error3")
				Expect(err).NotTo(HaveOccurred())

				// Reset job2 by stopping + deleting before re-enqueueing
				_, err = backend.StopJob(ctx, "job-2", "reset for ordering test")
				Expect(err).NotTo(HaveOccurred())
				err = backend.DeleteJobs(ctx, nil, []string{"job-2"})
				Expect(err).NotTo(HaveOccurred())

				// Re-enqueue job2 (it was dequeued but we need it back)
				job2Again := &jobpool.Job{
					ID:            "job-2",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test2"),
					CreatedAt:     baseTime.Add(-2 * time.Minute),
				}
				_, err = backend.EnqueueJob(ctx, job2Again)
				Expect(err).NotTo(HaveOccurred())

				// Dequeue all jobs
				jobs, err := backend.DequeueJobs(ctx, "worker-1", nil, 10)
				Expect(err).NotTo(HaveOccurred())
				Expect(jobs).To(HaveLen(3))

				// Should be ordered by max(created_at, last_retry_at) ascending (oldest first):
				// job-2 (max = created_at, oldest) < job-1 (max = last_retry_at, recent) < job-3 (max = last_retry_at, very recent)
				Expect(jobs[0].ID).To(Equal("job-2"))
				Expect(jobs[1].ID).To(Equal("job-1"))
				Expect(jobs[2].ID).To(Equal("job-3"))
			})
		})

		Context("when mixing INITIAL_PENDING and FAILED_RETRY jobs", func() {
			It("should order all eligible jobs by max(created_at, last_retry_at) ascending (oldest first)", func() {
				// Per spec (queue.md line 200): Jobs are selected in ascending order (oldest first)
				baseTime := time.Now()
				// INITIAL_PENDING job created recently
				job1 := &jobpool.Job{
					ID:            "job-pending",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test1"),
					CreatedAt:     baseTime.Add(-1 * time.Minute),
				}
				// FAILED_RETRY job with recent retry
				job2 := &jobpool.Job{
					ID:            "job-failed",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test2"),
					CreatedAt:     baseTime.Add(-5 * time.Minute),
				}

				_, err := backend.EnqueueJob(ctx, job1)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.EnqueueJob(ctx, job2)
				Expect(err).NotTo(HaveOccurred())

				// Make job2 fail to set last_retry_at
				_, err = backend.DequeueJobs(ctx, "worker-temp", nil, 10)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.FailJob(ctx, "job-failed", "error")
				Expect(err).NotTo(HaveOccurred())

				// Reset job1 by stopping + deleting before re-enqueueing
				_, err = backend.StopJob(ctx, "job-pending", "reset for ordering test")
				Expect(err).NotTo(HaveOccurred())
				err = backend.DeleteJobs(ctx, nil, []string{"job-pending"})
				Expect(err).NotTo(HaveOccurred())

				// Re-enqueue job1
				job1Again := &jobpool.Job{
					ID:            "job-pending",
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("test1"),
					CreatedAt:     baseTime.Add(-1 * time.Minute),
				}
				_, err = backend.EnqueueJob(ctx, job1Again)
				Expect(err).NotTo(HaveOccurred())

				// Dequeue all jobs
				jobs, err := backend.DequeueJobs(ctx, "worker-1", nil, 10)
				Expect(err).NotTo(HaveOccurred())
				Expect(jobs).To(HaveLen(2))

				// Should be ordered ascending (oldest first):
				// job-pending (created_at = -1min, older) < job-failed (last_retry_at = now, newer)
				Expect(jobs[0].ID).To(Equal("job-pending"))
				Expect(jobs[1].ID).To(Equal("job-failed"))
			})
		})
	})
}

var _ = Describe("InMemoryBackend", func() {
	BackendTestSuite(func() (jobpool.Backend, func()) {
		backend := jobpool.NewInMemoryBackend()
		return backend, func() {
			_ = backend.Close()
		}
	})
})

var _ = Describe("BadgerBackend", func() {
	BackendTestSuite(func() (jobpool.Backend, func()) {
		tmpDir, err := os.MkdirTemp("", "test_jobpool_*")
		Expect(err).NotTo(HaveOccurred())

		backend, err := jobpool.NewBadgerBackend(tmpDir, testLogger())
		Expect(err).NotTo(HaveOccurred())

		return backend, func() {
			_ = backend.Close()
			_ = os.RemoveAll(tmpDir)
		}
	})
})
