package jobpool_test

import (
	"context"
	"time"

	"github.com/VsevolodSauta/jobpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

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
				Status:        jobpool.JobStatusPending,
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
				Status:        jobpool.JobStatusPending,
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
			Expect(retrieved.Status).To(Equal(jobpool.JobStatusPending))
			Expect(retrieved.JobType).To(Equal("test"))
			Expect(retrieved.JobDefinition).To(Equal([]byte("test data")))
			Expect(retrieved.Tags).To(ContainElement("tag1"))
		})
	})

	Describe("EnqueueJobs", func() {
		It("should enqueue multiple jobs", func() {
			jobs := []*jobpool.Job{
				{
					ID:            "job-1",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("data1"),
					CreatedAt:     time.Now(),
				},
				{
					ID:            "job-2",
					Status:        jobpool.JobStatusPending,
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
	})

	Describe("DequeueJobs", func() {
		It("should dequeue pending jobs", func() {
			job := &jobpool.Job{
				ID:            "job-1",
				Status:        jobpool.JobStatusPending,
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
		})

		It("should filter by tags", func() {
			job1 := &jobpool.Job{
				ID:            "job-1",
				Status:        jobpool.JobStatusPending,
				JobType:       "test",
				JobDefinition: []byte("data1"),
				Tags:          []string{"tag1"},
				CreatedAt:     time.Now(),
			}
			job2 := &jobpool.Job{
				ID:            "job-2",
				Status:        jobpool.JobStatusPending,
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
		})

		It("should respect limit", func() {
			for i := 0; i < 5; i++ {
				job := &jobpool.Job{
					ID:            "job-" + string(rune('1'+i)),
					Status:        jobpool.JobStatusPending,
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
	})

	Describe("CompleteJob", func() {
		It("should complete a job and return freed assignee ID", func() {
			job := &jobpool.Job{
				ID:            "job-1",
				Status:        jobpool.JobStatusPending,
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
			Expect(freedAssigneeIDs).To(Equal([]string{"assignee-1"}))

			completed, err := backend.GetJob(ctx, "job-1")
			Expect(err).NotTo(HaveOccurred())
			Expect(completed.Status).To(Equal(jobpool.JobStatusCompleted))
			Expect(completed.Result).To(Equal([]byte("result")))
			Expect(completed.AssigneeID).To(BeEmpty())
		})

		It("should return error if job was not assigned", func() {
			job := &jobpool.Job{
				ID:            "job-1",
				Status:        jobpool.JobStatusPending,
				JobType:       "test",
				JobDefinition: []byte("test data"),
				CreatedAt:     time.Now(),
			}

			_, err := backend.EnqueueJob(ctx, job)
			Expect(err).NotTo(HaveOccurred())

			_, err = backend.CompleteJob(ctx, "job-1", []byte("result"))
			Expect(err).To(HaveOccurred()) // Job must be in RUNNING state to complete
		})
	})

	Describe("FailJob", func() {
		It("should fail a job and return freed assignee ID", func() {
			job := &jobpool.Job{
				ID:            "job-1",
				Status:        jobpool.JobStatusPending,
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
			Expect(freedAssigneeIDs).To(Equal([]string{"assignee-1"}))

			failed, err := backend.GetJob(ctx, "job-1")
			Expect(err).NotTo(HaveOccurred())
			Expect(failed.Status).To(Equal(jobpool.JobStatusPending))
			Expect(failed.ErrorMessage).To(Equal("error message"))
			Expect(failed.RetryCount).To(Equal(1))
			Expect(failed.AssigneeID).To(BeEmpty())
		})
	})

	Describe("StopJob", func() {
		It("should stop a job and return freed assignee ID", func() {
			job := &jobpool.Job{
				ID:            "job-1",
				Status:        jobpool.JobStatusPending,
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
			Expect(freedAssigneeIDs).To(Equal([]string{"assignee-1"}))

			stopped, err := backend.GetJob(ctx, "job-1")
			Expect(err).NotTo(HaveOccurred())
			Expect(stopped.Status).To(Equal(jobpool.JobStatusStopped))
			Expect(stopped.ErrorMessage).To(Equal("stopped"))
			Expect(stopped.AssigneeID).To(BeEmpty())
		})
	})

	Describe("GetJobStats", func() {
		It("should return stats for jobs with matching tags", func() {
			job1 := &jobpool.Job{
				ID:            "job-1",
				Status:        jobpool.JobStatusPending,
				JobType:       "test",
				JobDefinition: []byte("data1"),
				Tags:          []string{"tag1"},
				CreatedAt:     time.Now(),
			}
			job2 := &jobpool.Job{
				ID:            "job-2",
				Status:        jobpool.JobStatusPending,
				JobType:       "test",
				JobDefinition: []byte("data2"),
				Tags:          []string{"tag1", "tag2"},
				CreatedAt:     time.Now(),
			}
			job3 := &jobpool.Job{
				ID:            "job-3",
				Status:        jobpool.JobStatusPending,
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

// SQLite backend tests are in queue_test.go with //go:build sqlite tag
