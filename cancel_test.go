//go:build sqlite
// +build sqlite

package jobpool_test

import (
	"context"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/VsevolodSauta/jobpool"
)

func TestJobCancellation(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Job Cancellation Suite")
}

var _ = Describe("Job Cancellation", func() {
	var (
		backend jobpool.Backend
		queue   jobpool.Queue
		ctx     context.Context
		tmpFile *os.File
	)

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		tmpFile, err = os.CreateTemp("", "test_jobpool_*.db")
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

	Describe("Cancelling pending jobs", func() {
		Context("when a job is pending", func() {
			It("should mark the job as unscheduled", func() {
				// Given: A pending job in the queue
				job := &jobpool.Job{
					ID:            "job-1",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// When: The job is cancelled
				err = queue.CancelJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())

				// Then: The job status should be unscheduled
				cancelledJob, err := queue.GetJob(ctx, "job-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(cancelledJob.Status).To(Equal(jobpool.JobStatusUnscheduled))
			})
		})
	})

	Describe("Cancelling running jobs", func() {
		Context("when a job is running", func() {
			It("should mark the job as cancelling", func() {
				// Given: A running job in the queue
				job := &jobpool.Job{
					ID:            "job-2",
					Status:        jobpool.JobStatusRunning,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
					AssigneeID:    "worker-1",
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				// Manually set status to running (normally done by DequeueJobs)
				err = queue.UpdateJobStatus(ctx, "job-2", jobpool.JobStatusRunning, nil, "")
				Expect(err).NotTo(HaveOccurred())

				// When: The job is cancelled
				err = queue.CancelJob(ctx, "job-2")
				Expect(err).NotTo(HaveOccurred())

				// Then: The job status should be cancelling
				cancelledJob, err := queue.GetJob(ctx, "job-2")
				Expect(err).NotTo(HaveOccurred())
				Expect(cancelledJob.Status).To(Equal(jobpool.JobStatusCancelling))
			})
		})

		Context("when a job is already in cancelling state", func() {
			It("should return an error", func() {
				// Given: A job in cancelling state
				job := &jobpool.Job{
					ID:            "job-cancelling",
					Status:        jobpool.JobStatusRunning,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
					AssigneeID:    "worker-1",
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				err = queue.UpdateJobStatus(ctx, "job-cancelling", jobpool.JobStatusRunning, nil, "")
				Expect(err).NotTo(HaveOccurred())
				err = queue.CancelJob(ctx, "job-cancelling")
				Expect(err).NotTo(HaveOccurred())

				// When: The job is cancelled again
				err = queue.CancelJob(ctx, "job-cancelling")

				// Then: An error should be returned
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("already in cancelling state"))
			})
		})
	})

	Describe("Cancelling jobs in terminal states", func() {
		Context("when a job is already completed", func() {
			It("should return an error", func() {
				// Given: A completed job
				job := &jobpool.Job{
					ID:            "job-3",
					Status:        jobpool.JobStatusCompleted,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				err = queue.UpdateJobStatus(ctx, "job-3", jobpool.JobStatusCompleted, []byte("result"), "")
				Expect(err).NotTo(HaveOccurred())

				// When: The job is cancelled
				err = queue.CancelJob(ctx, "job-3")

				// Then: An error should be returned
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("terminal state"))
			})
		})

		Context("when a job is already failed", func() {
			It("should transition to stopped", func() {
				// Given: A failed job
				job := &jobpool.Job{
					ID:            "job-4",
					Status:        jobpool.JobStatusFailed,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				err = queue.UpdateJobStatus(ctx, "job-4", jobpool.JobStatusFailed, nil, "error")
				Expect(err).NotTo(HaveOccurred())

				// When: The job is cancelled
				err = queue.CancelJob(ctx, "job-4")
				Expect(err).NotTo(HaveOccurred())

				// Then: The job should transition to stopped
				cancelledJob, err := queue.GetJob(ctx, "job-4")
				Expect(err).NotTo(HaveOccurred())
				Expect(cancelledJob.Status).To(Equal(jobpool.JobStatusStopped))
			})
		})

		Context("when a job is in unknown_retry state", func() {
			It("should transition to stopped", func() {
				// Given: A job in unknown_retry state
				job := &jobpool.Job{
					ID:            "job-unknown-retry",
					Status:        jobpool.JobStatusUnknownRetry,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// When: The job is cancelled
				err = queue.CancelJob(ctx, "job-unknown-retry")
				Expect(err).NotTo(HaveOccurred())

				// Then: The job should transition to stopped
				cancelledJob, err := queue.GetJob(ctx, "job-unknown-retry")
				Expect(err).NotTo(HaveOccurred())
				Expect(cancelledJob.Status).To(Equal(jobpool.JobStatusStopped))
			})
		})
	})

	Describe("GetJob functionality", func() {
		Context("when a job exists", func() {
			It("should return the job", func() {
				// Given: A job in the queue
				job := &jobpool.Job{
					ID:            "job-5",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"tag1", "tag2"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// When: Getting the job
				retrievedJob, err := queue.GetJob(ctx, "job-5")

				// Then: The job should be returned with all fields
				Expect(err).NotTo(HaveOccurred())
				Expect(retrievedJob.ID).To(Equal("job-5"))
				Expect(retrievedJob.JobType).To(Equal("test"))
				Expect(retrievedJob.Tags).To(Equal([]string{"tag1", "tag2"}))
			})
		})

		Context("when a job does not exist", func() {
			It("should return an error", func() {
				// When: Getting a non-existent job
				_, err := queue.GetJob(ctx, "non-existent")

				// Then: An error should be returned
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("not found"))
			})
		})
	})

	Describe("Worker disconnect behavior", func() {
		Context("when a worker disconnects", func() {
			It("should mark all running and cancelling jobs assigned to that worker as unknown", func() {
				// Given: Multiple running and cancelling jobs assigned to a worker
				job1 := &jobpool.Job{
					ID:            "job-worker-1",
					Status:        jobpool.JobStatusRunning,
					JobType:       "test",
					JobDefinition: []byte("test1"),
					CreatedAt:     time.Now(),
					AssigneeID:    "worker-1",
				}
				job2 := &jobpool.Job{
					ID:            "job-worker-2",
					Status:        jobpool.JobStatusRunning,
					JobType:       "test",
					JobDefinition: []byte("test2"),
					CreatedAt:     time.Now(),
					AssigneeID:    "worker-1",
				}
				job3 := &jobpool.Job{
					ID:            "job-worker-3",
					Status:        jobpool.JobStatusRunning,
					JobType:       "test",
					JobDefinition: []byte("test3"),
					CreatedAt:     time.Now(),
					AssigneeID:    "worker-1",
				}
				_, err := queue.EnqueueJob(ctx, job1)
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.EnqueueJob(ctx, job2)
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.EnqueueJob(ctx, job3)
				Expect(err).NotTo(HaveOccurred())
				// Manually set status to running
				err = queue.UpdateJobStatus(ctx, "job-worker-1", jobpool.JobStatusRunning, nil, "")
				Expect(err).NotTo(HaveOccurred())
				err = queue.UpdateJobStatus(ctx, "job-worker-2", jobpool.JobStatusRunning, nil, "")
				Expect(err).NotTo(HaveOccurred())
				err = queue.UpdateJobStatus(ctx, "job-worker-3", jobpool.JobStatusRunning, nil, "")
				Expect(err).NotTo(HaveOccurred())
				// Cancel job3 to put it in cancelling state
				err = queue.CancelJob(ctx, "job-worker-3")
				Expect(err).NotTo(HaveOccurred())

				// When: Worker disconnects (simulated by marking jobs as unknown)
				err = queue.MarkJobsAsUnknown(ctx, "worker-1")
				Expect(err).NotTo(HaveOccurred())

				// Then: All running and cancelling jobs should be marked as unknown
				job1After, err := queue.GetJob(ctx, "job-worker-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(job1After.Status).To(Equal(jobpool.JobStatusUnknownRetry))

				job2After, err := queue.GetJob(ctx, "job-worker-2")
				Expect(err).NotTo(HaveOccurred())
				Expect(job2After.Status).To(Equal(jobpool.JobStatusUnknownRetry))

				job3After, err := queue.GetJob(ctx, "job-worker-3")
				Expect(err).NotTo(HaveOccurred())
				Expect(job3After.Status).To(Equal(jobpool.JobStatusUnknownRetry))
			})
		})
	})

	Describe("Cancelling state transitions", func() {
		Context("when a job in cancelling state transitions to completed", func() {
			It("should allow transition from cancelling to completed", func() {
				// Given: A job in cancelling state
				job := &jobpool.Job{
					ID:            "job-cancelling-complete",
					Status:        jobpool.JobStatusRunning,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
					AssigneeID:    "worker-1",
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				err = queue.UpdateJobStatus(ctx, "job-cancelling-complete", jobpool.JobStatusRunning, nil, "")
				Expect(err).NotTo(HaveOccurred())
				err = queue.CancelJob(ctx, "job-cancelling-complete")
				Expect(err).NotTo(HaveOccurred())

				// When: Job status is updated to completed
				err = queue.UpdateJobStatus(ctx, "job-cancelling-complete", jobpool.JobStatusCompleted, []byte("result"), "")
				Expect(err).NotTo(HaveOccurred())

				// Then: Job status should be completed
				updatedJob, err := queue.GetJob(ctx, "job-cancelling-complete")
				Expect(err).NotTo(HaveOccurred())
				Expect(updatedJob.Status).To(Equal(jobpool.JobStatusCompleted))
			})
		})

		Context("when a job in cancelling state transitions to stopped", func() {
			It("should allow transition from cancelling to stopped", func() {
				// Given: A job in cancelling state
				job := &jobpool.Job{
					ID:            "job-cancelling-stopped",
					Status:        jobpool.JobStatusRunning,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
					AssigneeID:    "worker-1",
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				err = queue.UpdateJobStatus(ctx, "job-cancelling-stopped", jobpool.JobStatusRunning, nil, "")
				Expect(err).NotTo(HaveOccurred())
				err = queue.CancelJob(ctx, "job-cancelling-stopped")
				Expect(err).NotTo(HaveOccurred())

				// When: Job status is updated to stopped
				err = queue.UpdateJobStatus(ctx, "job-cancelling-stopped", jobpool.JobStatusStopped, nil, "cancelled")
				Expect(err).NotTo(HaveOccurred())

				// Then: Job status should be stopped
				updatedJob, err := queue.GetJob(ctx, "job-cancelling-stopped")
				Expect(err).NotTo(HaveOccurred())
				Expect(updatedJob.Status).To(Equal(jobpool.JobStatusStopped))
			})
		})

		Context("when a job in cancelling state transitions to unknown", func() {
			It("should allow transition from cancelling to unknown", func() {
				// Given: A job in cancelling state
				job := &jobpool.Job{
					ID:            "job-cancelling-unknown",
					Status:        jobpool.JobStatusRunning,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
					AssigneeID:    "worker-1",
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				err = queue.UpdateJobStatus(ctx, "job-cancelling-unknown", jobpool.JobStatusRunning, nil, "")
				Expect(err).NotTo(HaveOccurred())
				err = queue.CancelJob(ctx, "job-cancelling-unknown")
				Expect(err).NotTo(HaveOccurred())

				// When: Job status is updated to unknown
				err = queue.UpdateJobStatus(ctx, "job-cancelling-unknown", jobpool.JobStatusUnknownStopped, nil, "worker timeout")
				Expect(err).NotTo(HaveOccurred())

				// Then: Job status should be unknown
				updatedJob, err := queue.GetJob(ctx, "job-cancelling-unknown")
				Expect(err).NotTo(HaveOccurred())
				Expect(updatedJob.Status).To(Equal(jobpool.JobStatusUnknownStopped))
			})
		})

		Context("when attempting invalid transition from cancelling", func() {
			It("should return an error for invalid transition", func() {
				// Given: A job in cancelling state
				job := &jobpool.Job{
					ID:            "job-cancelling-invalid",
					Status:        jobpool.JobStatusRunning,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
					AssigneeID:    "worker-1",
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				err = queue.UpdateJobStatus(ctx, "job-cancelling-invalid", jobpool.JobStatusRunning, nil, "")
				Expect(err).NotTo(HaveOccurred())
				err = queue.CancelJob(ctx, "job-cancelling-invalid")
				Expect(err).NotTo(HaveOccurred())

				// When: Attempting to transition to an invalid state (e.g., PENDING)
				err = queue.UpdateJobStatus(ctx, "job-cancelling-invalid", jobpool.JobStatusPending, nil, "")

				// Then: An error should be returned
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("invalid transition from CANCELLING"))
			})
		})
	})

	Describe("Service restart behavior", func() {
		Context("when service restarts with running and cancelling jobs", func() {
			It("should mark all running and cancelling jobs as unknown", func() {
				// Given: Multiple running and cancelling jobs in the queue
				job1 := &jobpool.Job{
					ID:            "job-restart-1",
					Status:        jobpool.JobStatusRunning,
					JobType:       "test",
					JobDefinition: []byte("test1"),
					CreatedAt:     time.Now(),
					AssigneeID:    "worker-1",
				}
				job2 := &jobpool.Job{
					ID:            "job-restart-2",
					Status:        jobpool.JobStatusRunning,
					JobType:       "test",
					JobDefinition: []byte("test2"),
					CreatedAt:     time.Now(),
					AssigneeID:    "worker-2",
				}
				job3 := &jobpool.Job{
					ID:            "job-restart-3",
					Status:        jobpool.JobStatusRunning,
					JobType:       "test",
					JobDefinition: []byte("test3"),
					CreatedAt:     time.Now(),
					AssigneeID:    "worker-3",
				}
				_, err := queue.EnqueueJob(ctx, job1)
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.EnqueueJob(ctx, job2)
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.EnqueueJob(ctx, job3)
				Expect(err).NotTo(HaveOccurred())
				// Manually set status to running
				err = queue.UpdateJobStatus(ctx, "job-restart-1", jobpool.JobStatusRunning, nil, "")
				Expect(err).NotTo(HaveOccurred())
				err = queue.UpdateJobStatus(ctx, "job-restart-2", jobpool.JobStatusRunning, nil, "")
				Expect(err).NotTo(HaveOccurred())
				err = queue.UpdateJobStatus(ctx, "job-restart-3", jobpool.JobStatusRunning, nil, "")
				Expect(err).NotTo(HaveOccurred())
				// Cancel job3 to put it in cancelling state
				err = queue.CancelJob(ctx, "job-restart-3")
				Expect(err).NotTo(HaveOccurred())

				// When: Service restarts (simulated by calling ResetRunningJobs)
				err = queue.ResetRunningJobs(ctx)
				Expect(err).NotTo(HaveOccurred())

				// Then: All running and cancelling jobs should be marked as unknown
				job1After, err := queue.GetJob(ctx, "job-restart-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(job1After.Status).To(Equal(jobpool.JobStatusUnknownRetry))
				Expect(job1After.AssigneeID).To(BeEmpty())

				job2After, err := queue.GetJob(ctx, "job-restart-2")
				Expect(err).NotTo(HaveOccurred())
				Expect(job2After.Status).To(Equal(jobpool.JobStatusUnknownRetry))
				Expect(job2After.AssigneeID).To(BeEmpty())

				job3After, err := queue.GetJob(ctx, "job-restart-3")
				Expect(err).NotTo(HaveOccurred())
				Expect(job3After.Status).To(Equal(jobpool.JobStatusUnknownRetry))
				Expect(job3After.AssigneeID).To(BeEmpty())
			})
		})

		Context("when service restarts with no running jobs", func() {
			It("should not affect pending or completed jobs", func() {
				// Given: Pending and completed jobs in the queue
				pendingJob := &jobpool.Job{
					ID:            "job-pending",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				completedJob := &jobpool.Job{
					ID:            "job-completed",
					Status:        jobpool.JobStatusCompleted,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, pendingJob)
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.EnqueueJob(ctx, completedJob)
				Expect(err).NotTo(HaveOccurred())
				err = queue.UpdateJobStatus(ctx, "job-completed", jobpool.JobStatusCompleted, []byte("result"), "")
				Expect(err).NotTo(HaveOccurred())

				// When: Service restarts
				err = queue.ResetRunningJobs(ctx)
				Expect(err).NotTo(HaveOccurred())

				// Then: Pending and completed jobs should remain unchanged
				pendingAfter, err := queue.GetJob(ctx, "job-pending")
				Expect(err).NotTo(HaveOccurred())
				Expect(pendingAfter.Status).To(Equal(jobpool.JobStatusPending))

				completedAfter, err := queue.GetJob(ctx, "job-completed")
				Expect(err).NotTo(HaveOccurred())
				Expect(completedAfter.Status).To(Equal(jobpool.JobStatusCompleted))
			})
		})
	})
})
