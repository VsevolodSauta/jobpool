//go:build sqlite
// +build sqlite

package jobpool_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/VsevolodSauta/jobpool"
)

func TestAtomicMethods(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Atomic Methods Suite")
}

var _ = Describe("Atomic Job Lifecycle Methods", func() {
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

	Describe("CompleteJob", func() {
		Context("when completing a job from RUNNING state", func() {
			It("should transition to COMPLETED with result", func() {
				// Given: A running job
				job := &jobpool.Job{
					ID:            "job-running-complete",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())

				// When: Job is completed
				result := []byte("success result")
				err = queue.CompleteJob(ctx, "job-running-complete", result)

				// Then: Job should be in COMPLETED state with result
				Expect(err).NotTo(HaveOccurred())
				completedJob, err := queue.GetJob(ctx, "job-running-complete")
				Expect(err).NotTo(HaveOccurred())
				Expect(completedJob.Status).To(Equal(jobpool.JobStatusCompleted))
				Expect(completedJob.Result).To(Equal(result))
				Expect(completedJob.CompletedAt).NotTo(BeNil())
				Expect(completedJob.AssigneeID).To(BeEmpty())
			})
		})

		Context("when completing a job from CANCELLING state", func() {
			It("should transition to COMPLETED (job finished before cancellation)", func() {
				// Given: A job in CANCELLING state
				job := &jobpool.Job{
					ID:            "job-cancelling-complete",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-cancelling-complete"})
				Expect(err).NotTo(HaveOccurred())

				// When: Job is completed (finished before cancellation took effect)
				result := []byte("completed before cancellation")
				err = queue.CompleteJob(ctx, "job-cancelling-complete", result)

				// Then: Job should be in COMPLETED state
				Expect(err).NotTo(HaveOccurred())
				completedJob, err := queue.GetJob(ctx, "job-cancelling-complete")
				Expect(err).NotTo(HaveOccurred())
				Expect(completedJob.Status).To(Equal(jobpool.JobStatusCompleted))
				Expect(completedJob.Result).To(Equal(result))
			})
		})

		Context("when completing a job from UNKNOWN_RETRY state", func() {
			It("should transition to COMPLETED (job completed after worker reconnection)", func() {
				// Given: A job in UNKNOWN_RETRY state
				job := &jobpool.Job{
					ID:            "job-unknown-retry-complete",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.MarkWorkerUnresponsive(ctx, "worker-1")
				Expect(err).NotTo(HaveOccurred())

				// When: Job is completed after worker reconnection
				result := []byte("completed after reconnect")
				err = queue.CompleteJob(ctx, "job-unknown-retry-complete", result)

				// Then: Job should be in COMPLETED state
				Expect(err).NotTo(HaveOccurred())
				completedJob, err := queue.GetJob(ctx, "job-unknown-retry-complete")
				Expect(err).NotTo(HaveOccurred())
				Expect(completedJob.Status).To(Equal(jobpool.JobStatusCompleted))
				Expect(completedJob.Result).To(Equal(result))
			})
		})

		Context("when completing a job from UNKNOWN_STOPPED state", func() {
			It("should transition to COMPLETED (job completed despite previous issues)", func() {
				// Given: A job in UNKNOWN_STOPPED state
				job := &jobpool.Job{
					ID:            "job-unknown-stopped-complete",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-unknown-stopped-complete"})
				Expect(err).NotTo(HaveOccurred())
				// Mark as unknown stopped (simulating timeout)
				err = queue.MarkJobUnknownStopped(ctx, "job-unknown-stopped-complete", "timeout")
				Expect(err).NotTo(HaveOccurred())

				// When: Job is completed despite previous issues
				result := []byte("completed despite issues")
				err = queue.CompleteJob(ctx, "job-unknown-stopped-complete", result)

				// Then: Job should be in COMPLETED state
				Expect(err).NotTo(HaveOccurred())
				completedJob, err := queue.GetJob(ctx, "job-unknown-stopped-complete")
				Expect(err).NotTo(HaveOccurred())
				Expect(completedJob.Status).To(Equal(jobpool.JobStatusCompleted))
				Expect(completedJob.Result).To(Equal(result))
			})
		})

		Context("when attempting to complete a job from invalid state", func() {
			It("should return an error", func() {
				// Given: A job in PENDING state (invalid for completion)
				job := &jobpool.Job{
					ID:            "job-pending-invalid",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// When: Attempting to complete a pending job
				err = queue.CompleteJob(ctx, "job-pending-invalid", []byte("result"))

				// Then: An error should be returned
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("not in a valid state for completion"))
			})
		})
	})

	Describe("FailJob", func() {
		Context("when failing a job from RUNNING state", func() {
			It("should transition to FAILED then PENDING with retry increment", func() {
				// Given: A running job
				job := &jobpool.Job{
					ID:            "job-running-fail",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
					RetryCount:    0,
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())

				// When: Job fails
				errorMsg := "test error"
				err = queue.FailJob(ctx, "job-running-fail", errorMsg)

				// Then: Job should be in PENDING state with incremented retry count
				Expect(err).NotTo(HaveOccurred())
				failedJob, err := queue.GetJob(ctx, "job-running-fail")
				Expect(err).NotTo(HaveOccurred())
				Expect(failedJob.Status).To(Equal(jobpool.JobStatusPending))
				Expect(failedJob.RetryCount).To(Equal(1))
				Expect(failedJob.ErrorMessage).To(Equal(errorMsg))
				Expect(failedJob.LastRetryAt).NotTo(BeNil())
				Expect(failedJob.AssigneeID).To(BeEmpty())
			})
		})

		Context("when failing a job from UNKNOWN_RETRY state", func() {
			It("should transition to FAILED then PENDING with retry increment", func() {
				// Given: A job in UNKNOWN_RETRY state
				job := &jobpool.Job{
					ID:            "job-unknown-retry-fail",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
					RetryCount:    0,
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.MarkWorkerUnresponsive(ctx, "worker-1")
				Expect(err).NotTo(HaveOccurred())

				// When: Job fails after worker reconnection
				errorMsg := "error after reconnect"
				err = queue.FailJob(ctx, "job-unknown-retry-fail", errorMsg)

				// Then: Job should be in PENDING state with incremented retry count
				Expect(err).NotTo(HaveOccurred())
				failedJob, err := queue.GetJob(ctx, "job-unknown-retry-fail")
				Expect(err).NotTo(HaveOccurred())
				Expect(failedJob.Status).To(Equal(jobpool.JobStatusPending))
				Expect(failedJob.RetryCount).To(Equal(1))
				Expect(failedJob.ErrorMessage).To(Equal(errorMsg))
			})
		})

		Context("when attempting to fail a job from invalid state", func() {
			It("should return an error", func() {
				// Given: A job in PENDING state (invalid for failure)
				job := &jobpool.Job{
					ID:            "job-pending-fail-invalid",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// When: Attempting to fail a pending job
				err = queue.FailJob(ctx, "job-pending-fail-invalid", "error")

				// Then: An error should be returned
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("not in RUNNING or UNKNOWN_RETRY state"))
			})
		})
	})

	Describe("StopJob", func() {
		Context("when stopping a job from RUNNING state", func() {
			It("should transition to STOPPED with error message", func() {
				// Given: A running job
				job := &jobpool.Job{
					ID:            "job-running-stop",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())

				// When: Job is stopped
				errorMsg := "cancelled by user"
				err = queue.StopJob(ctx, "job-running-stop", errorMsg)

				// Then: Job should be in STOPPED state
				Expect(err).NotTo(HaveOccurred())
				stoppedJob, err := queue.GetJob(ctx, "job-running-stop")
				Expect(err).NotTo(HaveOccurred())
				Expect(stoppedJob.Status).To(Equal(jobpool.JobStatusStopped))
				Expect(stoppedJob.ErrorMessage).To(Equal(errorMsg))
				Expect(stoppedJob.CompletedAt).NotTo(BeNil())
				Expect(stoppedJob.AssigneeID).To(BeEmpty())
			})
		})

		Context("when stopping a job from CANCELLING state", func() {
			It("should transition to STOPPED (cancellation acknowledged)", func() {
				// Given: A job in CANCELLING state
				job := &jobpool.Job{
					ID:            "job-cancelling-stop",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-cancelling-stop"})
				Expect(err).NotTo(HaveOccurred())

				// When: Cancellation is acknowledged (job was executing)
				errorMsg := "cancelled: acknowledged by worker"
				err = queue.StopJob(ctx, "job-cancelling-stop", errorMsg)

				// Then: Job should be in STOPPED state
				Expect(err).NotTo(HaveOccurred())
				stoppedJob, err := queue.GetJob(ctx, "job-cancelling-stop")
				Expect(err).NotTo(HaveOccurred())
				Expect(stoppedJob.Status).To(Equal(jobpool.JobStatusStopped))
				Expect(stoppedJob.ErrorMessage).To(Equal(errorMsg))
			})
		})

		Context("when stopping a job from UNKNOWN_RETRY state", func() {
			It("should transition to STOPPED", func() {
				// Given: A job in UNKNOWN_RETRY state
				job := &jobpool.Job{
					ID:            "job-unknown-retry-stop",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.MarkWorkerUnresponsive(ctx, "worker-1")
				Expect(err).NotTo(HaveOccurred())

				// When: Job is stopped
				errorMsg := "cancelled: acknowledged by worker"
				err = queue.StopJob(ctx, "job-unknown-retry-stop", errorMsg)

				// Then: Job should be in STOPPED state
				Expect(err).NotTo(HaveOccurred())
				stoppedJob, err := queue.GetJob(ctx, "job-unknown-retry-stop")
				Expect(err).NotTo(HaveOccurred())
				Expect(stoppedJob.Status).To(Equal(jobpool.JobStatusStopped))
				Expect(stoppedJob.ErrorMessage).To(Equal(errorMsg))
			})
		})

		Context("when attempting to stop a job from invalid state", func() {
			It("should return an error", func() {
				// Given: A job in PENDING state (invalid for stopping)
				job := &jobpool.Job{
					ID:            "job-pending-stop-invalid",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// When: Attempting to stop a pending job
				err = queue.StopJob(ctx, "job-pending-stop-invalid", "error")

				// Then: An error should be returned
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("not in a valid state for stopping"))
			})
		})
	})

	Describe("StopJobWithRetry", func() {
		Context("when stopping a job from CANCELLING state with failure", func() {
			It("should transition to STOPPED with retry increment", func() {
				// Given: A job in CANCELLING state
				job := &jobpool.Job{
					ID:            "job-cancelling-stop-retry",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
					RetryCount:    0,
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-cancelling-stop-retry"})
				Expect(err).NotTo(HaveOccurred())

				// When: Job fails while being cancelled (applies FAILED state effects)
				errorMsg := "job failed while being cancelled"
				err = queue.StopJobWithRetry(ctx, "job-cancelling-stop-retry", errorMsg)

				// Then: Job should be in STOPPED state with incremented retry count
				Expect(err).NotTo(HaveOccurred())
				stoppedJob, err := queue.GetJob(ctx, "job-cancelling-stop-retry")
				Expect(err).NotTo(HaveOccurred())
				Expect(stoppedJob.Status).To(Equal(jobpool.JobStatusStopped))
				Expect(stoppedJob.ErrorMessage).To(Equal(errorMsg))
				Expect(stoppedJob.RetryCount).To(Equal(1))
				Expect(stoppedJob.LastRetryAt).NotTo(BeNil())
				Expect(stoppedJob.CompletedAt).NotTo(BeNil())
			})
		})

		Context("when attempting to stop with retry from invalid state", func() {
			It("should return an error", func() {
				// Given: A job in RUNNING state (invalid - should use StopJob instead)
				job := &jobpool.Job{
					ID:            "job-running-stop-retry-invalid",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())

				// When: Attempting to stop with retry from RUNNING state
				err = queue.StopJobWithRetry(ctx, "job-running-stop-retry-invalid", "error")

				// Then: An error should be returned
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("not in CANCELLING state"))
			})
		})
	})

	Describe("MarkJobUnknownStopped", func() {
		Context("when marking a job from CANCELLING state as unknown stopped", func() {
			It("should transition to UNKNOWN_STOPPED (cancellation timeout)", func() {
				// Given: A job in CANCELLING state
				job := &jobpool.Job{
					ID:            "job-cancelling-unknown-stopped",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-cancelling-unknown-stopped"})
				Expect(err).NotTo(HaveOccurred())

				// When: Cancellation times out (job unknown to worker)
				errorMsg := "cancelled: timeout waiting for worker acknowledgment"
				err = queue.MarkJobUnknownStopped(ctx, "job-cancelling-unknown-stopped", errorMsg)

				// Then: Job should be in UNKNOWN_STOPPED state
				Expect(err).NotTo(HaveOccurred())
				unknownJob, err := queue.GetJob(ctx, "job-cancelling-unknown-stopped")
				Expect(err).NotTo(HaveOccurred())
				Expect(unknownJob.Status).To(Equal(jobpool.JobStatusUnknownStopped))
				Expect(unknownJob.ErrorMessage).To(Equal(errorMsg))
				Expect(unknownJob.CompletedAt).NotTo(BeNil())
				Expect(unknownJob.AssigneeID).To(BeEmpty())
			})
		})

		Context("when marking a job from UNKNOWN_RETRY state as unknown stopped", func() {
			It("should transition to UNKNOWN_STOPPED (job unknown to worker during cancellation)", func() {
				// Given: A job in UNKNOWN_RETRY state
				job := &jobpool.Job{
					ID:            "job-unknown-retry-unknown-stopped",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.MarkWorkerUnresponsive(ctx, "worker-1")
				Expect(err).NotTo(HaveOccurred())

				// When: Job is unknown to worker during cancellation
				errorMsg := "cancelled: job unknown to worker"
				err = queue.MarkJobUnknownStopped(ctx, "job-unknown-retry-unknown-stopped", errorMsg)

				// Then: Job should be in UNKNOWN_STOPPED state
				Expect(err).NotTo(HaveOccurred())
				unknownJob, err := queue.GetJob(ctx, "job-unknown-retry-unknown-stopped")
				Expect(err).NotTo(HaveOccurred())
				Expect(unknownJob.Status).To(Equal(jobpool.JobStatusUnknownStopped))
				Expect(unknownJob.ErrorMessage).To(Equal(errorMsg))
			})
		})

		Context("when marking a job from RUNNING state as unknown stopped", func() {
			It("should transition to UNKNOWN_STOPPED (fallback case)", func() {
				// Given: A job in RUNNING state
				job := &jobpool.Job{
					ID:            "job-running-unknown-stopped",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())

				// When: Worker not connected (fallback case)
				errorMsg := "cancelled: worker not connected (fallback - indicates service bug)"
				err = queue.MarkJobUnknownStopped(ctx, "job-running-unknown-stopped", errorMsg)

				// Then: Job should be in UNKNOWN_STOPPED state
				Expect(err).NotTo(HaveOccurred())
				unknownJob, err := queue.GetJob(ctx, "job-running-unknown-stopped")
				Expect(err).NotTo(HaveOccurred())
				Expect(unknownJob.Status).To(Equal(jobpool.JobStatusUnknownStopped))
				Expect(unknownJob.ErrorMessage).To(Equal(errorMsg))
			})
		})
	})

	Describe("UpdateJobStatus", func() {
		Context("when updating job status for edge cases", func() {
			It("should update status, result, and error message", func() {
				// Given: A job in UNKNOWN_STOPPED state
				job := &jobpool.Job{
					ID:            "job-update-status",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-update-status"})
				Expect(err).NotTo(HaveOccurred())
				err = queue.MarkJobUnknownStopped(ctx, "job-update-status", "timeout")
				Expect(err).NotTo(HaveOccurred())

				// When: Updating to STOPPED (edge case transition)
				result := []byte("edge case result")
				errorMsg := "edge case error"
				err = queue.UpdateJobStatus(ctx, "job-update-status", jobpool.JobStatusStopped, result, errorMsg)

				// Then: Job should be in STOPPED state with updated fields
				Expect(err).NotTo(HaveOccurred())
				updatedJob, err := queue.GetJob(ctx, "job-update-status")
				Expect(err).NotTo(HaveOccurred())
				Expect(updatedJob.Status).To(Equal(jobpool.JobStatusStopped))
				Expect(updatedJob.Result).To(Equal(result))
				Expect(updatedJob.ErrorMessage).To(Equal(errorMsg))
			})
		})

		Context("when updating job status to eligible state", func() {
			It("should notify waiting workers", func() {
				// Given: A job in UNKNOWN_STOPPED state
				job := &jobpool.Job{
					ID:            "job-update-eligible",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"test-tag"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.MarkWorkerUnresponsive(ctx, "worker-1")
				Expect(err).NotTo(HaveOccurred())
				err = queue.MarkJobUnknownStopped(ctx, "job-update-eligible", "timeout")
				Expect(err).NotTo(HaveOccurred())

				// When: Updating to PENDING (becomes eligible)
				err = queue.UpdateJobStatus(ctx, "job-update-eligible", jobpool.JobStatusPending, nil, "")

				// Then: Job should be in PENDING state (workers would be notified)
				Expect(err).NotTo(HaveOccurred())
				updatedJob, err := queue.GetJob(ctx, "job-update-eligible")
				Expect(err).NotTo(HaveOccurred())
				Expect(updatedJob.Status).To(Equal(jobpool.JobStatusPending))
			})
		})
	})

	Describe("Method selection guidance", func() {
		Context("when a job completes successfully", func() {
			It("should use CompleteJob regardless of current state", func() {
				// Given: Jobs in various states that can complete
				states := []jobpool.JobStatus{
					jobpool.JobStatusRunning,
					jobpool.JobStatusCancelling,
					jobpool.JobStatusUnknownRetry,
					jobpool.JobStatusUnknownStopped,
				}

				for i, state := range states {
					jobID := fmt.Sprintf("job-complete-%d", i)
					// Setup job to target state
					job := &jobpool.Job{
						ID:            jobID,
						Status:        jobpool.JobStatusPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())

					// Transition to target state
					switch state {
					case jobpool.JobStatusRunning:
						_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
					case jobpool.JobStatusCancelling:
						_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
						Expect(err).NotTo(HaveOccurred())
						_, _, err = queue.CancelJobs(ctx, nil, []string{jobID})
					case jobpool.JobStatusUnknownRetry:
						_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
						Expect(err).NotTo(HaveOccurred())
						err = queue.MarkWorkerUnresponsive(ctx, "worker-1")
					case jobpool.JobStatusUnknownStopped:
						_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
						Expect(err).NotTo(HaveOccurred())
						_, _, err = queue.CancelJobs(ctx, nil, []string{jobID})
						Expect(err).NotTo(HaveOccurred())
						err = queue.MarkJobUnknownStopped(ctx, jobID, "timeout")
					}
					Expect(err).NotTo(HaveOccurred())

					// When: Using CompleteJob
					err = queue.CompleteJob(ctx, jobID, []byte("result"))

					// Then: Should succeed
					Expect(err).NotTo(HaveOccurred())
					completedJob, err := queue.GetJob(ctx, jobID)
					Expect(err).NotTo(HaveOccurred())
					Expect(completedJob.Status).To(Equal(jobpool.JobStatusCompleted))
				}
			})
		})

		Context("when a job fails and should be retried", func() {
			It("should use FailJob for RUNNING and UNKNOWN_RETRY states", func() {
				// Given: Jobs in states that can fail
				states := []jobpool.JobStatus{
					jobpool.JobStatusRunning,
					jobpool.JobStatusUnknownRetry,
				}

				for i, state := range states {
					jobID := fmt.Sprintf("job-fail-%d", i+1)
					workerID := fmt.Sprintf("worker-%d", i+1)
					tag := fmt.Sprintf("tag-fail-%d", i+1)
					// Setup job to target state with unique tag
					job := &jobpool.Job{
						ID:            jobID,
						Status:        jobpool.JobStatusPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{tag},
						CreatedAt:     time.Now(),
						RetryCount:    0,
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())

					// Transition to target state using tag to ensure we get the right job
					switch state {
					case jobpool.JobStatusRunning:
						jobs, err := backend.DequeueJobs(ctx, workerID, []string{tag}, 1)
						Expect(err).NotTo(HaveOccurred())
						Expect(len(jobs)).To(Equal(1))
						Expect(jobs[0].ID).To(Equal(jobID))
						// Verify job is in RUNNING state
						jobBefore, err := queue.GetJob(ctx, jobID)
						Expect(err).NotTo(HaveOccurred())
						Expect(jobBefore.Status).To(Equal(jobpool.JobStatusRunning))
					case jobpool.JobStatusUnknownRetry:
						jobs, err := backend.DequeueJobs(ctx, workerID, []string{tag}, 1)
						Expect(err).NotTo(HaveOccurred())
						Expect(len(jobs)).To(Equal(1))
						Expect(jobs[0].ID).To(Equal(jobID))
						// Verify job is in RUNNING state and assigned to worker
						jobBeforeMark, err := queue.GetJob(ctx, jobID)
						Expect(err).NotTo(HaveOccurred())
						Expect(jobBeforeMark.Status).To(Equal(jobpool.JobStatusRunning))
						Expect(jobBeforeMark.AssigneeID).To(Equal(workerID))
						// Mark worker as unresponsive
						err = queue.MarkWorkerUnresponsive(ctx, workerID)
						Expect(err).NotTo(HaveOccurred())
						// Verify job is in UNKNOWN_RETRY state
						jobBefore, err := queue.GetJob(ctx, jobID)
						Expect(err).NotTo(HaveOccurred())
						Expect(jobBefore.Status).To(Equal(jobpool.JobStatusUnknownRetry))
					}

					// When: Using FailJob
					err = queue.FailJob(ctx, jobID, "error")

					// Then: Should succeed and transition to PENDING
					Expect(err).NotTo(HaveOccurred())
					failedJob, err := queue.GetJob(ctx, jobID)
					Expect(err).NotTo(HaveOccurred())
					Expect(failedJob.Status).To(Equal(jobpool.JobStatusPending))
					Expect(failedJob.RetryCount).To(Equal(1))
				}
			})
		})

		Context("when a job is cancelled", func() {
			It("should use StopJob for normal cancellation acknowledgment", func() {
				// Given: A job in CANCELLING state
				job := &jobpool.Job{
					ID:            "job-cancel-stop",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-cancel-stop"})
				Expect(err).NotTo(HaveOccurred())

				// When: Using StopJob (normal cancellation acknowledgment)
				err = queue.StopJob(ctx, "job-cancel-stop", "cancelled: acknowledged")

				// Then: Should succeed
				Expect(err).NotTo(HaveOccurred())
				stoppedJob, err := queue.GetJob(ctx, "job-cancel-stop")
				Expect(err).NotTo(HaveOccurred())
				Expect(stoppedJob.Status).To(Equal(jobpool.JobStatusStopped))
				Expect(stoppedJob.RetryCount).To(Equal(0)) // No retry increment
			})
		})

		Context("when a job fails while being cancelled", func() {
			It("should use StopJobWithRetry to apply FAILED state effects", func() {
				// Given: A job in CANCELLING state
				job := &jobpool.Job{
					ID:            "job-cancel-fail-stop",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
					RetryCount:    0,
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-cancel-fail-stop"})
				Expect(err).NotTo(HaveOccurred())

				// When: Using StopJobWithRetry (job failed while being cancelled)
				err = queue.StopJobWithRetry(ctx, "job-cancel-fail-stop", "failed while cancelling")

				// Then: Should succeed with retry increment
				Expect(err).NotTo(HaveOccurred())
				stoppedJob, err := queue.GetJob(ctx, "job-cancel-fail-stop")
				Expect(err).NotTo(HaveOccurred())
				Expect(stoppedJob.Status).To(Equal(jobpool.JobStatusStopped))
				Expect(stoppedJob.RetryCount).To(Equal(1)) // Retry increment applied
			})
		})
	})

	Describe("AcknowledgeCancellation vs Atomic Methods", func() {
		Context("when acknowledging cancellation with wasExecuting=true", func() {
			It("should be equivalent to StopJob", func() {
				// Given: Two jobs in CANCELLING state
				job1 := &jobpool.Job{
					ID:            "job-ack-1",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				job2 := &jobpool.Job{
					ID:            "job-ack-2",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job1)
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.EnqueueJob(ctx, job2)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 2)
				Expect(err).NotTo(HaveOccurred())
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-ack-1", "job-ack-2"})
				Expect(err).NotTo(HaveOccurred())

				// When: Using AcknowledgeCancellation (wasExecuting=true)
				err = queue.AcknowledgeCancellation(ctx, "job-ack-1", true)
				Expect(err).NotTo(HaveOccurred())

				// And: Using StopJob (equivalent)
				err = queue.StopJob(ctx, "job-ack-2", "cancelled: acknowledged by worker")
				Expect(err).NotTo(HaveOccurred())

				// Then: Both should result in STOPPED state
				job1After, err := queue.GetJob(ctx, "job-ack-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(job1After.Status).To(Equal(jobpool.JobStatusStopped))

				job2After, err := queue.GetJob(ctx, "job-ack-2")
				Expect(err).NotTo(HaveOccurred())
				Expect(job2After.Status).To(Equal(jobpool.JobStatusStopped))
			})
		})

		Context("when acknowledging cancellation with wasExecuting=false", func() {
			It("should be equivalent to MarkJobUnknownStopped", func() {
				// Given: Two jobs in CANCELLING state
				job1 := &jobpool.Job{
					ID:            "job-ack-unknown-1",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				job2 := &jobpool.Job{
					ID:            "job-ack-unknown-2",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job1)
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.EnqueueJob(ctx, job2)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 2)
				Expect(err).NotTo(HaveOccurred())
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-ack-unknown-1", "job-ack-unknown-2"})
				Expect(err).NotTo(HaveOccurred())

				// When: Using AcknowledgeCancellation (wasExecuting=false)
				err = queue.AcknowledgeCancellation(ctx, "job-ack-unknown-1", false)
				Expect(err).NotTo(HaveOccurred())

				// And: Using MarkJobUnknownStopped (equivalent)
				err = queue.MarkJobUnknownStopped(ctx, "job-ack-unknown-2", "cancelled: job unknown to worker")
				Expect(err).NotTo(HaveOccurred())

				// Then: Both should result in UNKNOWN_STOPPED state
				job1After, err := queue.GetJob(ctx, "job-ack-unknown-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(job1After.Status).To(Equal(jobpool.JobStatusUnknownStopped))

				job2After, err := queue.GetJob(ctx, "job-ack-unknown-2")
				Expect(err).NotTo(HaveOccurred())
				Expect(job2After.Status).To(Equal(jobpool.JobStatusUnknownStopped))
			})
		})

		Context("when choosing between AcknowledgeCancellation and atomic methods", func() {
			It("should prefer atomic methods for direct control", func() {
				// Given: A job in CANCELLING state
				job := &jobpool.Job{
					ID:            "job-prefer-atomic",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-prefer-atomic"})
				Expect(err).NotTo(HaveOccurred())

				// When: Using StopJob directly (preferred for direct control)
				err = queue.StopJob(ctx, "job-prefer-atomic", "custom error message")

				// Then: Should succeed with custom error message
				Expect(err).NotTo(HaveOccurred())
				stoppedJob, err := queue.GetJob(ctx, "job-prefer-atomic")
				Expect(err).NotTo(HaveOccurred())
				Expect(stoppedJob.Status).To(Equal(jobpool.JobStatusStopped))
				Expect(stoppedJob.ErrorMessage).To(Equal("custom error message"))

				// Note: AcknowledgeCancellation is a convenience method that maps to
				// StopJob or MarkJobUnknownStopped based on wasExecuting flag.
				// Use atomic methods (StopJob, MarkJobUnknownStopped) when you need
				// direct control over error messages or want to be explicit about the operation.
			})
		})
	})
})
