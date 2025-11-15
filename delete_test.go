//go:build sqlite
// +build sqlite

package jobpool_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/VsevolodSauta/jobpool"
)

func TestJobDeletion(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Job Deletion Suite")
}

var _ = Describe("DeleteJobs", func() {
	var (
		backend jobpool.Backend
		queue   jobpool.Queue
		ctx     context.Context
		tmpFile *os.File
	)

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		tmpFile, err = os.CreateTemp("", "test_jobpool_delete_*.db")
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

	Describe("when deleting jobs in final states", func() {
		Context("with COMPLETED jobs", func() {
			It("should successfully delete completed jobs", func() {
				// Given: A completed job
				job := &jobpool.Job{
					ID:            "job-completed-1",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.CompleteJob(ctx, "job-completed-1", []byte("result"))
				Expect(err).NotTo(HaveOccurred())

				// When: DeleteJobs is called
				err = queue.DeleteJobs(ctx, nil, []string{"job-completed-1"})

				// Then: Job should be deleted
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.GetJob(ctx, "job-completed-1")
				Expect(err).To(HaveOccurred())
				Expect(strings.Contains(err.Error(), "not found")).To(BeTrue())
			})
		})

		Context("with UNSCHEDULED jobs", func() {
			It("should successfully delete unscheduled jobs", func() {
				// Given: An unscheduled job (cancelled before assignment)
				job := &jobpool.Job{
					ID:            "job-unscheduled-1",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-unscheduled-1"})
				Expect(err).NotTo(HaveOccurred())

				// Verify job is in UNSCHEDULED state
				cancelledJob, err := queue.GetJob(ctx, "job-unscheduled-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(cancelledJob.Status).To(Equal(jobpool.JobStatusUnscheduled))

				// When: DeleteJobs is called
				err = queue.DeleteJobs(ctx, nil, []string{"job-unscheduled-1"})

				// Then: Job should be deleted
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.GetJob(ctx, "job-unscheduled-1")
				Expect(err).To(HaveOccurred())
			})
		})

		Context("with STOPPED jobs", func() {
			It("should successfully delete stopped jobs", func() {
				// Given: A stopped job (cancelled while running)
				job := &jobpool.Job{
					ID:            "job-stopped-1",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.StopJob(ctx, "job-stopped-1", "stopped")
				Expect(err).NotTo(HaveOccurred())

				// When: DeleteJobs is called
				err = queue.DeleteJobs(ctx, nil, []string{"job-stopped-1"})

				// Then: Job should be deleted
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.GetJob(ctx, "job-stopped-1")
				Expect(err).To(HaveOccurred())
			})
		})

		Context("with UNKNOWN_STOPPED jobs", func() {
			It("should successfully delete unknown_stopped jobs", func() {
				// Given: An unknown_stopped job
				job := &jobpool.Job{
					ID:            "job-unknown-stopped-1",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.MarkJobUnknownStopped(ctx, "job-unknown-stopped-1", "unknown")
				Expect(err).NotTo(HaveOccurred())

				// When: DeleteJobs is called
				err = queue.DeleteJobs(ctx, nil, []string{"job-unknown-stopped-1"})

				// Then: Job should be deleted
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.GetJob(ctx, "job-unknown-stopped-1")
				Expect(err).To(HaveOccurred())
			})
		})

		Context("with multiple jobs in different final states", func() {
			It("should successfully delete all jobs", func() {
				// Given: Multiple jobs in different final states
				// Create completed job
				completedJob := &jobpool.Job{
					ID:            "job-completed-multi",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, completedJob)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.CompleteJob(ctx, "job-completed-multi", []byte("result"))
				Expect(err).NotTo(HaveOccurred())

				// Create unscheduled job
				unscheduledJob := &jobpool.Job{
					ID:            "job-unscheduled-multi",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err = queue.EnqueueJob(ctx, unscheduledJob)
				Expect(err).NotTo(HaveOccurred())
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-unscheduled-multi"})
				Expect(err).NotTo(HaveOccurred())

				// Create stopped job
				stoppedJob := &jobpool.Job{
					ID:            "job-stopped-multi",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err = queue.EnqueueJob(ctx, stoppedJob)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-2", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.StopJob(ctx, "job-stopped-multi", "stopped")
				Expect(err).NotTo(HaveOccurred())

				// When: DeleteJobs is called with all job IDs
				err = queue.DeleteJobs(ctx, nil, []string{"job-completed-multi", "job-unscheduled-multi", "job-stopped-multi"})

				// Then: All jobs should be deleted
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.GetJob(ctx, "job-completed-multi")
				Expect(err).To(HaveOccurred())
				_, err = queue.GetJob(ctx, "job-unscheduled-multi")
				Expect(err).To(HaveOccurred())
				_, err = queue.GetJob(ctx, "job-stopped-multi")
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Describe("when deleting jobs not in final states", func() {
		Context("with PENDING jobs", func() {
			It("should return an error", func() {
				// Given: A pending job
				job := &jobpool.Job{
					ID:            "job-pending-1",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())

				// When: DeleteJobs is called
				err = queue.DeleteJobs(ctx, nil, []string{"job-pending-1"})

				// Then: Should return error
				Expect(err).To(HaveOccurred())
				Expect(strings.Contains(err.Error(), "not in final states")).To(BeTrue())
				Expect(strings.Contains(err.Error(), "job-pending-1")).To(BeTrue())

				// Job should still exist
				_, err = queue.GetJob(ctx, "job-pending-1")
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Context("with RUNNING jobs", func() {
			It("should return an error", func() {
				// Given: A running job
				job := &jobpool.Job{
					ID:            "job-running-1",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())

				// When: DeleteJobs is called
				err = queue.DeleteJobs(ctx, nil, []string{"job-running-1"})

				// Then: Should return error
				Expect(err).To(HaveOccurred())
				Expect(strings.Contains(err.Error(), "not in final states")).To(BeTrue())

				// Job should still exist
				runningJob, err := queue.GetJob(ctx, "job-running-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(runningJob.Status).To(Equal(jobpool.JobStatusRunning))
			})
		})

		Context("with FAILED jobs", func() {
			It("should return an error", func() {
				// Given: A failed job (which transitions to PENDING)
				job := &jobpool.Job{
					ID:            "job-failed-1",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.FailJob(ctx, "job-failed-1", "error")
				Expect(err).NotTo(HaveOccurred())

				// Job is now in PENDING state after FailJob
				// When: DeleteJobs is called
				err = queue.DeleteJobs(ctx, nil, []string{"job-failed-1"})

				// Then: Should return error (PENDING is not a final state)
				Expect(err).To(HaveOccurred())
				Expect(strings.Contains(err.Error(), "not in final states")).To(BeTrue())
			})
		})

		Context("with CANCELLING jobs", func() {
			It("should return an error", func() {
				// Given: A cancelling job
				job := &jobpool.Job{
					ID:            "job-cancelling-1",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				_, _, err = queue.CancelJobs(ctx, nil, []string{"job-cancelling-1"})
				Expect(err).NotTo(HaveOccurred())

				// Verify job is in CANCELLING state
				cancellingJob, err := queue.GetJob(ctx, "job-cancelling-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(cancellingJob.Status).To(Equal(jobpool.JobStatusCancelling))

				// When: DeleteJobs is called
				err = queue.DeleteJobs(ctx, nil, []string{"job-cancelling-1"})

				// Then: Should return error
				Expect(err).To(HaveOccurred())
				Expect(strings.Contains(err.Error(), "not in final states")).To(BeTrue())
			})
		})

		Context("with UNKNOWN_RETRY jobs", func() {
			It("should return an error", func() {
				// Given: An unknown_retry job
				job := &jobpool.Job{
					ID:            "job-unknown-retry-1",
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

				// Verify job is in UNKNOWN_RETRY state
				unknownJob, err := queue.GetJob(ctx, "job-unknown-retry-1")
				Expect(err).NotTo(HaveOccurred())
				Expect(unknownJob.Status).To(Equal(jobpool.JobStatusUnknownRetry))

				// When: DeleteJobs is called
				err = queue.DeleteJobs(ctx, nil, []string{"job-unknown-retry-1"})

				// Then: Should return error
				Expect(err).To(HaveOccurred())
				Expect(strings.Contains(err.Error(), "not in final states")).To(BeTrue())
			})
		})

		Context("with mixed final and non-final states", func() {
			It("should return an error and not delete any jobs", func() {
				// Given: A completed job and a pending job
				completedJob := &jobpool.Job{
					ID:            "job-completed-mixed",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, completedJob)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.CompleteJob(ctx, "job-completed-mixed", []byte("result"))
				Expect(err).NotTo(HaveOccurred())

				pendingJob := &jobpool.Job{
					ID:            "job-pending-mixed",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err = queue.EnqueueJob(ctx, pendingJob)
				Expect(err).NotTo(HaveOccurred())

				// When: DeleteJobs is called with both job IDs
				err = queue.DeleteJobs(ctx, nil, []string{"job-completed-mixed", "job-pending-mixed"})

				// Then: Should return error
				Expect(err).To(HaveOccurred())
				Expect(strings.Contains(err.Error(), "not in final states")).To(BeTrue())
				Expect(strings.Contains(err.Error(), "job-pending-mixed")).To(BeTrue())

				// Neither job should be deleted (transaction rollback)
				_, err = queue.GetJob(ctx, "job-completed-mixed")
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.GetJob(ctx, "job-pending-mixed")
				Expect(err).NotTo(HaveOccurred())
			})
		})
	})

	Describe("when deleting jobs by tags", func() {
		Context("with jobs matching tags in final states", func() {
			It("should successfully delete all matching jobs", func() {
				// Given: Multiple jobs with same tag in final states
				for i := 0; i < 3; i++ {
					jobID := fmt.Sprintf("job-tag-%d", i)
					job := &jobpool.Job{
						ID:            jobID,
						Status:        jobpool.JobStatusPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						Tags:          []string{"project-1"},
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
					_, err = backend.DequeueJobs(ctx, fmt.Sprintf("worker-%d", i), []string{"project-1"}, 1)
					Expect(err).NotTo(HaveOccurred())
					err = queue.CompleteJob(ctx, jobID, []byte("result"))
					Expect(err).NotTo(HaveOccurred())
				}

				// When: DeleteJobs is called with tags
				err := queue.DeleteJobs(ctx, []string{"project-1"}, nil)

				// Then: All matching jobs should be deleted
				Expect(err).NotTo(HaveOccurred())
				for i := 0; i < 3; i++ {
					jobID := fmt.Sprintf("job-tag-%d", i)
					_, err := queue.GetJob(ctx, jobID)
					Expect(err).To(HaveOccurred())
				}
			})
		})

		Context("with jobs matching multiple tags (AND logic)", func() {
			It("should only delete jobs matching all tags", func() {
				// Given: Jobs with different tag combinations
				job1 := &jobpool.Job{
					ID:            "job-tag-both",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"project-1", "service-1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job1)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", []string{"project-1", "service-1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.CompleteJob(ctx, "job-tag-both", []byte("result"))
				Expect(err).NotTo(HaveOccurred())

				job2 := &jobpool.Job{
					ID:            "job-tag-project-only",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"project-1"},
					CreatedAt:     time.Now(),
				}
				_, err = queue.EnqueueJob(ctx, job2)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-2", []string{"project-1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.CompleteJob(ctx, "job-tag-project-only", []byte("result"))
				Expect(err).NotTo(HaveOccurred())

				// When: DeleteJobs is called with both tags
				err = queue.DeleteJobs(ctx, []string{"project-1", "service-1"}, nil)

				// Then: Only job with both tags should be deleted
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.GetJob(ctx, "job-tag-both")
				Expect(err).To(HaveOccurred())
				_, err = queue.GetJob(ctx, "job-tag-project-only")
				Expect(err).NotTo(HaveOccurred()) // Should still exist
			})
		})
	})

	Describe("when deleting jobs by job IDs", func() {
		Context("with valid job IDs in final states", func() {
			It("should successfully delete specified jobs", func() {
				// Given: Multiple completed jobs
				jobIDs := []string{"job-id-1", "job-id-2", "job-id-3"}
				for _, jobID := range jobIDs {
					job := &jobpool.Job{
						ID:            jobID,
						Status:        jobpool.JobStatusPending,
						JobType:       "test",
						JobDefinition: []byte("test"),
						CreatedAt:     time.Now(),
					}
					_, err := queue.EnqueueJob(ctx, job)
					Expect(err).NotTo(HaveOccurred())
					_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
					Expect(err).NotTo(HaveOccurred())
					err = queue.CompleteJob(ctx, jobID, []byte("result"))
					Expect(err).NotTo(HaveOccurred())
				}

				// When: DeleteJobs is called with job IDs
				err := queue.DeleteJobs(ctx, nil, jobIDs)

				// Then: All specified jobs should be deleted
				Expect(err).NotTo(HaveOccurred())
				for _, jobID := range jobIDs {
					_, err := queue.GetJob(ctx, jobID)
					Expect(err).To(HaveOccurred())
				}
			})
		})

		Context("with non-existent job IDs", func() {
			It("should succeed (non-existent jobs are skipped)", func() {
				// Given: A completed job
				job := &jobpool.Job{
					ID:            "job-exists",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.CompleteJob(ctx, "job-exists", []byte("result"))
				Expect(err).NotTo(HaveOccurred())

				// When: DeleteJobs is called with existing and non-existent job IDs
				err = queue.DeleteJobs(ctx, nil, []string{"job-exists", "job-not-exists"})

				// Then: Should succeed (non-existent jobs are skipped)
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.GetJob(ctx, "job-exists")
				Expect(err).To(HaveOccurred()) // Should be deleted
			})
		})
	})

	Describe("when deleting jobs by both tags and job IDs", func() {
		Context("with union of tags and job IDs", func() {
			It("should delete jobs matching either tags or job IDs", func() {
				// Given: Jobs with tags and specific job IDs
				// Job with tag
				taggedJob := &jobpool.Job{
					ID:            "job-tagged",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"project-1"},
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, taggedJob)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", []string{"project-1"}, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.CompleteJob(ctx, "job-tagged", []byte("result"))
				Expect(err).NotTo(HaveOccurred())

				// Job with specific ID (no matching tag)
				directJob := &jobpool.Job{
					ID:            "job-direct",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"project-2"},
					CreatedAt:     time.Now(),
				}
				_, err = queue.EnqueueJob(ctx, directJob)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-2", []string{"project-2"}, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.CompleteJob(ctx, "job-direct", []byte("result"))
				Expect(err).NotTo(HaveOccurred())

				// When: DeleteJobs is called with both tags and job IDs
				err = queue.DeleteJobs(ctx, []string{"project-1"}, []string{"job-direct"})

				// Then: Both jobs should be deleted
				Expect(err).NotTo(HaveOccurred())
				_, err = queue.GetJob(ctx, "job-tagged")
				Expect(err).To(HaveOccurred())
				_, err = queue.GetJob(ctx, "job-direct")
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Describe("edge cases", func() {
		Context("with empty tags and empty job IDs", func() {
			It("should succeed without error", func() {
				// When: DeleteJobs is called with empty parameters
				err := queue.DeleteJobs(ctx, nil, nil)

				// Then: Should succeed (no-op)
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Context("with empty tags and empty job IDs slice", func() {
			It("should succeed without error", func() {
				// When: DeleteJobs is called with empty slices
				err := queue.DeleteJobs(ctx, []string{}, []string{})

				// Then: Should succeed (no-op)
				Expect(err).NotTo(HaveOccurred())
			})
		})
	})
})
