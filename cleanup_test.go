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

func TestCleanupExpiredJobs(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Cleanup Expired Jobs Suite")
}

var _ = Describe("CleanupExpiredJobs", func() {
	var (
		backend jobpool.Backend
		queue   jobpool.Queue
		ctx     context.Context
		tmpFile *os.File
	)

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		tmpFile, err = os.CreateTemp("", "test_jobpool_cleanup_*.db")
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

	Describe("when cleaning up expired completed jobs", func() {
		Context("with completed jobs older than TTL", func() {
			It("should delete expired completed jobs", func() {
				// Given: Multiple completed jobs with different completion times
				sqliteBackend, ok := backend.(*jobpool.SQLiteBackend)
				Expect(ok).To(BeTrue(), "Backend should be SQLiteBackend for this test")

				// Create old completed job
				oldJob := &jobpool.Job{
					ID:            "job-old-completed",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, oldJob)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.CompleteJob(ctx, "job-old-completed", []byte("result"))
				Expect(err).NotTo(HaveOccurred())

				// Set completed_at to 2 hours ago using test helper
				oldTime := time.Now().Add(-2 * time.Hour)
				err = sqliteBackend.SetJobCompletedAtForTesting(ctx, "job-old-completed", oldTime)
				Expect(err).NotTo(HaveOccurred())

				// Create recent completed job (completed now, so within TTL)
				recentJob := &jobpool.Job{
					ID:            "job-recent-completed",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err = queue.EnqueueJob(ctx, recentJob)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.CompleteJob(ctx, "job-recent-completed", []byte("result"))
				Expect(err).NotTo(HaveOccurred())

				// When: CleanupExpiredJobs is called with 1 hour TTL
				ttl := 1 * time.Hour
				err = queue.CleanupExpiredJobs(ctx, ttl)
				Expect(err).NotTo(HaveOccurred())

				// Then: Old job should be deleted, recent job should remain
				_, err = queue.GetJob(ctx, "job-old-completed")
				Expect(err).To(HaveOccurred()) // Job should not exist

				recentJobAfter, err := queue.GetJob(ctx, "job-recent-completed")
				Expect(err).NotTo(HaveOccurred())
				Expect(recentJobAfter.Status).To(Equal(jobpool.JobStatusCompleted))
			})
		})

		Context("with non-completed jobs", func() {
			It("should not delete pending, running, or failed jobs", func() {
				// Given: Jobs in various non-completed states
				now := time.Now()
				oldTime := now.Add(-2 * time.Hour)

				// Create old pending job
				pendingJob := &jobpool.Job{
					ID:            "job-old-pending",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     oldTime,
				}
				_, err := queue.EnqueueJob(ctx, pendingJob)
				Expect(err).NotTo(HaveOccurred())

				// Create old running job (use tags to ensure correct dequeue)
				runningJob := &jobpool.Job{
					ID:            "job-old-running",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"running"},
					CreatedAt:     oldTime,
				}
				_, err = queue.EnqueueJob(ctx, runningJob)
				Expect(err).NotTo(HaveOccurred())
				dequeued, err := backend.DequeueJobs(ctx, "worker-1", []string{"running"}, 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(dequeued)).To(Equal(1))
				Expect(dequeued[0].ID).To(Equal("job-old-running"))

				// Create old stopped job (need to be running first, use tags)
				stoppedJob := &jobpool.Job{
					ID:            "job-old-stopped",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					Tags:          []string{"stopped"},
					CreatedAt:     oldTime,
				}
				_, err = queue.EnqueueJob(ctx, stoppedJob)
				Expect(err).NotTo(HaveOccurred())
				dequeued, err = backend.DequeueJobs(ctx, "worker-2", []string{"stopped"}, 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(dequeued)).To(Equal(1))
				Expect(dequeued[0].ID).To(Equal("job-old-stopped"))
				// Now stop it (job is now RUNNING)
				err = queue.StopJob(ctx, "job-old-stopped", "stopped")
				Expect(err).NotTo(HaveOccurred())

				// When: CleanupExpiredJobs is called
				ttl := 1 * time.Hour
				err = queue.CleanupExpiredJobs(ctx, ttl)
				Expect(err).NotTo(HaveOccurred())

				// Then: All non-completed jobs should remain
				pendingJobAfter, err := queue.GetJob(ctx, "job-old-pending")
				Expect(err).NotTo(HaveOccurred())
				Expect(pendingJobAfter.Status).To(Equal(jobpool.JobStatusPending))

				runningJobAfter, err := queue.GetJob(ctx, "job-old-running")
				Expect(err).NotTo(HaveOccurred())
				Expect(runningJobAfter.Status).To(Equal(jobpool.JobStatusRunning))

				stoppedJobAfter, err := queue.GetJob(ctx, "job-old-stopped")
				Expect(err).NotTo(HaveOccurred())
				Expect(stoppedJobAfter.Status).To(Equal(jobpool.JobStatusStopped))
			})
		})

		Context("with recently completed jobs", func() {
			It("should not delete jobs completed within TTL", func() {
				// Given: A recently completed job
				job := &jobpool.Job{
					ID:            "job-recent",
					Status:        jobpool.JobStatusPending,
					JobType:       "test",
					JobDefinition: []byte("test"),
					CreatedAt:     time.Now(),
				}
				_, err := queue.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
				_, err = backend.DequeueJobs(ctx, "worker-1", nil, 1)
				Expect(err).NotTo(HaveOccurred())
				err = queue.CompleteJob(ctx, "job-recent", []byte("result"))
				Expect(err).NotTo(HaveOccurred())

				// When: CleanupExpiredJobs is called with long TTL
				ttl := 24 * time.Hour
				err = queue.CleanupExpiredJobs(ctx, ttl)
				Expect(err).NotTo(HaveOccurred())

				// Then: Job should still exist
				jobAfter, err := queue.GetJob(ctx, "job-recent")
				Expect(err).NotTo(HaveOccurred())
				Expect(jobAfter.Status).To(Equal(jobpool.JobStatusCompleted))
			})
		})

		Context("with empty database", func() {
			It("should not return an error", func() {
				// When: CleanupExpiredJobs is called on empty database
				ttl := 1 * time.Hour
				err := queue.CleanupExpiredJobs(ctx, ttl)

				// Then: Should succeed without error
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Context("with all jobs expired", func() {
			It("should delete all expired completed jobs", func() {
				// Given: Multiple old completed jobs
				sqliteBackend, ok := backend.(*jobpool.SQLiteBackend)
				Expect(ok).To(BeTrue(), "Backend should be SQLiteBackend for this test")

				for i := 0; i < 5; i++ {
					jobID := fmt.Sprintf("job-expired-%d", i)
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

					// Set completed_at to 2 hours ago
					oldTime := time.Now().Add(-2 * time.Hour)
					err = sqliteBackend.SetJobCompletedAtForTesting(ctx, jobID, oldTime)
					Expect(err).NotTo(HaveOccurred())
				}

				// When: CleanupExpiredJobs is called with 1 hour TTL
				ttl := 1 * time.Hour
				err := queue.CleanupExpiredJobs(ctx, ttl)
				Expect(err).NotTo(HaveOccurred())

				// Then: All expired jobs should be deleted
				for i := 0; i < 5; i++ {
					jobID := fmt.Sprintf("job-expired-%d", i)
					_, err := queue.GetJob(ctx, jobID)
					Expect(err).To(HaveOccurred()) // Jobs should not exist
				}
			})
		})

		Context("with no expired jobs", func() {
			It("should not delete any jobs", func() {
				// Given: Recent completed jobs
				for i := 0; i < 3; i++ {
					jobID := fmt.Sprintf("job-recent-%d", i)
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

				// When: CleanupExpiredJobs is called with very long TTL
				ttl := 365 * 24 * time.Hour // 1 year
				err := queue.CleanupExpiredJobs(ctx, ttl)
				Expect(err).NotTo(HaveOccurred())

				// Then: All jobs should still exist
				for i := 0; i < 3; i++ {
					jobID := fmt.Sprintf("job-recent-%d", i)
					job, err := queue.GetJob(ctx, jobID)
					Expect(err).NotTo(HaveOccurred())
					Expect(job.Status).To(Equal(jobpool.JobStatusCompleted))
				}
			})
		})
	})
})
