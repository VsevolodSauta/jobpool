package jobpool_test

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/VsevolodSauta/jobpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("BadgerBackend", func() {
	BackendTestSuite(func() (jobpool.Backend, func()) {
		tmpDir, err := os.MkdirTemp("", "jobpool_badger_*")
		Expect(err).NotTo(HaveOccurred())

		backend, err := jobpool.NewBadgerBackend(tmpDir, testLogger())
		Expect(err).NotTo(HaveOccurred())

		return backend, func() {
			_ = backend.Close()
			_ = os.RemoveAll(tmpDir)
		}
	})

	Describe("pending job indexing", func() {
		It("should return jobs on the first dequeue even when 100+ pending jobs exist with complex tags", func() {
			tmpDir, err := os.MkdirTemp("", "jobpool_badger_pending_*")
			Expect(err).NotTo(HaveOccurred())

			backend, err := jobpool.NewBadgerBackend(tmpDir, testLogger())
			Expect(err).NotTo(HaveOccurred())
			defer func() {
				_ = backend.Close()
				_ = os.RemoveAll(tmpDir)
			}()

			ctx := context.Background()
			totalJobs := 136
			baseTags := []string{
				"0f56bb14-12b7-4595-ab0b-5eb041de4ccf",
				"zyphos_indexer_service",
				"project-alpha",
			}
			for i := 0; i < totalJobs; i++ {
				job := &jobpool.Job{
					ID:            fmt.Sprintf("job-%03d", i),
					Status:        jobpool.JobStatusInitialPending,
					JobType:       "test",
					JobDefinition: []byte("payload"),
					Tags: append(
						[]string{
							fmt.Sprintf("doc-%03d", i),
							fmt.Sprintf("entity-%02d", i%10),
						},
						baseTags...,
					),
					CreatedAt: time.Now().Add(time.Duration(i) * time.Millisecond),
				}

				_, err := backend.EnqueueJob(ctx, job)
				Expect(err).NotTo(HaveOccurred())
			}

			// When we dequeue with limit 8 we should immediately receive 8 jobs.
			jobs, err := backend.DequeueJobs(ctx, "worker-1", baseTags, 8)
			Expect(err).NotTo(HaveOccurred())
			Expect(jobs).To(HaveLen(8))
		})
	})
})
