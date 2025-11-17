//go:build sqlite
// +build sqlite

package jobpool_test

import (
	"os"

	"github.com/VsevolodSauta/jobpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("SQLiteBackend", func() {
	BackendTestSuite(func() (jobpool.Backend, func()) {
		tmpFile, err := os.CreateTemp("", "test_jobpool_*.db")
		Expect(err).NotTo(HaveOccurred())
		tmpFile.Close()

		backend, err := jobpool.NewSQLiteBackend(tmpFile.Name(), testLogger())
		Expect(err).NotTo(HaveOccurred())

		return backend, func() {
			_ = backend.Close()
			_ = os.Remove(tmpFile.Name())
		}
	})
})
