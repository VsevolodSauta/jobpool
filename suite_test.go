//go:build sqlite
// +build sqlite

package jobpool_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestJobPool(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "JobPool Suite")
}
