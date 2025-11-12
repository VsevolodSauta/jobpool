module github.com/VsevolodSauta/jobpool

go 1.23.0

require (
	github.com/dgraph-io/badger/v4 v4.2.0
	// github.com/mattn/go-sqlite3 is optional - only needed when building with -tags sqlite
	// To build without CGO, exclude the sqlite build tag
	github.com/mattn/go-sqlite3 v1.14.18 // optional: requires CGO and -tags sqlite
)

require (
	github.com/onsi/ginkgo/v2 v2.27.2
	github.com/onsi/gomega v1.38.2
)

require (
	github.com/Masterminds/semver/v3 v3.4.0 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgraph-io/ristretto v0.1.1 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-task/slim-sprig/v3 v3.0.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/google/flatbuffers v1.12.1 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/pprof v0.0.0-20250403155104-27863c87afa6 // indirect
	github.com/klauspost/compress v1.12.3 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	go.opencensus.io v0.22.5 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/mod v0.27.0 // indirect
	golang.org/x/net v0.43.0 // indirect
	golang.org/x/sync v0.16.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
	golang.org/x/text v0.28.0 // indirect
	golang.org/x/tools v0.36.0 // indirect
	google.golang.org/protobuf v1.36.7 // indirect
)
