# Contributing to JobPool

Thank you for your interest in contributing to JobPool!

## Development Setup

1. Fork the repository
2. Clone your fork: `git clone git@github.com:YOUR_USERNAME/jobpool.git`
3. Create a branch: `git checkout -b feature/your-feature`
4. Make your changes
5. Run tests: `go test ./...`
6. Ensure code is formatted: `go fmt ./...`
7. Submit a pull request

## Code Style

- Follow standard Go formatting (`go fmt`)
- Write tests for new features
- Update documentation for API changes
- Add examples for new functionality

## Testing

- All tests must pass: `go test ./...`
- Add tests for new features
- Maintain or improve test coverage

### Running Tests

```bash
# All tests (with SQLite if CGO enabled)
make test

# Tests without CGO (BadgerDB only)
make test-no-cgo

# Tests with race detector
make test-race

# Tests with coverage
make test-coverage

# Benchmarks
make bench
```

### Test Coverage

The library maintains 60%+ test coverage. To view coverage:

```bash
make test-coverage
# Opens coverage.html in browser
```

## Documentation

- Add godoc comments for exported types and functions

## Building Examples

Examples can be built using Makefile targets:

```bash
# Build all examples
make examples

# Build individual examples
make example-basic        # Requires CGO and sqlite tag
make example-basic-badger # No CGO required
make example-worker       # Requires CGO and sqlite tag
```

Examples are built into the `tmp/` directory to simplify gitignore management.

## Version Management

### Creating a New Version

1. Update `CHANGELOG.md` with new version section
2. Update version in documentation if needed
3. Commit changes
4. Create and push tag:

```bash
git tag v1.0.1
git push origin v1.0.1
```

### Semantic Versioning

- **MAJOR** (1.0.0): Breaking API changes
- **MINOR** (1.1.0): New features, backwards compatible
- **PATCH** (1.0.1): Bug fixes, backwards compatible

**Important**: Git tags are required for Go modules. Without them, users cannot fetch a specific version using `go get github.com/VsevolodSauta/jobpool@v1.0.0`.

## Pull Request Process

1. Update CHANGELOG.md with your changes
2. Ensure all tests pass
3. Update documentation as needed
4. Submit PR with clear description of changes

## Troubleshooting

### Import Errors
- Ensure `go.mod` module path matches import path
- Run `go mod tidy` in library directory
- Verify `go.sum` is up to date

### Test Failures
- Clean test databases: `make clean`
- Check for race conditions: `make test-race`
- Verify all dependencies are installed: `go mod download`

