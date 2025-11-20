//go:build sqlite
// +build sqlite

package jobpool

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
)

// SQLiteBackend implements the Backend interface using SQLite.
// It provides ACID transactions and is suitable for single-server deployments.
type SQLiteBackend struct {
	db     *sql.DB
	logger *slog.Logger
}

// NewSQLiteBackend creates a new SQLite backend.
// The database file will be created if it doesn't exist.
// dbPath is the path to the SQLite database file.
// logger is used for structured logging (currently unused but kept for consistency with other backends).
func NewSQLiteBackend(dbPath string, logger *slog.Logger) (*SQLiteBackend, error) {
	dsn := fmt.Sprintf("%s?_foreign_keys=on&_busy_timeout=5000", dbPath)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	backend := &SQLiteBackend{
		db:     db,
		logger: logger,
	}

	// Initialize schema
	if err := backend.initSchema(); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return backend, nil
}

// Close closes the database connection
func (b *SQLiteBackend) Close() error {
	return b.db.Close()
}

// initSchema initializes the database schema
func (b *SQLiteBackend) initSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS jobs (
		id TEXT PRIMARY KEY,
		status TEXT NOT NULL,
		job_type TEXT NOT NULL,
		job_definition BLOB NOT NULL,
		created_at INTEGER NOT NULL,
		started_at INTEGER,
		finalized_at INTEGER,
		error_message TEXT,
		result BLOB,
		retry_count INTEGER DEFAULT 0,
		last_retry_at INTEGER,
		assignee_id TEXT,
		assigned_at INTEGER
	);

	CREATE TABLE IF NOT EXISTS job_tags (
		job_id TEXT NOT NULL,
		tag TEXT NOT NULL,
		PRIMARY KEY (job_id, tag),
		FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
	);

	CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
	CREATE INDEX IF NOT EXISTS idx_job_tags_job_id ON job_tags(job_id);
	CREATE INDEX IF NOT EXISTS idx_job_tags_tag ON job_tags(tag);
	CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at);
	CREATE INDEX IF NOT EXISTS idx_jobs_assignee_id ON jobs(assignee_id);
	`

	_, err := b.db.Exec(schema)
	return err
}

// EnqueueJob enqueues a single job
func (b *SQLiteBackend) EnqueueJob(ctx context.Context, job *Job) (string, error) {
	if _, err := normalizeContext(ctx); err != nil {
		return "", err
	}
	if job == nil {
		return "", fmt.Errorf("job is nil")
	}
	b.logger.Debug("EnqueueJob: enqueuing job", "jobID", job.ID, "tags", job.Tags, "status", job.Status)
	if job.ID == "" {
		return "", fmt.Errorf("job ID is required")
	}
	if job.Status != JobStatusInitialPending {
		return "", fmt.Errorf("job %s must have status %s", job.ID, JobStatusInitialPending)
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = time.Now()
	}
	job.AssigneeID = ""
	job.AssignedAt = nil
	job.StartedAt = nil
	job.FinalizedAt = nil
	job.ErrorMessage = ""
	job.Result = nil
	job.RetryCount = 0
	job.LastRetryAt = nil

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
		INSERT INTO jobs (id, status, job_type, job_definition, created_at, retry_count, assignee_id, assigned_at, started_at, finalized_at, error_message, result, last_retry_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, job.ID, job.Status, job.JobType, job.JobDefinition, job.CreatedAt.UnixNano(), job.RetryCount, job.AssigneeID, nil, nil, nil, nil, nil, nil)
	if err != nil {
		if isConstraintError(err) {
			return "", fmt.Errorf("job already exists: %s", job.ID)
		}
		return "", fmt.Errorf("failed to insert job: %w", err)
	}

	// Insert tags
	for _, tag := range job.Tags {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO job_tags (job_id, tag)
			VALUES (?, ?)
		`, job.ID, tag)
		if err != nil {
			return "", fmt.Errorf("failed to insert tag: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("failed to commit transaction: %w", err)
	}

	return job.ID, nil
}

// EnqueueJobs enqueues multiple jobs in a batch
func (b *SQLiteBackend) EnqueueJobs(ctx context.Context, jobs []*Job) ([]string, error) {
	if _, err := normalizeContext(ctx); err != nil {
		return nil, err
	}
	if len(jobs) == 0 {
		return []string{}, nil
	}

	seen := make(map[string]struct{}, len(jobs))
	now := time.Now()
	for idx, job := range jobs {
		if job == nil {
			return nil, fmt.Errorf("job at index %d is nil", idx)
		}
		if job.ID == "" {
			return nil, fmt.Errorf("job at index %d is missing ID", idx)
		}
		if job.Status != JobStatusInitialPending {
			return nil, fmt.Errorf("job %s must have status %s", job.ID, JobStatusInitialPending)
		}
		if _, exists := seen[job.ID]; exists {
			return nil, fmt.Errorf("duplicate job ID %s in batch", job.ID)
		}
		seen[job.ID] = struct{}{}
		if job.CreatedAt.IsZero() {
			job.CreatedAt = now
		}
		job.AssigneeID = ""
		job.AssignedAt = nil
		job.StartedAt = nil
		job.FinalizedAt = nil
		job.ErrorMessage = ""
		job.Result = nil
		job.RetryCount = 0
		job.LastRetryAt = nil
	}

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	jobIDs := make([]string, 0, len(jobs))

	// Insert jobs (fails if job already exists)
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO jobs (id, status, job_type, job_definition, created_at, retry_count, assignee_id, assigned_at, started_at, finalized_at, error_message, result, last_retry_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, job := range jobs {
		_, err = stmt.ExecContext(ctx, job.ID, job.Status, job.JobType, job.JobDefinition, job.CreatedAt.UnixNano(), job.RetryCount, job.AssigneeID, nil, nil, nil, nil, nil, nil)
		if err != nil {
			if isConstraintError(err) {
				return nil, fmt.Errorf("job already exists: %s", job.ID)
			}
			return nil, fmt.Errorf("failed to insert job: %w", err)
		}
		jobIDs = append(jobIDs, job.ID)
	}

	// Insert tags
	tagStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO job_tags (job_id, tag)
		VALUES (?, ?)
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare tag statement: %w", err)
	}
	defer tagStmt.Close()

	for _, job := range jobs {
		for _, tag := range job.Tags {
			_, err = tagStmt.ExecContext(ctx, job.ID, tag)
			if err != nil {
				return nil, fmt.Errorf("failed to insert tag: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return jobIDs, nil
}

// DequeueJobs dequeues pending jobs up to the limit and assigns them to the given assigneeID
// tags: Filter jobs by tags using AND logic (jobs must have ALL provided tags). Empty slice means no filtering.
func (b *SQLiteBackend) DequeueJobs(ctx context.Context, assigneeID string, tags []string, limit int) ([]*Job, error) {
	if _, err := normalizeContext(ctx); err != nil {
		return nil, err
	}
	if assigneeID == "" {
		return nil, fmt.Errorf("assigneeID is required")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be greater than 0")
	}

	const maxAttempts = 15
	initialBackoff := 10 * time.Millisecond
	maxBackoff := 100 * time.Millisecond
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		jobs, err := b.dequeueJobsOnce(ctx, assigneeID, tags, limit)
		if err == nil {
			return jobs, nil
		}
		if !isBusyError(err) {
			return nil, err
		}
		lastErr = err

		// Exponential backoff with jitter: 10ms, 20ms, 40ms, 80ms, 100ms (capped)
		backoff := initialBackoff * time.Duration(1<<uint(attempt))
		if backoff > maxBackoff {
			backoff = maxBackoff
		}

		if err := waitWithContext(ctx, backoff); err != nil {
			return nil, err
		}
	}

	if lastErr != nil {
		return nil, fmt.Errorf("failed to assign jobs after retries: %w", lastErr)
	}
	return nil, fmt.Errorf("failed to assign jobs after retries")
}

func (b *SQLiteBackend) dequeueJobsOnce(ctx context.Context, assigneeID string, tags []string, limit int) ([]*Job, error) {
	b.logger.Debug("dequeueJobsOnce: starting", "assigneeID", assigneeID, "tags", tags, "limit", limit)
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UnixNano()

	query, args := buildDequeueQuery(tags, limit)
	b.logger.Debug("dequeueJobsOnce: built query", "query", query, "args", args, "tags", tags, "limit", limit)

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query jobs: %w", err)
	}
	defer rows.Close()

	jobs := make([]*Job, 0, limit)
	jobIDs := make([]string, 0, limit)

	for rows.Next() {
		job := &Job{}
		var createdAt, startedAt, completedAt, lastRetryAt, assignedAt sql.NullInt64
		var assignee, errorMessage sql.NullString
		var result sql.NullString

		if err := rows.Scan(
			&job.ID, &job.Status, &job.JobType, &job.JobDefinition,
			&createdAt, &startedAt, &completedAt, &errorMessage,
			&result, &job.RetryCount, &lastRetryAt, &assignee, &assignedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan job: %w", err)
		}

		job.CreatedAt = time.Unix(0, createdAt.Int64)
		if startedAt.Valid {
			t := time.Unix(0, startedAt.Int64)
			job.StartedAt = &t
		}
		if completedAt.Valid {
			t := time.Unix(0, completedAt.Int64)
			job.FinalizedAt = &t
		}
		if lastRetryAt.Valid {
			t := time.Unix(0, lastRetryAt.Int64)
			job.LastRetryAt = &t
		}
		if assignedAt.Valid {
			t := time.Unix(0, assignedAt.Int64)
			job.AssignedAt = &t
		}
		if assignee.Valid {
			job.AssigneeID = assignee.String
		}
		if errorMessage.Valid {
			job.ErrorMessage = errorMessage.String
		}
		if result.Valid {
			job.Result = []byte(result.String)
		}

		jobs = append(jobs, job)
		jobIDs = append(jobIDs, job.ID)
	}

	if len(jobIDs) == 0 {
		b.logger.Debug("dequeueJobsOnce: no jobs found", "assigneeID", assigneeID, "tags", tags, "limit", limit)
		return []*Job{}, nil
	}

	b.logger.Debug("dequeueJobsOnce: found jobs", "assigneeID", assigneeID, "tags", tags, "jobCount", len(jobIDs), "jobIDs", jobIDs)

	updateArgs := make([]interface{}, 0, len(jobIDs)+4)
	updateArgs = append(updateArgs, JobStatusRunning, assigneeID, now, now)
	for _, id := range jobIDs {
		updateArgs = append(updateArgs, id)
	}

	updateQuery := fmt.Sprintf(`
		UPDATE jobs
		SET status = ?,
		    assignee_id = ?, assigned_at = ?,
		    started_at = COALESCE(started_at, ?)
		WHERE id IN (%s)
	`, placeholdersStr(len(jobIDs)))

	if _, err := tx.ExecContext(ctx, updateQuery, updateArgs...); err != nil {
		return nil, fmt.Errorf("failed to assign jobs: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	for _, job := range jobs {
		tagsForJob, err := b.getJobTags(ctx, job.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to get tags for job %s: %w", job.ID, err)
		}
		job.Tags = tagsForJob
		job.Status = JobStatusRunning
		job.AssigneeID = assigneeID
		assignedTime := time.Unix(0, now)
		job.AssignedAt = &assignedTime
		if job.StartedAt == nil {
			started := assignedTime
			job.StartedAt = &started
		}
		b.logger.Debug("dequeueJobsOnce: assigned job", "jobID", job.ID, "assigneeID", assigneeID, "jobTags", job.Tags, "requestedTags", tags)
	}

	b.logger.Debug("dequeueJobsOnce: completed", "assigneeID", assigneeID, "tags", tags, "assignedCount", len(jobs))
	return jobs, nil
}

func buildDequeueQuery(tags []string, limit int) (string, []interface{}) {
	if len(tags) > 0 {
		query := `
			SELECT j.id, j.status, j.job_type, j.job_definition,
			       j.created_at, j.started_at, j.finalized_at,
			       j.error_message, j.result, j.retry_count,
			       j.last_retry_at, j.assignee_id, j.assigned_at
			FROM jobs j
			INNER JOIN job_tags jt ON j.id = jt.job_id
			WHERE j.status IN (?, ?, ?)
			  AND jt.tag IN (` + placeholdersStr(len(tags)) + `)
			GROUP BY j.id, j.status, j.job_type, j.job_definition,
			         j.created_at, j.started_at, j.finalized_at,
			         j.error_message, j.result, j.retry_count,
			         j.last_retry_at, j.assignee_id, j.assigned_at
			HAVING COUNT(DISTINCT jt.tag) = ?
			ORDER BY COALESCE(j.last_retry_at, j.created_at) ASC
			LIMIT ?
		`
		args := make([]interface{}, 0, 3+len(tags)+2)
		args = append(args, JobStatusInitialPending, JobStatusFailedRetry, JobStatusUnknownRetry)
		for _, tag := range tags {
			args = append(args, tag)
		}
		args = append(args, len(tags), limit)
		return query, args
	}

	query := `
		SELECT j.id, j.status, j.job_type, j.job_definition,
		       j.created_at, j.started_at, j.finalized_at,
		       j.error_message, j.result, j.retry_count,
		       j.last_retry_at, j.assignee_id, j.assigned_at
		FROM jobs j
		WHERE j.status IN (?, ?, ?)
		ORDER BY COALESCE(j.last_retry_at, j.created_at) ASC
		LIMIT ?
	`
	args := []interface{}{JobStatusInitialPending, JobStatusFailedRetry, JobStatusUnknownRetry, limit}
	return query, args
}

func waitWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isConstraintError(err error) bool {
	var sqliteErr sqlite3.Error
	if errors.As(err, &sqliteErr) {
		return sqliteErr.Code == sqlite3.ErrConstraint
	}
	return false
}

func isBusyError(err error) bool {
	var sqliteErr sqlite3.Error
	if errors.As(err, &sqliteErr) {
		return sqliteErr.Code == sqlite3.ErrBusy || sqliteErr.Code == sqlite3.ErrLocked
	}
	return false
}

// getJobTags retrieves tags for a job
func (b *SQLiteBackend) getJobTags(ctx context.Context, jobID string) ([]string, error) {
	rows, err := b.db.QueryContext(ctx, `
		SELECT tag
		FROM job_tags
		WHERE job_id = ?
	`, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to query tags: %w", err)
	}
	defer rows.Close()

	tags := make([]string, 0)
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, fmt.Errorf("failed to scan tag: %w", err)
		}
		tags = append(tags, tag)
	}

	return tags, rows.Err()
}

// CompleteJob atomically transitions a job to COMPLETED with the given result.
// Supports RUNNING, CANCELLING, UNKNOWN_RETRY, and UNKNOWN_STOPPED → COMPLETED transitions.
func (b *SQLiteBackend) CompleteJob(ctx context.Context, jobID string, result []byte) (map[string]int, error) {
	if _, err := normalizeContext(ctx); err != nil {
		return nil, err
	}
	now := time.Now().UnixNano()

	job, err := b.GetJob(ctx, jobID)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, context.Canceled
		}
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	// Validate job is in a valid state that can transition to COMPLETED
	validStates := map[JobStatus]bool{
		JobStatusRunning:        true,
		JobStatusCancelling:     true,
		JobStatusUnknownRetry:   true,
		JobStatusUnknownStopped: true,
	}
	if !validStates[job.Status] {
		return nil, fmt.Errorf("job %s is not in a valid state for completion (current: %s)", jobID, job.Status)
	}

	// Get freed assignee ID before clearing (with count/multiplicity)
	freedAssigneeIDs := make(map[string]int)
	shouldFreeAssignee := job.Status == JobStatusRunning || job.Status == JobStatusCancelling
	if shouldFreeAssignee && job.AssigneeID != "" {
		freedAssigneeIDs[job.AssigneeID] = 1
	}

	// Update to COMPLETED in a transaction
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	var startedAt sql.NullInt64
	if job.StartedAt != nil {
		startedAt = sql.NullInt64{Int64: job.StartedAt.UnixNano(), Valid: true}
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE jobs
		SET status = ?,
		    result = ?,
		    finalized_at = ?,
		    started_at = COALESCE(?, started_at)
		WHERE id = ? AND status IN (?, ?, ?, ?)
	`, JobStatusCompleted, result, now, startedAt, jobID, JobStatusRunning, JobStatusCancelling, JobStatusUnknownRetry, JobStatusUnknownStopped)
	if err != nil {
		return nil, fmt.Errorf("failed to update job status: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return freedAssigneeIDs, nil
}

// FailJob atomically transitions a job to FAILED_RETRY, incrementing the retry count.
// Supports RUNNING and UNKNOWN_RETRY → FAILED_RETRY transitions.
// Job remains in FAILED_RETRY state (eligible for scheduling, but never returns to INITIAL_PENDING).
func (b *SQLiteBackend) FailJob(ctx context.Context, jobID string, errorMsg string) (map[string]int, error) {
	if _, err := normalizeContext(ctx); err != nil {
		return nil, err
	}
	if errorMsg == "" {
		return nil, fmt.Errorf("error message is required")
	}
	now := time.Now().UnixNano()

	// Get current job to validate it's in a valid state and get assignee ID
	job, err := b.GetJob(ctx, jobID)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, context.Canceled
		}
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	// Validate job is in a state that can transition to FAILED_RETRY
	if job.Status != JobStatusRunning && job.Status != JobStatusUnknownRetry {
		return nil, fmt.Errorf("job %s is not in RUNNING or UNKNOWN_RETRY state (current: %s)", jobID, job.Status)
	}

	// Get freed assignee ID before clearing (with count/multiplicity)
	freedAssigneeIDs := make(map[string]int)
	shouldFreeAssignee := job.Status == JobStatusRunning
	if shouldFreeAssignee && job.AssigneeID != "" {
		freedAssigneeIDs[job.AssigneeID] = 1
	}

	// Atomic transaction: transition to FAILED_RETRY with retry increment
	// Job stays in FAILED_RETRY state (eligible for scheduling, but never returns to INITIAL_PENDING)
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Update to FAILED_RETRY with error message, increment retry count, and clear assignee
	// Job remains in FAILED_RETRY state (eligible for scheduling like INITIAL_PENDING, but never transitions back to INITIAL_PENDING)
	_, err = tx.ExecContext(ctx, `
		UPDATE jobs
		SET status = ?,
		    error_message = ?,
		    retry_count = retry_count + 1,
		    last_retry_at = ?
		WHERE id = ? AND status IN (?, ?)
	`, JobStatusFailedRetry, errorMsg, now, jobID, JobStatusRunning, JobStatusUnknownRetry)
	if err != nil {
		return nil, fmt.Errorf("failed to update job to FAILED_RETRY: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return freedAssigneeIDs, nil
}

// StopJob atomically transitions a job to STOPPED with an error message.
// Supports RUNNING, CANCELLING, and UNKNOWN_RETRY → STOPPED transitions.
func (b *SQLiteBackend) StopJob(ctx context.Context, jobID string, errorMsg string) (map[string]int, error) {
	if _, err := normalizeContext(ctx); err != nil {
		return nil, err
	}
	now := time.Now().UnixNano()

	// Get current job to validate it's in a valid state and get assignee ID
	job, err := b.GetJob(ctx, jobID)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, context.Canceled
		}
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	// Validate job is in a state that can transition to STOPPED
	validStates := map[JobStatus]bool{
		JobStatusRunning:      true,
		JobStatusCancelling:   true,
		JobStatusUnknownRetry: true,
	}
	if !validStates[job.Status] {
		return nil, fmt.Errorf("job %s is not in a valid state for stopping (current: %s)", jobID, job.Status)
	}

	// Get freed assignee ID before clearing (with count/multiplicity)
	freedAssigneeIDs := make(map[string]int)
	shouldFreeAssignee := job.Status == JobStatusRunning || job.Status == JobStatusCancelling
	if shouldFreeAssignee && job.AssigneeID != "" {
		freedAssigneeIDs[job.AssigneeID] = 1
	}

	// Update to STOPPED in a transaction
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
		UPDATE jobs
		SET status = ?,
		    error_message = ?,
		    finalized_at = COALESCE(finalized_at, ?)
		WHERE id = ? AND status IN (?, ?, ?)
	`, JobStatusStopped, errorMsg, now, jobID, JobStatusRunning, JobStatusCancelling, JobStatusUnknownRetry)
	if err != nil {
		return nil, fmt.Errorf("failed to update job status: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return freedAssigneeIDs, nil
}

// StopJobWithRetry atomically transitions a job from CANCELLING to STOPPED with retry increment.
// Applies all effects from the transitory FAILED_RETRY state (retry increment + error message).
func (b *SQLiteBackend) StopJobWithRetry(ctx context.Context, jobID string, errorMsg string) (map[string]int, error) {
	if _, err := normalizeContext(ctx); err != nil {
		return nil, err
	}
	now := time.Now().UnixNano()

	// Get current job to validate it's in CANCELLING state and get assignee ID
	job, err := b.GetJob(ctx, jobID)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, context.Canceled
		}
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	if job.Status != JobStatusCancelling {
		return nil, fmt.Errorf("job %s is not in CANCELLING state (current: %s)", jobID, job.Status)
	}

	// Get freed assignee ID before clearing (with count/multiplicity)
	freedAssigneeIDs := make(map[string]int)
	if job.AssigneeID != "" {
		freedAssigneeIDs[job.AssigneeID] = 1
	}

	// Atomic transaction: CANCELLING → STOPPED with retry increment
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
		UPDATE jobs
		SET status = ?,
		    error_message = ?,
		    retry_count = retry_count + 1,
		    last_retry_at = ?,
		    finalized_at = COALESCE(finalized_at, ?)
		WHERE id = ? AND status = ?
	`, JobStatusStopped, errorMsg, now, now, jobID, JobStatusCancelling)
	if err != nil {
		return nil, fmt.Errorf("failed to update job status: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return freedAssigneeIDs, nil
}

// MarkJobUnknownStopped atomically transitions a job to UNKNOWN_STOPPED with an error message.
// Supports CANCELLING, UNKNOWN_RETRY, and RUNNING → UNKNOWN_STOPPED transitions.
func (b *SQLiteBackend) MarkJobUnknownStopped(ctx context.Context, jobID string, errorMsg string) (map[string]int, error) {
	if _, err := normalizeContext(ctx); err != nil {
		return nil, err
	}
	now := time.Now().UnixNano()

	// Get current job to get assignee ID before clearing
	job, err := b.GetJob(ctx, jobID)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, context.Canceled
		}
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	// Get freed assignee ID before clearing (with count/multiplicity)
	freedAssigneeIDs := make(map[string]int)
	shouldFreeAssignee := job.Status == JobStatusRunning || job.Status == JobStatusCancelling
	if shouldFreeAssignee && job.AssigneeID != "" {
		freedAssigneeIDs[job.AssigneeID] = 1
	}

	// Update to UNKNOWN_STOPPED in a transaction
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
		UPDATE jobs
		SET status = ?,
		    error_message = ?,
		    finalized_at = COALESCE(finalized_at, ?)
		WHERE id = ? AND status IN (?, ?, ?)
	`, JobStatusUnknownStopped, errorMsg, now, jobID, JobStatusCancelling, JobStatusUnknownRetry, JobStatusRunning)
	if err != nil {
		return nil, fmt.Errorf("failed to update job status: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return freedAssigneeIDs, nil
}

// UpdateJobStatus updates a job's status, result, and error message.
// This is a generic method for edge cases not covered by atomic methods.
func (b *SQLiteBackend) UpdateJobStatus(ctx context.Context, jobID string, status JobStatus, result []byte, errorMsg string) (map[string]int, error) {
	if _, err := normalizeContext(ctx); err != nil {
		return nil, err
	}
	now := time.Now().UnixNano()

	job, err := b.GetJob(ctx, jobID)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, context.Canceled
		}
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	if !isValidTransition(job.Status, status) {
		return nil, fmt.Errorf("invalid transition from %s to %s", job.Status, status)
	}

	freedAssigneeIDs := make(map[string]int)
	wasRunningOrCancelling := job.Status == JobStatusRunning || job.Status == JobStatusCancelling
	isTerminal := status == JobStatusCompleted || status == JobStatusStopped || status == JobStatusUnknownStopped
	if wasRunningOrCancelling && isTerminal && job.AssigneeID != "" {
		freedAssigneeIDs[job.AssigneeID] = 1
	}

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	var resultVal sql.NullString
	if result != nil {
		resultVal = sql.NullString{String: string(result), Valid: true}
	}

	var errorMsgVal sql.NullString
	if errorMsg != "" {
		errorMsgVal = sql.NullString{String: errorMsg, Valid: true}
	}

	var startedAt sql.NullInt64
	var finalizedAt sql.NullInt64
	if status == JobStatusRunning && job.StartedAt == nil {
		startedAt = sql.NullInt64{Int64: now, Valid: true}
	}
	if isTerminal && job.FinalizedAt == nil {
		finalizedAt = sql.NullInt64{Int64: now, Valid: true}
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE jobs
		SET status = ?,
		    result = COALESCE(?, result),
		    error_message = COALESCE(?, error_message),
		    started_at = COALESCE(?, started_at),
		    finalized_at = COALESCE(?, finalized_at)
		WHERE id = ?
	`, status, resultVal, errorMsgVal, startedAt, finalizedAt, jobID)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, context.Canceled
		}
		return nil, fmt.Errorf("failed to update job status: %w", err)
	}

	if err := tx.Commit(); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, context.Canceled
		}
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return freedAssigneeIDs, nil
}

// GetJobStats gets statistics for jobs matching ALL provided tags (AND logic)
func (b *SQLiteBackend) GetJobStats(ctx context.Context, tags []string) (*JobStats, error) {
	var query string
	var args []interface{}

	if len(tags) == 0 {
		// When tags are empty, return stats for all jobs
		query = `
			SELECT 
				j.status,
				COUNT(DISTINCT j.id) as count,
				SUM(j.retry_count) as total_retries
			FROM jobs j
			GROUP BY j.status
		`
		args = []interface{}{}
	} else {
		// Build query with AND logic: jobs must have ALL tags
		// Use subquery to find jobs with all tags, then aggregate by status
		query = `
			SELECT 
				j.status,
				COUNT(DISTINCT j.id) as count,
				SUM(j.retry_count) as total_retries
			FROM jobs j
			WHERE j.id IN (
				SELECT jt.job_id
				FROM job_tags jt
				WHERE jt.tag IN (` + placeholdersStr(len(tags)) + `)
				GROUP BY jt.job_id
				HAVING COUNT(DISTINCT jt.tag) = ?
			)
			GROUP BY j.status
		`
		args = make([]interface{}, 0, len(tags)+1)
		for _, tag := range tags {
			args = append(args, tag)
		}
		args = append(args, len(tags))
	}

	rows, err := b.db.QueryContext(ctx, query, args...)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, context.Canceled
		}
		return nil, fmt.Errorf("failed to query job stats: %w", err)
	}
	defer rows.Close()

	stats := &JobStats{
		Tags:          tags,
		TotalJobs:     0,
		PendingJobs:   0,
		RunningJobs:   0,
		CompletedJobs: 0,
		StoppedJobs:   0,
		FailedJobs:    0,
		TotalRetries:  0,
	}

	for rows.Next() {
		var status JobStatus
		var count, totalRetries int32

		if err := rows.Scan(&status, &count, &totalRetries); err != nil {
			return nil, fmt.Errorf("failed to scan status: %w", err)
		}

		stats.TotalJobs += count
		stats.TotalRetries += totalRetries

		switch status {
		case JobStatusInitialPending:
			stats.PendingJobs += count
		case JobStatusRunning:
			stats.RunningJobs += count
		case JobStatusCompleted:
			stats.CompletedJobs += count
		case JobStatusFailedRetry, JobStatusUnknownRetry:
			stats.FailedJobs += count
		case JobStatusStopped, JobStatusUnscheduled, JobStatusUnknownStopped:
			stats.StoppedJobs += count
		}
	}

	return stats, rows.Err()
}

// ResetRunningJobs marks all running and cancelling jobs as unknown (for service restart)
// This is called when the service restarts and there are jobs that were in progress
// RUNNING jobs -> UNKNOWN_RETRY (eligible for retry)
// CANCELLING jobs -> UNKNOWN_STOPPED (terminal state, cancellation was in progress)
func (b *SQLiteBackend) ResetRunningJobs(ctx context.Context) error {
	var err error
	if ctx, err = normalizeContext(ctx); err != nil {
		return err
	}
	// First, query to see what jobs will be reset (for logging)
	var runningJobIDs []string
	rows, err := b.db.QueryContext(ctx, `SELECT id FROM jobs WHERE status = ?`, JobStatusRunning)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var jobID string
			if err := rows.Scan(&jobID); err == nil {
				runningJobIDs = append(runningJobIDs, jobID)
			}
		}
	}

	var cancellingJobIDs []string
	rows2, err := b.db.QueryContext(ctx, `SELECT id FROM jobs WHERE status = ?`, JobStatusCancelling)
	if err == nil {
		defer rows2.Close()
		for rows2.Next() {
			var jobID string
			if err := rows2.Scan(&jobID); err == nil {
				cancellingJobIDs = append(cancellingJobIDs, jobID)
			}
		}
	}

	b.logger.Debug("ResetRunningJobs: found jobs to reset", "runningCount", len(runningJobIDs), "cancellingCount", len(cancellingJobIDs))

	// Log tags for running jobs
	if len(runningJobIDs) > 0 {
		for _, jobID := range runningJobIDs {
			tags, _ := b.getJobTags(ctx, jobID)
			b.logger.Debug("ResetRunningJobs: resetting RUNNING job", "jobID", jobID, "tags", tags)
		}
	}

	// Mark RUNNING jobs as UNKNOWN_RETRY (assignee info preserved per spec)
	result, err := b.db.ExecContext(ctx, `
		UPDATE jobs
		SET status = ?
		WHERE status = ?
	`, JobStatusUnknownRetry, JobStatusRunning)
	if err != nil {
		return fmt.Errorf("failed to reset running jobs: %w", err)
	}
	rowsAffected, _ := result.RowsAffected()
	b.logger.Debug("ResetRunningJobs: reset RUNNING jobs", "rowsAffected", rowsAffected)

	// Mark CANCELLING jobs as UNKNOWN_STOPPED
	result2, err := b.db.ExecContext(ctx, `
		UPDATE jobs
		SET status = ?
		WHERE status = ?
	`, JobStatusUnknownStopped, JobStatusCancelling)
	if err != nil {
		return fmt.Errorf("failed to reset cancelling jobs: %w", err)
	}
	rowsAffected2, _ := result2.RowsAffected()
	b.logger.Debug("ResetRunningJobs: reset CANCELLING jobs", "rowsAffected", rowsAffected2)

	return nil
}

// CleanupExpiredJobs deletes completed jobs older than TTL
func (b *SQLiteBackend) CleanupExpiredJobs(ctx context.Context, ttl time.Duration) error {
	if _, err := normalizeContext(ctx); err != nil {
		return err
	}
	if ttl <= 0 {
		return fmt.Errorf("ttl must be greater than 0")
	}
	cutoff := time.Now().Add(-ttl).UnixNano()
	_, err := b.db.ExecContext(ctx, `
		DELETE FROM jobs
		WHERE status = ? AND finalized_at IS NOT NULL AND finalized_at <= ?
	`, JobStatusCompleted, cutoff)
	if err != nil {
		return fmt.Errorf("failed to cleanup expired jobs: %w", err)
	}

	return nil
}

// DeleteJobs forcefully deletes jobs by tags and/or job IDs
// This method validates that all jobs are in final states (COMPLETED, UNSCHEDULED, STOPPED, UNKNOWN_STOPPED)
// before deletion. If any job is not in a final state, an error is returned.
func (b *SQLiteBackend) DeleteJobs(ctx context.Context, tags []string, jobIDs []string) error {
	var err error
	if ctx, err = normalizeContext(ctx); err != nil {
		return err
	}
	if len(tags) == 0 && len(jobIDs) == 0 {
		return fmt.Errorf("either tags or jobIDs must be provided")
	}
	// Collect all job IDs to delete (union of tag-based and ID-based)
	jobIDSet := make(map[string]bool)

	// Get job IDs from tags (AND logic)
	if len(tags) > 0 {
		query := `
			SELECT DISTINCT j.id
			FROM jobs j
			INNER JOIN job_tags jt ON j.id = jt.job_id
			WHERE jt.tag IN (` + placeholdersStr(len(tags)) + `)
			GROUP BY j.id
			HAVING COUNT(DISTINCT jt.tag) = ?
		`
		args := make([]interface{}, 0, len(tags)+1)
		for _, tag := range tags {
			args = append(args, tag)
		}
		args = append(args, len(tags))

		rows, err := b.db.QueryContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("failed to query jobs by tags: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var jobID string
			if err := rows.Scan(&jobID); err != nil {
				return fmt.Errorf("failed to scan job ID: %w", err)
			}
			jobIDSet[jobID] = true
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("error iterating job IDs: %w", err)
		}
	}

	// Add job IDs from the jobIDs parameter
	for _, jobID := range jobIDs {
		jobIDSet[jobID] = true
	}

	if len(jobIDSet) == 0 {
		return nil
	}

	// Convert set to slice
	allJobIDs := make([]string, 0, len(jobIDSet))
	for jobID := range jobIDSet {
		allJobIDs = append(allJobIDs, jobID)
	}

	// Validate all jobs are in final states
	nonFinalJobs := make([]string, 0)
	for _, jobID := range allJobIDs {
		job, err := b.GetJob(ctx, jobID)
		if err != nil {
			// Job not found - skip it (not an error for cleanup)
			continue
		}

		// Check if job is in a final state
		if job.Status != JobStatusCompleted &&
			job.Status != JobStatusUnscheduled &&
			job.Status != JobStatusStopped &&
			job.Status != JobStatusUnknownStopped {
			nonFinalJobs = append(nonFinalJobs, jobID)
		}
	}

	// If any job is not in a final state, return error
	if len(nonFinalJobs) > 0 {
		return fmt.Errorf("cannot delete jobs: %d job(s) are not in final states (COMPLETED, UNSCHEDULED, STOPPED, UNKNOWN_STOPPED): %v", len(nonFinalJobs), nonFinalJobs)
	}

	// All jobs are in final states - proceed with deletion
	// Use transaction to ensure atomicity
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Build placeholders for IN clause
	placeholders := placeholdersStr(len(allJobIDs))
	args := make([]interface{}, len(allJobIDs))
	for i, jobID := range allJobIDs {
		args[i] = jobID
	}

	// Delete jobs (cascade will handle job_tags table)
	_, err = tx.ExecContext(ctx, `
		DELETE FROM jobs
		WHERE id IN (`+placeholders+`)
	`, args...)
	if err != nil {
		return fmt.Errorf("failed to delete jobs: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetJob retrieves a job by ID
func (b *SQLiteBackend) GetJob(ctx context.Context, jobID string) (*Job, error) {
	var err error
	if ctx, err = normalizeContext(ctx); err != nil {
		return nil, err
	}
	job := &Job{}
	var createdAt, startedAt, completedAt, lastRetryAt, assignedAt sql.NullInt64
	var assigneeID, errorMessage sql.NullString
	var result sql.NullString

	err = b.db.QueryRowContext(ctx, `
		SELECT id, status, job_type, job_definition, created_at,
		       started_at, finalized_at, error_message, result,
		       retry_count, last_retry_at, assignee_id, assigned_at
		FROM jobs
		WHERE id = ?
	`, jobID).Scan(
		&job.ID, &job.Status, &job.JobType, &job.JobDefinition,
		&createdAt, &startedAt, &completedAt, &errorMessage,
		&result, &job.RetryCount, &lastRetryAt, &assigneeID, &assignedAt,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	job.CreatedAt = time.Unix(0, createdAt.Int64)
	if startedAt.Valid {
		t := time.Unix(0, startedAt.Int64)
		job.StartedAt = &t
	}
	if completedAt.Valid {
		t := time.Unix(0, completedAt.Int64)
		job.FinalizedAt = &t
	}
	if lastRetryAt.Valid {
		t := time.Unix(0, lastRetryAt.Int64)
		job.LastRetryAt = &t
	}
	if assigneeID.Valid {
		job.AssigneeID = assigneeID.String
	}
	if assignedAt.Valid {
		t := time.Unix(0, assignedAt.Int64)
		job.AssignedAt = &t
	}
	if errorMessage.Valid {
		job.ErrorMessage = errorMessage.String
	}
	if result.Valid {
		job.Result = []byte(result.String)
	}

	// Load tags
	tags, err := b.getJobTags(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get tags for job %s: %w", jobID, err)
	}
	job.Tags = tags

	return job, nil
}

// CancelJobs cancels jobs by tags and/or job IDs (batch cancellation)
func (b *SQLiteBackend) CancelJobs(ctx context.Context, tags []string, jobIDs []string) ([]string, []string, error) {
	var err error
	if ctx, err = normalizeContext(ctx); err != nil {
		return nil, nil, err
	}
	if len(tags) == 0 && len(jobIDs) == 0 {
		return nil, nil, fmt.Errorf("either tags or jobIDs must be provided")
	}
	// Collect all job IDs to cancel (union of tag-based and ID-based)
	jobIDSet := make(map[string]bool)

	// Get job IDs from tags (AND logic)
	if len(tags) > 0 {
		query := `
			SELECT DISTINCT j.id
			FROM jobs j
			INNER JOIN job_tags jt ON j.id = jt.job_id
			WHERE jt.tag IN (` + placeholdersStr(len(tags)) + `)
			GROUP BY j.id
			HAVING COUNT(DISTINCT jt.tag) = ?
		`
		args := make([]interface{}, 0, len(tags)+1)
		for _, tag := range tags {
			args = append(args, tag)
		}
		args = append(args, len(tags))

		rows, err := b.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to query jobs by tags: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var jobID string
			if err := rows.Scan(&jobID); err != nil {
				return nil, nil, fmt.Errorf("failed to scan job ID: %w", err)
			}
			jobIDSet[jobID] = true
		}
		if err := rows.Err(); err != nil {
			return nil, nil, fmt.Errorf("error iterating job IDs: %w", err)
		}
	}

	// Add job IDs from the jobIDs parameter
	for _, jobID := range jobIDs {
		jobIDSet[jobID] = true
	}

	if len(jobIDSet) == 0 {
		return []string{}, []string{}, nil
	}

	// Convert set to slice
	allJobIDs := make([]string, 0, len(jobIDSet))
	for jobID := range jobIDSet {
		allJobIDs = append(allJobIDs, jobID)
	}

	// Get all jobs to check their status
	cancelledJobIDs := make([]string, 0)
	unknownJobIDs := make([]string, 0)

	for _, jobID := range allJobIDs {
		job, err := b.GetJob(ctx, jobID)
		if err != nil {
			// Job not found
			unknownJobIDs = append(unknownJobIDs, jobID)
			continue
		}

		// Check if job is in a terminal state
		if job.Status == JobStatusCompleted ||
			job.Status == JobStatusStopped || job.Status == JobStatusUnscheduled ||
			job.Status == JobStatusUnknownStopped {
			// Already in terminal state - consider as unknown (already completed/cancelled)
			unknownJobIDs = append(unknownJobIDs, jobID)
			continue
		}

		// Check if job is already in cancelling state
		if job.Status == JobStatusCancelling {
			// Already cancelling - consider as cancelled
			cancelledJobIDs = append(cancelledJobIDs, jobID)
			continue
		}

		// Determine new status based on current status
		var newStatus JobStatus
		if job.Status == JobStatusInitialPending {
			newStatus = JobStatusUnscheduled
		} else if job.Status == JobStatusRunning {
			newStatus = JobStatusCancelling
		} else if job.Status == JobStatusFailedRetry {
			// FAILED_RETRY jobs can be cancelled to STOPPED
			newStatus = JobStatusStopped
		} else if job.Status == JobStatusUnknownRetry {
			// UNKNOWN_RETRY jobs can be cancelled to STOPPED
			newStatus = JobStatusStopped
		} else {
			// Unexpected status
			unknownJobIDs = append(unknownJobIDs, jobID)
			continue
		}

		// Update job status
		// For FAILED_RETRY and UNKNOWN_RETRY jobs transitioning to STOPPED, set finalized_at
		now := time.Now().UnixNano()
		var completedAt sql.NullInt64
		if newStatus == JobStatusStopped && (job.Status == JobStatusFailedRetry || job.Status == JobStatusUnknownRetry) {
			completedAt = sql.NullInt64{Int64: now, Valid: true}
		}

		_, err = b.db.ExecContext(ctx, `
			UPDATE jobs
			SET status = ?,
			    finalized_at = COALESCE(?, finalized_at)
			WHERE id = ?
		`, newStatus, completedAt, jobID)
		if err != nil {
			// Failed to update - consider as unknown
			unknownJobIDs = append(unknownJobIDs, jobID)
			continue
		}

		// Successfully cancelled
		cancelledJobIDs = append(cancelledJobIDs, jobID)
	}

	return cancelledJobIDs, unknownJobIDs, nil
}

// MarkWorkerUnresponsive marks all jobs assigned to the given assigneeID as unresponsive
// RUNNING → UNKNOWN_RETRY, CANCELLING → UNKNOWN_STOPPED
func (b *SQLiteBackend) MarkWorkerUnresponsive(ctx context.Context, assigneeID string) error {
	var err error
	if ctx, err = normalizeContext(ctx); err != nil {
		return err
	}
	if assigneeID == "" {
		return fmt.Errorf("assigneeID is required")
	}
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Update RUNNING jobs to UNKNOWN_RETRY
	// Note: AssigneeID and AssignedAt are preserved for historical tracking (per spec)
	_, err = tx.ExecContext(ctx, `
		UPDATE jobs
		SET status = ?
		WHERE assignee_id = ? AND status = ?
	`, JobStatusUnknownRetry, assigneeID, JobStatusRunning)
	if err != nil {
		return fmt.Errorf("failed to mark running jobs as unknown: %w", err)
	}

	// Update CANCELLING jobs to UNKNOWN_STOPPED
	// Note: AssigneeID and AssignedAt are preserved for historical tracking (per spec)
	now := time.Now().UnixNano()
	_, err = tx.ExecContext(ctx, `
		UPDATE jobs
		SET status = ?,
		    finalized_at = COALESCE(finalized_at, ?)
		WHERE assignee_id = ? AND status = ?
	`, JobStatusUnknownStopped, now, assigneeID, JobStatusCancelling)
	if err != nil {
		return fmt.Errorf("failed to mark cancelling jobs as unknown stopped: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// AcknowledgeCancellation handles cancellation acknowledgment from worker
// wasExecuting: true if job was executing when cancellation was processed, false if job was unknown/finished
// CANCELLING → STOPPED (wasExecuting=true) or UNKNOWN_STOPPED (wasExecuting=false)
func (b *SQLiteBackend) AcknowledgeCancellation(ctx context.Context, jobID string, wasExecuting bool) error {
	var err error
	if ctx, err = normalizeContext(ctx); err != nil {
		return err
	}
	// Get current job to validate it's in CANCELLING state
	job, err := b.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("failed to get job: %w", err)
	}

	if job.Status != JobStatusCancelling {
		return fmt.Errorf("job %s is not in CANCELLING state (current: %s)", jobID, job.Status)
	}

	var newStatus JobStatus
	if wasExecuting {
		newStatus = JobStatusStopped
	} else {
		newStatus = JobStatusUnknownStopped
	}

	now := time.Now().UnixNano()
	_, err = b.db.ExecContext(ctx, `
		UPDATE jobs
		SET status = ?,
		    finalized_at = COALESCE(finalized_at, ?)
		WHERE id = ? AND status = ?
	`, newStatus, now, jobID, JobStatusCancelling)
	if err != nil {
		return fmt.Errorf("failed to acknowledge cancellation: %w", err)
	}

	return nil
}

// placeholdersStr generates SQL placeholders string
func placeholdersStr(n int) string {
	if n == 0 {
		return ""
	}
	result := "?"
	for i := 1; i < n; i++ {
		result += ", ?"
	}
	return result
}

// SetJobFinalizedAtForTesting sets the finalized_at timestamp for a job (test helper only)
// This is used in tests to simulate jobs completed at different times
func (b *SQLiteBackend) SetJobFinalizedAtForTesting(ctx context.Context, jobID string, completedAt time.Time) error {
	_, err := b.db.ExecContext(ctx, `
		UPDATE jobs
		SET finalized_at = ?
		WHERE id = ?
	`, completedAt.UnixNano(), jobID)
	return err
}
