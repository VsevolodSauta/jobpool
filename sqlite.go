//go:build sqlite
// +build sqlite

package jobpool

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// SQLiteBackend implements the Backend interface using SQLite.
// It provides ACID transactions and is suitable for single-server deployments.
type SQLiteBackend struct {
	db *sql.DB
}

// NewSQLiteBackend creates a new SQLite backend.
// The database file will be created if it doesn't exist.
// dbPath is the path to the SQLite database file.
func NewSQLiteBackend(dbPath string) (*SQLiteBackend, error) {
	db, err := sql.Open("sqlite3", dbPath+"?_foreign_keys=on")
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	backend := &SQLiteBackend{db: db}

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
		completed_at INTEGER,
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
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Insert job
	_, err = tx.ExecContext(ctx, `
		INSERT INTO jobs (id, status, job_type, job_definition, created_at, retry_count, assignee_id, assigned_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, job.ID, job.Status, job.JobType, job.JobDefinition, job.CreatedAt.Unix(), job.RetryCount, job.AssigneeID, nil)
	if err != nil {
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
	if len(jobs) == 0 {
		return []string{}, nil
	}

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	jobIDs := make([]string, 0, len(jobs))

	// Insert jobs
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO jobs (id, status, job_type, job_definition, created_at, retry_count, assignee_id, assigned_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, job := range jobs {
		_, err = stmt.ExecContext(ctx, job.ID, job.Status, job.JobType, job.JobDefinition, job.CreatedAt.Unix(), job.RetryCount, job.AssigneeID, nil)
		if err != nil {
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
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// First, select and update jobs atomically
	// Select PENDING, FAILED, and UNKNOWN_RETRY jobs (UNKNOWN_STOPPED should not be scheduled)
	// FAILED jobs are treated like PENDING during scheduling (can be assigned without state change)
	now := time.Now().Unix()

	// Build query with optional tag filtering
	var query string
	var args []interface{}

	if len(tags) > 0 {
		// Filter by tags using AND logic: job must have ALL provided tags
		// Use GROUP BY and HAVING to ensure all tags are present
		query = `
			SELECT j.id, j.status, j.job_type, j.job_definition, j.created_at, 
			       j.started_at, j.completed_at, j.error_message, j.result, 
			       j.retry_count, j.last_retry_at, j.assignee_id, j.assigned_at
			FROM jobs j
			INNER JOIN job_tags jt ON j.id = jt.job_id
			WHERE j.status IN (?, ?, ?)
			  AND jt.tag IN (` + placeholdersStr(len(tags)) + `)
			GROUP BY j.id, j.status, j.job_type, j.job_definition, j.created_at, 
			         j.started_at, j.completed_at, j.error_message, j.result, 
			         j.retry_count, j.last_retry_at, j.assignee_id, j.assigned_at
			HAVING COUNT(DISTINCT jt.tag) = ?
			ORDER BY COALESCE(j.last_retry_at, j.created_at) ASC
			LIMIT ?
		`
		args = make([]interface{}, 0, 3+len(tags)+2)
		args = append(args, JobStatusPending, JobStatusFailed, JobStatusUnknownRetry)
		for _, tag := range tags {
			args = append(args, tag)
		}
		args = append(args, len(tags), limit)
	} else {
		// No tag filtering
		query = `
			SELECT j.id, j.status, j.job_type, j.job_definition, j.created_at, 
			       j.started_at, j.completed_at, j.error_message, j.result, 
			       j.retry_count, j.last_retry_at, j.assignee_id, j.assigned_at
			FROM jobs j
			WHERE j.status IN (?, ?, ?)
			ORDER BY COALESCE(j.last_retry_at, j.created_at) ASC
			LIMIT ?
		`
		args = []interface{}{JobStatusPending, JobStatusFailed, JobStatusUnknownRetry, limit}
	}

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query jobs: %w", err)
	}

	jobs := make([]*Job, 0, limit)
	jobIDs := make([]string, 0, limit)

	for rows.Next() {
		job := &Job{}
		var createdAt, startedAt, completedAt, lastRetryAt, assignedAt sql.NullInt64
		var assigneeID, errorMessage sql.NullString
		var result sql.NullString

		err := rows.Scan(
			&job.ID, &job.Status, &job.JobType, &job.JobDefinition,
			&createdAt, &startedAt, &completedAt, &errorMessage,
			&result, &job.RetryCount, &lastRetryAt, &assigneeID, &assignedAt,
		)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed to scan job: %w", err)
		}

		job.CreatedAt = time.Unix(createdAt.Int64, 0)
		if startedAt.Valid {
			t := time.Unix(startedAt.Int64, 0)
			job.StartedAt = &t
		}
		if completedAt.Valid {
			t := time.Unix(completedAt.Int64, 0)
			job.CompletedAt = &t
		}
		if lastRetryAt.Valid {
			t := time.Unix(lastRetryAt.Int64, 0)
			job.LastRetryAt = &t
		}
		if assigneeID.Valid {
			job.AssigneeID = assigneeID.String
		}
		if assignedAt.Valid {
			t := time.Unix(assignedAt.Int64, 0)
			job.AssignedAt = &t
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
	rows.Close()

	if len(jobIDs) == 0 {
		tx.Rollback()
		return []*Job{}, nil
	}

	// Update jobs to assign them and set status to running
	args = make([]interface{}, 0, len(jobIDs)+3)
	args = append(args, JobStatusRunning, assigneeID, now)
	for _, id := range jobIDs {
		args = append(args, id)
	}

	query = fmt.Sprintf(`
		UPDATE jobs
		SET status = ?,
		    assignee_id = ?, assigned_at = ?
		WHERE id IN (%s)
	`, placeholdersStr(len(jobIDs)))

	_, err = tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to assign jobs: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Load tags for all jobs
	for _, job := range jobs {
		tags, err := b.getJobTags(ctx, job.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to get tags for job %s: %w", job.ID, err)
		}
		job.Tags = tags
		job.AssigneeID = assigneeID
		assignedTime := time.Unix(now, 0)
		job.AssignedAt = &assignedTime
	}

	return jobs, nil
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
func (b *SQLiteBackend) CompleteJob(ctx context.Context, jobID string, result []byte) error {
	now := time.Now().Unix()

	// Get current job to validate it's in a valid state
	job, err := b.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("failed to get job: %w", err)
	}

	// Validate job is in a state that can transition to COMPLETED
	validStates := map[JobStatus]bool{
		JobStatusRunning:        true,
		JobStatusCancelling:     true,
		JobStatusUnknownRetry:   true,
		JobStatusUnknownStopped: true,
	}
	if !validStates[job.Status] {
		return fmt.Errorf("job %s is not in a valid state for completion (current: %s)", jobID, job.Status)
	}

	// Update to COMPLETED in a transaction
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	var startedAt sql.NullInt64
	if job.StartedAt != nil {
		startedAt = sql.NullInt64{Int64: job.StartedAt.Unix(), Valid: true}
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE jobs
		SET status = ?,
		    result = ?,
		    completed_at = ?,
		    started_at = COALESCE(?, started_at),
		    assignee_id = NULL,
		    assigned_at = NULL
		WHERE id = ? AND status IN (?, ?, ?, ?)
	`, JobStatusCompleted, result, now, startedAt, jobID, JobStatusRunning, JobStatusCancelling, JobStatusUnknownRetry, JobStatusUnknownStopped)
	if err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// FailJob atomically transitions a job to FAILED, then to PENDING, incrementing the retry count.
// Supports RUNNING and UNKNOWN_RETRY → FAILED → PENDING transitions.
func (b *SQLiteBackend) FailJob(ctx context.Context, jobID string, errorMsg string) error {
	now := time.Now().Unix()

	// Get current job to validate it's in a valid state
	job, err := b.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("failed to get job: %w", err)
	}

	// Validate job is in a state that can transition to FAILED
	if job.Status != JobStatusRunning && job.Status != JobStatusUnknownRetry {
		return fmt.Errorf("job %s is not in RUNNING or UNKNOWN_RETRY state (current: %s)", jobID, job.Status)
	}

	// Atomic transaction: FAILED → PENDING with retry increment
	// We do this in a single transaction, but the FAILED state is observable
	// because we update status to FAILED first, then immediately to PENDING
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// First, update to FAILED with error message
	_, err = tx.ExecContext(ctx, `
		UPDATE jobs
		SET status = ?,
		    error_message = ?
		WHERE id = ? AND status IN (?, ?)
	`, JobStatusFailed, errorMsg, jobID, JobStatusRunning, JobStatusUnknownRetry)
	if err != nil {
		return fmt.Errorf("failed to update job to FAILED: %w", err)
	}

	// Immediately increment retry count and set to PENDING
	_, err = tx.ExecContext(ctx, `
		UPDATE jobs
		SET retry_count = retry_count + 1,
		    last_retry_at = ?,
		    status = ?,
		    assignee_id = NULL,
		    assigned_at = NULL
		WHERE id = ? AND status = ?
	`, now, JobStatusPending, jobID, JobStatusFailed)
	if err != nil {
		return fmt.Errorf("failed to increment retry count: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// StopJob atomically transitions a job to STOPPED with an error message.
// Supports RUNNING, CANCELLING, and UNKNOWN_RETRY → STOPPED transitions.
func (b *SQLiteBackend) StopJob(ctx context.Context, jobID string, errorMsg string) error {
	now := time.Now().Unix()

	// Get current job to validate it's in a valid state
	job, err := b.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("failed to get job: %w", err)
	}

	// Validate job is in a state that can transition to STOPPED
	validStates := map[JobStatus]bool{
		JobStatusRunning:      true,
		JobStatusCancelling:   true,
		JobStatusUnknownRetry: true,
	}
	if !validStates[job.Status] {
		return fmt.Errorf("job %s is not in a valid state for stopping (current: %s)", jobID, job.Status)
	}

	// Update to STOPPED in a transaction
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
		UPDATE jobs
		SET status = ?,
		    error_message = ?,
		    completed_at = COALESCE(completed_at, ?),
		    assignee_id = NULL,
		    assigned_at = NULL
		WHERE id = ? AND status IN (?, ?, ?)
	`, JobStatusStopped, errorMsg, now, jobID, JobStatusRunning, JobStatusCancelling, JobStatusUnknownRetry)
	if err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// StopJobWithRetry atomically transitions a job from CANCELLING to STOPPED with retry increment.
// Applies all effects from the transitory FAILED state (retry increment + error message).
func (b *SQLiteBackend) StopJobWithRetry(ctx context.Context, jobID string, errorMsg string) error {
	now := time.Now().Unix()

	// Get current job to validate it's in CANCELLING state
	job, err := b.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("failed to get job: %w", err)
	}

	if job.Status != JobStatusCancelling {
		return fmt.Errorf("job %s is not in CANCELLING state (current: %s)", jobID, job.Status)
	}

	// Atomic transaction: CANCELLING → STOPPED with retry increment
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
		UPDATE jobs
		SET status = ?,
		    error_message = ?,
		    retry_count = retry_count + 1,
		    last_retry_at = ?,
		    completed_at = COALESCE(completed_at, ?),
		    assignee_id = NULL,
		    assigned_at = NULL
		WHERE id = ? AND status = ?
	`, JobStatusStopped, errorMsg, now, now, jobID, JobStatusCancelling)
	if err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// MarkJobUnknownStopped atomically transitions a job to UNKNOWN_STOPPED with an error message.
// Supports CANCELLING, UNKNOWN_RETRY, and RUNNING → UNKNOWN_STOPPED transitions.
func (b *SQLiteBackend) MarkJobUnknownStopped(ctx context.Context, jobID string, errorMsg string) error {
	now := time.Now().Unix()

	// Update to UNKNOWN_STOPPED in a transaction
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
		UPDATE jobs
		SET status = ?,
		    error_message = ?,
		    completed_at = COALESCE(completed_at, ?),
		    assignee_id = NULL,
		    assigned_at = NULL
		WHERE id = ? AND status IN (?, ?, ?)
	`, JobStatusUnknownStopped, errorMsg, now, jobID, JobStatusCancelling, JobStatusUnknownRetry, JobStatusRunning)
	if err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// UpdateJobStatus updates a job's status, result, and error message.
// This is a generic method for edge cases not covered by atomic methods.
func (b *SQLiteBackend) UpdateJobStatus(ctx context.Context, jobID string, status JobStatus, result []byte, errorMsg string) error {
	now := time.Now().Unix()

	// Get current job to check if it exists
	job, err := b.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("failed to get job: %w", err)
	}

	// Update job status in a transaction
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Build update query based on what fields need to be updated
	var resultVal sql.NullString
	if result != nil {
		resultVal = sql.NullString{String: string(result), Valid: true}
	}

	var errorMsgVal sql.NullString
	if errorMsg != "" {
		errorMsgVal = sql.NullString{String: errorMsg, Valid: true}
	}

	// Determine which timestamps to update based on status
	var startedAt sql.NullInt64
	var completedAt sql.NullInt64

	if status == JobStatusRunning && job.StartedAt == nil {
		startedAt = sql.NullInt64{Int64: now, Valid: true}
	}

	if status == JobStatusCompleted || status == JobStatusStopped || status == JobStatusUnknownStopped {
		if job.CompletedAt == nil {
			completedAt = sql.NullInt64{Int64: now, Valid: true}
		}
	}

	// Clear assignee_id and assigned_at if transitioning to non-running state
	shouldClearAssignee := status != JobStatusRunning && status != JobStatusCancelling

	var assigneeIDVal, assignedAtVal interface{}
	if shouldClearAssignee {
		assigneeIDVal = nil
		assignedAtVal = nil
	} else {
		// Keep existing values
		assigneeIDVal = job.AssigneeID
		if job.AssignedAt != nil {
			assignedAtVal = job.AssignedAt.Unix()
		} else {
			assignedAtVal = nil
		}
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE jobs
		SET status = ?,
		    result = COALESCE(?, result),
		    error_message = COALESCE(?, error_message),
		    started_at = COALESCE(?, started_at),
		    completed_at = COALESCE(?, completed_at),
		    assignee_id = ?,
		    assigned_at = ?
		WHERE id = ?
	`, status, resultVal, errorMsgVal, startedAt, completedAt, assigneeIDVal, assignedAtVal, jobID)
	if err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetJobStats gets statistics for jobs matching ALL provided tags (AND logic)
func (b *SQLiteBackend) GetJobStats(ctx context.Context, tags []string) (*JobStats, error) {
	if len(tags) == 0 {
		return &JobStats{
			Tags:          tags,
			TotalJobs:     0,
			PendingJobs:   0,
			RunningJobs:   0,
			CompletedJobs: 0,
			FailedJobs:    0,
			TotalRetries:  0,
		}, nil
	}

	// Build query with AND logic: jobs must have ALL tags
	query := `
		SELECT 
			j.status,
			COUNT(DISTINCT j.id) as count,
			SUM(j.retry_count) as total_retries
		FROM jobs j
		INNER JOIN job_tags jt ON j.id = jt.job_id
		WHERE jt.tag IN (` + placeholdersStr(len(tags)) + `)
		GROUP BY j.id, j.status
		HAVING COUNT(DISTINCT jt.tag) = ?
	`

	args := make([]interface{}, 0, len(tags)+1)
	for _, tag := range tags {
		args = append(args, tag)
	}
	args = append(args, len(tags))

	rows, err := b.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query job stats: %w", err)
	}
	defer rows.Close()

	stats := &JobStats{
		Tags:          tags,
		TotalJobs:     0,
		PendingJobs:   0,
		RunningJobs:   0,
		CompletedJobs: 0,
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
		case JobStatusPending:
			stats.PendingJobs += count
		case JobStatusRunning:
			stats.RunningJobs += count
		case JobStatusCompleted:
			stats.CompletedJobs += count
		case JobStatusFailed:
			stats.FailedJobs += count
		}
	}

	return stats, rows.Err()
}

// ResetRunningJobs marks all running and cancelling jobs as unknown (for service restart)
// This is called when the service restarts and there are jobs that were in progress
// RUNNING jobs -> UNKNOWN_RETRY (eligible for retry)
// CANCELLING jobs -> UNKNOWN_STOPPED (terminal state, cancellation was in progress)
func (b *SQLiteBackend) ResetRunningJobs(ctx context.Context) error {
	// Mark RUNNING jobs as UNKNOWN_RETRY
	_, err := b.db.ExecContext(ctx, `
		UPDATE jobs
		SET status = ?,
		    assignee_id = NULL,
		    assigned_at = NULL
		WHERE status = ?
	`, JobStatusUnknownRetry, JobStatusRunning)
	if err != nil {
		return fmt.Errorf("failed to reset running jobs: %w", err)
	}

	// Mark CANCELLING jobs as UNKNOWN_STOPPED
	_, err = b.db.ExecContext(ctx, `
		UPDATE jobs
		SET status = ?,
		    assignee_id = NULL,
		    assigned_at = NULL
		WHERE status = ?
	`, JobStatusUnknownStopped, JobStatusCancelling)
	if err != nil {
		return fmt.Errorf("failed to reset cancelling jobs: %w", err)
	}

	return nil
}

// CleanupExpiredJobs deletes completed jobs older than TTL
func (b *SQLiteBackend) CleanupExpiredJobs(ctx context.Context, ttl time.Duration) error {
	cutoff := time.Now().Add(-ttl).Unix()
	_, err := b.db.ExecContext(ctx, `
		DELETE FROM jobs
		WHERE status = ? AND completed_at IS NOT NULL AND completed_at < ?
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
	job := &Job{}
	var createdAt, startedAt, completedAt, lastRetryAt, assignedAt sql.NullInt64
	var assigneeID, errorMessage sql.NullString
	var result sql.NullString

	err := b.db.QueryRowContext(ctx, `
		SELECT id, status, job_type, job_definition, created_at,
		       started_at, completed_at, error_message, result,
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

	job.CreatedAt = time.Unix(createdAt.Int64, 0)
	if startedAt.Valid {
		t := time.Unix(startedAt.Int64, 0)
		job.StartedAt = &t
	}
	if completedAt.Valid {
		t := time.Unix(completedAt.Int64, 0)
		job.CompletedAt = &t
	}
	if lastRetryAt.Valid {
		t := time.Unix(lastRetryAt.Int64, 0)
		job.LastRetryAt = &t
	}
	if assigneeID.Valid {
		job.AssigneeID = assigneeID.String
	}
	if assignedAt.Valid {
		t := time.Unix(assignedAt.Int64, 0)
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
		if job.Status == JobStatusPending {
			newStatus = JobStatusUnscheduled
		} else if job.Status == JobStatusRunning {
			newStatus = JobStatusCancelling
		} else if job.Status == JobStatusFailed {
			// FAILED jobs can be cancelled to STOPPED
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
		// For FAILED and UNKNOWN_RETRY jobs transitioning to STOPPED, set completed_at
		now := time.Now().Unix()
		var completedAt sql.NullInt64
		if newStatus == JobStatusStopped && (job.Status == JobStatusFailed || job.Status == JobStatusUnknownRetry) {
			completedAt = sql.NullInt64{Int64: now, Valid: true}
		}

		_, err = b.db.ExecContext(ctx, `
			UPDATE jobs
			SET status = ?,
			    completed_at = COALESCE(?, completed_at)
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
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Update RUNNING jobs to UNKNOWN_RETRY
	_, err = tx.ExecContext(ctx, `
		UPDATE jobs
		SET status = ?,
		    assignee_id = NULL,
		    assigned_at = NULL
		WHERE assignee_id = ? AND status = ?
	`, JobStatusUnknownRetry, assigneeID, JobStatusRunning)
	if err != nil {
		return fmt.Errorf("failed to mark running jobs as unknown: %w", err)
	}

	// Update CANCELLING jobs to UNKNOWN_STOPPED
	now := time.Now().Unix()
	_, err = tx.ExecContext(ctx, `
		UPDATE jobs
		SET status = ?,
		    completed_at = COALESCE(completed_at, ?),
		    assignee_id = NULL,
		    assigned_at = NULL
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

	now := time.Now().Unix()
	_, err = b.db.ExecContext(ctx, `
		UPDATE jobs
		SET status = ?,
		    completed_at = COALESCE(completed_at, ?),
		    assignee_id = NULL,
		    assigned_at = NULL
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

// SetJobCompletedAtForTesting sets the completed_at timestamp for a job (test helper only)
// This is used in tests to simulate jobs completed at different times
func (b *SQLiteBackend) SetJobCompletedAtForTesting(ctx context.Context, jobID string, completedAt time.Time) error {
	_, err := b.db.ExecContext(ctx, `
		UPDATE jobs
		SET completed_at = ?
		WHERE id = ?
	`, completedAt.Unix(), jobID)
	return err
}
