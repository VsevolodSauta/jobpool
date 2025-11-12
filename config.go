package jobpool

import (
	"os"
	"strconv"
	"time"
)

// Config represents job pool configuration.
type Config struct {
	// TTL for completed jobs (default: 30 days).
	// Completed jobs older than TTL will be automatically deleted.
	TTL time.Duration

	// Cleanup periodicity (default: 1 day).
	// How often the cleanup process runs to delete expired jobs.
	CleanupInterval time.Duration

	// Batch size for worker (default: 10).
	// Maximum number of jobs to process in a single batch.
	BatchSize int
}

// LoadConfig loads job pool configuration from environment variables.
// It reads the following environment variables:
//   - JOBPOOL_TTL: TTL for completed jobs (default: 30 days)
//   - JOBPOOL_CLEANUP_INTERVAL: Cleanup interval (default: 1 day)
//   - JOBPOOL_BATCH_SIZE: Batch size for worker (default: 10)
//
// Duration values can be specified as:
//   - Integer number of days (e.g., "30" = 30 days)
//   - Duration string (e.g., "24h", "7d", "1h30m")
//
// Returns a Config struct with default values if environment variables are not set.
func LoadConfig() *Config {
	cfg := &Config{
		TTL:             getEnvDuration("JOBPOOL_TTL", 30*24*time.Hour),           // 30 days
		CleanupInterval: getEnvDuration("JOBPOOL_CLEANUP_INTERVAL", 24*time.Hour), // 1 day
		BatchSize:       getEnvInt("JOBPOOL_BATCH_SIZE", 10),
	}

	return cfg
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if days, err := strconv.Atoi(value); err == nil {
			return time.Duration(days) * 24 * time.Hour
		}
		// Try parsing as duration string (e.g., "24h", "7d")
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}
