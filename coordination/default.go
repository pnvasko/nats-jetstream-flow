package coordination

import "time"

const (
	defaultLimiterBucketPrefix  = "limiter"
	defaultLimiterBucketScope   = "default"
	defaultLimiterGroupIdPrefix = "group"

	defaultLimiterRetryWait = 50 * time.Millisecond
	maxRetryLimiterAttempts = 10

	// Default TTL for inactive keys (time since last write).
	// Must exceed typical quiet gaps for active keys.
	defaultCleanupTTL = 24 * time.Hour

	defaultWaitGroupBucketPrefix = "wait_group"
	defaultWaitGroupScope        = "default"
)
