package coordination

import "time"

const (
	defaultRetryWait           = 50 * time.Millisecond
	defaultMaxRetryAttempts    = 10 // Limit retries for certain operations like Release
	defaultMaxProcessingJitter = 50 * time.Millisecond
)
