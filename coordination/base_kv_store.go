package coordination

import "time"

type baseKvStore struct {
	scope            string
	bucketPrefix     string
	bucketName       string
	retryWait        time.Duration
	cleanupTTL       time.Duration
	maxRetryAttempts int
}

func (s *baseKvStore) setScope(scope string) {
	s.scope = scope
}

func (s *baseKvStore) setBucketPrefix(bucketPrefix string) {
	s.bucketPrefix = bucketPrefix
}
func (s *baseKvStore) setBucketName(bucketName string) {
	s.bucketName = bucketName
}

func (s *baseKvStore) setCleanupTTL(ttl time.Duration) {
	s.cleanupTTL = ttl
}

func (s *baseKvStore) setRetryWait(ttl time.Duration) {
	s.retryWait = ttl
}

func (s *baseKvStore) setMaxRetryAttempts(n int) {
	s.maxRetryAttempts = n
}

var _ storeOptionScopable = (*baseKvStore)(nil)
