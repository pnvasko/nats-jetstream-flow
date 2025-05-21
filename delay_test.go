package nats_jetstream_flow

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestStaticDelay(t *testing.T) {
	delayDuration := 5 * time.Second
	sd := NewStaticDelay(delayDuration)

	assert.Implements(t, (*Delay)(nil), sd) // Check interface implementation

	// WaitTime should always return the static delay, regardless of retryNum
	assert.Equal(t, delayDuration, sd.WaitTime(0))
	assert.Equal(t, delayDuration, sd.WaitTime(1))
	assert.Equal(t, delayDuration, sd.WaitTime(100))
	assert.Equal(t, delayDuration, sd.WaitTime(uint64(1<<63))) // Test large retry num
}

func TestMaxRetryDelay(t *testing.T) {
	delayDuration := 2 * time.Second
	maxRetries := uint64(3)
	mrd := NewMaxRetryDelay(delayDuration, maxRetries)

	assert.Implements(t, (*Delay)(nil), mrd) // Check interface implementation

	// Retry numbers less than maxRetries should return the static delay
	assert.Equal(t, delayDuration, mrd.WaitTime(0))            // First attempt (0 retries)
	assert.Equal(t, delayDuration, mrd.WaitTime(1))            // 1 retry
	assert.Equal(t, delayDuration, mrd.WaitTime(maxRetries-1)) // Last retry before max

	// Retry numbers equal to or greater than maxRetries should return TermSignal
	assert.Equal(t, TermSignal, mrd.WaitTime(maxRetries))
	assert.Equal(t, TermSignal, mrd.WaitTime(maxRetries+1))
	assert.Equal(t, TermSignal, mrd.WaitTime(uint64(1<<63))) // Test large retry num
}
