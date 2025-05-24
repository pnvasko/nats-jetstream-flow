package coordination

import (
	"errors"
	"github.com/nats-io/nats.go/jetstream"
	"math/rand"
	"time"
)

func getJitter() time.Duration {
	for i := 0; i < defaultMaxRetryAttempts; i++ {
		jitterNanos := 100 + time.Duration(rand.Int63n(int64(defaultMaxProcessingJitter)))
		if jitterNanos > 0 {
			return time.Duration(jitterNanos)
		}
	}
	return 100 + defaultMaxProcessingJitter
}

func isJSAlreadyExistsError(err error) bool {
	var apiErr *jetstream.APIError

	ok := errors.As(err, &apiErr)
	if !ok {
		return false
	}
	return apiErr.ErrorCode == jetstream.JSErrCodeStreamNameInUse // || apiErr.ErrorCode == 10058
}

func isJSWrongLastSequence(err error) bool {
	var apiErr *jetstream.APIError

	ok := errors.As(err, &apiErr)
	if !ok {
		return false
	}
	return apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence
}
