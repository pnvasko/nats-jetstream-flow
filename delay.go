package nats_jetstream_flow

import "time"

const TermSignal = -1 * time.Nanosecond

type Delay interface {
	WaitTime(retryNum uint64) time.Duration
}
type StaticDelay struct {
	Delay time.Duration
}

func NewStaticDelay(delay time.Duration) StaticDelay {
	return StaticDelay{Delay: delay}
}

func (s StaticDelay) WaitTime(retryNum uint64) time.Duration {
	return s.Delay
}

var _ Delay = StaticDelay{}

type MaxRetryDelay struct {
	StaticDelay
	maxRetries uint64
}

func NewMaxRetryDelay(delay time.Duration, retryLimit uint64) MaxRetryDelay {
	return MaxRetryDelay{
		StaticDelay: NewStaticDelay(delay),
		maxRetries:  retryLimit,
	}
}

func (s MaxRetryDelay) WaitTime(retryNum uint64) time.Duration {
	if retryNum >= s.maxRetries {
		return TermSignal
	}
	return s.Delay
}

var _ Delay = MaxRetryDelay{}
