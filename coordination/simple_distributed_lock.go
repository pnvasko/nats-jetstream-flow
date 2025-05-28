package coordination

import (
	"context"
	"errors"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"time"
)

type DistributedLock struct {
	js        jetstream.JetStream
	subject   string
	consumer  string
	leaseTime time.Duration
	cancel    context.CancelFunc
}

func NewDistributedLock(js jetstream.JetStream, subject, consumer string, lease time.Duration) *DistributedLock {
	return &DistributedLock{
		js:        js,
		subject:   subject,
		consumer:  consumer,
		leaseTime: lease,
	}
}

func (l *DistributedLock) TryLock(ctx context.Context) error {
	cfg := jetstream.ConsumerConfig{
		Durable:       l.consumer,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       l.leaseTime,
		FilterSubject: l.subject,
		MaxDeliver:    1,
	}

	_, err := l.js.CreateOrUpdateConsumer(ctx, l.subject, cfg)
	if err == nil {
		// Lock acquired
		// Start keep-alive to periodically ack or renew lock
		ctx, cancel := context.WithCancel(ctx)
		l.cancel = cancel
		go l.keepAlive(ctx)
		return nil
	}

	// If consumer exists, lock is held
	if errors.Is(err, nats.ErrConsumerNameAlreadyInUse) {
		return errors.New("lock is already held")
	}

	return err
}

// keepAlive sends periodic acks or heartbeats to maintain the lease lock.
func (l *DistributedLock) keepAlive(ctx context.Context) {
	ticker := time.NewTicker(l.leaseTime / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// TODO: Implement logic to ack or heartbeat the consumer
			// to renew the lease.
			// This is simplified — in practice you’d fetch messages and ack them.
		}
	}
}

// Unlock releases the lock by deleting the consumer.
func (l *DistributedLock) Unlock(ctx context.Context) error {
	if l.cancel != nil {
		l.cancel()
	}
	return l.js.DeleteConsumer(ctx, l.subject, l.consumer)
}
