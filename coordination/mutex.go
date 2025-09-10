package coordination

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"github.com/rs/xid"
	"go.uber.org/zap"
)

var (
	ErrLockHeld               = errors.New("lock already held by this instance")
	ErrLockLost               = errors.New("lock ownership lost or expired")
	ErrPotentialDeadlock      = errors.New("potential deadlock detected: lock held too long")
	ErrKvBucketNoTTL          = errors.New("KV bucket must have a positive TTL to ensure eventual lock release")
	ErrLockAlreadyHeldByOther = errors.New("lock already held by another instance")
	ErrMaxRetriesExceeded     = errors.New("max retry attempts exceeded")
)

type Mutex struct {
	renewalCtx    context.Context    // Context for the renewal goroutine
	cancelRenewal context.CancelFunc // Function to cancel the renewal goroutine
	renewalWG     sync.WaitGroup

	kv              jetstream.KeyValue
	kvTTL           time.Duration
	key             string
	id              []byte
	idString        string
	renewalInterval time.Duration
	maxRetries      int           // Maximum number of retry attempts (0 = unlimited)
	maxLockDuration time.Duration // Maximum duration a lock can be held
	lockAcquiredAt  time.Time     // When the lock was acquired
	deadlockCheck   *time.Timer   // Timer for deadlock detection

	mu           sync.Mutex // Protects acquired, lockRevision, cancelRenewal, deadlockCheck, lockAcquiredAt
	acquired     bool
	lockRevision uint64

	rng    *rand.Rand
	logger common.Logger
}

// NewMutex creates a new distributed mutex.
//
//	kv: The NATS JetStream KeyValue store to use for the lock.
//	key: The unique identifier for this lock in the KV store.
//	renewalInterval: How often the lock should attempt to renew its lease. Must be > 0.
//	maxRetries: The maximum number of attempts to acquire the lock (0 for unlimited).
//	maxLockDuration: The maximum time an instance can hold the lock. If exceeded, a deadlock warning is logged.
//	                 A value of 0 means no maximum duration check.
//	logger: A logger instance for mutex operations.
func NewMutex(kv jetstream.KeyValue, key string, renewalInterval time.Duration, maxRetries int, maxLockDuration time.Duration, logger common.Logger) (*Mutex, error) {
	ctx := context.Background()

	if renewalInterval <= 0 {
		renewalInterval = 10 * time.Second // Default renewal interval
	}

	if maxRetries < 0 {
		maxRetries = 0 // Unlimited retries
	}
	if maxLockDuration < 0 {
		maxLockDuration = 0 // No max duration
	}

	st, err := kv.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get KV bucket status for key '%s': %w", key, err)
	}
	ttl := st.TTL()
	if ttl <= 0 {
		logger.Ctx(ctx).Sugar().Warnf(
			"KV bucket for key '%s' has no TTL; crashed holders may never release this lock", key,
		)
		return nil, ErrKvBucketNoTTL
	}

	id := xid.New()
	return &Mutex{
		kv:              kv,
		kvTTL:           ttl,
		key:             key,
		id:              id.Bytes(),
		idString:        id.String(),
		renewalInterval: renewalInterval,
		maxRetries:      maxRetries,
		maxLockDuration: maxLockDuration,
		// Using a mutex to protect rng if it were used by multiple goroutines concurrently.
		// For now, it's primarily used in Lock, so this is mostly defensive.
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
		logger: logger,
	}, nil
}

// TryLock attempts to acquire the lock immediately without waiting.
// Returns ErrLockHeld if this instance already holds the lock.
// Returns ErrLockAlreadyHeldByOther if another instance holds the lock.
// Returns other errors for NATS-related issues.
func (m *Mutex) TryLock(ctx context.Context, ttl time.Duration) error {
	m.mu.Lock()
	if m.acquired {
		m.mu.Unlock()
		return ErrLockHeld
	}
	m.mu.Unlock()

	// jetstream.KeyTTL(15*time.Minute)
	var opts []jetstream.KVCreateOpt
	if ttl > 0 {
		m.mu.Lock()
		m.maxLockDuration = ttl + ttl/10
		m.mu.Unlock()
		opts = append(opts, jetstream.KeyTTL(ttl))
	}

	rev, err := m.kv.Create(ctx, m.key, m.id, opts...)
	if err != nil {
		// Contention
		if errors.Is(err, jetstream.ErrKeyExists) {
			return ErrLockAlreadyHeldByOther
		}
		return fmt.Errorf("nats kv create failed during TryLock: %w", err)
	}
	l := m.logger.Ctx(ctx).Sugar()
	l.Debugf("TryLock acquired for key '%s' with revision %d (instance %s). TTL: %s", m.key, rev, m.idString, ttl)

	m.mu.Lock()
	defer m.mu.Unlock() // Use defer for immediate unlock

	m.acquired = true
	m.lockRevision = rev
	m.lockAcquiredAt = time.Now()

	m.setupRenewalAndDeadlockCheck() // Helper function for common setup
	return nil
}

// Lock attempts to acquire the lock, waiting and retrying with exponential backoff and jitter
// until the lock is acquired, the context is cancelled, or max retries are exceeded.
// Returns ErrLockHeld if this instance already holds the lock.
// Returns ErrMaxRetriesExceeded if max retries are hit.
// Returns context.Canceled or context.DeadlineExceeded if the provided context is done.
func (m *Mutex) Lock(ctx context.Context) error {
	m.logger.Ctx(ctx).Debug("starting lock acquisition attempt")
	m.mu.Lock()
	if m.acquired {
		m.mu.Unlock()
		return ErrLockHeld
	}
	m.mu.Unlock()

	l := m.logger.Ctx(ctx).Sugar()

	backoff := time.Millisecond * 50
	maxBackoff := time.Second * 2

	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	defer timer.Stop()

	retryCount := 0
	timer.Reset(0)

	for {
		if m.maxRetries > 0 && retryCount >= m.maxRetries {
			l.Warnf("Lock acquisition for key '%s' failed after %d attempts (instance %s)", m.key, retryCount, m.idString)
			return ErrMaxRetriesExceeded
		}

		select {
		case <-ctx.Done():
			cause := context.Cause(ctx)
			l.Debugf("Lock acquisition cancelled or timed out for key '%s' (instance %s): %v", m.key, m.idString, cause)
			return cause

		case <-timer.C:
			retryCount++

			rev, err := m.kv.Create(ctx, m.key, m.id)
			if err == nil {
				l.Debugf("Lock acquired for key '%s' with revision %d after %d attempts (instance %s)", m.key, rev, retryCount, m.idString)
				m.mu.Lock()
				defer m.mu.Unlock()
				m.acquired = true
				m.lockRevision = rev
				m.lockAcquiredAt = time.Now()
				m.setupRenewalAndDeadlockCheck()
				return nil
			}

			if ctxErr := context.Cause(ctx); ctxErr != nil {
				l.Warnf("Lock context cancelled or timed out for key '%s' during create attempt: %v", m.key, ctxErr)
				return ctxErr
			}

			if !errors.Is(err, jetstream.ErrKeyExists) {
				l.Errorf("Failed to attempt lock creation for key '%s' (instance %s): %v", m.key, m.idString, err)
				return fmt.Errorf("nats kv create failed: %w", err)
			}

			l.Debugf("Lock key '%s' exists, starting watch (instance %s)", m.key, m.idString)
			watcher, watchErr := m.kv.Watch(ctx, m.key)
			if watchErr != nil {
				l.Errorf("Failed to create watcher for key '%s' (instance %s): %v", m.key, m.idString, watchErr)

				// Check context error before returning the watch error
				if ctxErr := context.Cause(ctx); ctxErr != nil {
					return ctxErr
				}
				return fmt.Errorf("nats kv watch failed: %w", watchErr)
			}

			stopWatcher := func() {
				stopErr := watcher.Stop()
				if stopErr != nil &&
					!errors.Is(stopErr, nats.ErrTimeout) &&
					!errors.Is(stopErr, context.Canceled) &&
					!errors.Is(stopErr, context.DeadlineExceeded) {
					l.Warnf("Unexpected error stopping watcher for key '%s' (instance %s): %v", m.key, m.idString, stopErr)
				}
			}
			defer stopWatcher()

			var keyDeleted bool

			select {
			case <-ctx.Done():
				cause := context.Cause(ctx)
				l.Warnf("Lock attempt cancelled or timed out while watching key '%s' (instance %s): %v", m.key, m.idString, cause)
				return cause
			case update, ok := <-watcher.Updates():
				stopWatcher()
				if !ok {
					l.Warnf("Watcher channel closed unexpectedly for key '%s' (instance %s)", m.key, m.idString)
					keyDeleted = true
				} else if update == nil {
					l.Debugf("Watcher sentinel or no immediate delete observed for key '%s' (instance %s); applying backoff", m.key, m.idString)
					keyDeleted = false
				} else if update.Operation() == jetstream.KeyValueDelete || update.Operation() == jetstream.KeyValuePurge {
					l.Debugf("Watched key '%s' deleted or purged (Op: %s), attempting lock again (instance %s)", m.key, update.Operation(), m.idString)
					keyDeleted = true
				} else {
					l.Debugf("Watched key '%s' updated (Op: %s), retrying after backoff (instance %s)", m.key, update.Operation().String(), m.idString)
					keyDeleted = false
				}
			}

			if keyDeleted {
				timer.Reset(0)
			} else {
				backoff = minDuration(backoff*2, maxBackoff)
				jitter := time.Duration(float64(backoff) * (m.rng.Float64()*0.4 - 0.2))
				nextTry := backoff + jitter
				if nextTry <= 0 {
					nextTry = time.Millisecond * 10 // Ensure a minimum delay
				}
				timer.Reset(nextTry)
				// jitterFactor := time.Duration(m.rng.Int63n(int64(backoff) + 1))
				//if jitterFactor <= 0 {
				//	jitterFactor = 10 * time.Millisecond
				//}
				l.Debugf("Retrying lock acquisition for key '%s' after %v (attempt %d/%d, instance %s)",
					m.key, nextTry, retryCount, m.maxRetries, m.idString)
			}
		}
	}
}

// Unlock releases the acquired lock.
// It will attempt to delete the key from the KV store using the stored revision.
// If the key is not found or the revision mismatches, it logs a warning.
// Returns ErrLockLost if the lock was already lost (e.g., due to expiration or another instance stealing it).

func (m *Mutex) Unlock(ctx context.Context) error {
	m.mu.Lock()
	if !m.acquired {
		m.mu.Unlock()
		return nil // Not acquired, nothing to unlock
	}
	// Clean up internal state before attempting NATS delete
	if m.deadlockCheck != nil {
		m.deadlockCheck.Stop()
		m.deadlockCheck = nil
	}

	if m.cancelRenewal != nil {
		m.cancelRenewal()
		m.cancelRenewal = nil // Mark as cancelled
	}

	// Store current state for NATS call, then unlock mutex
	currentRevision := m.lockRevision
	m.mu.Unlock()

	l := m.logger.Ctx(ctx)
	l.Sugar().Debugf("Attempting to unlock key '%s' (instance %s, rev %d)", m.key, m.idString, currentRevision)

	done := make(chan struct{})
	go func() { m.renewalWG.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		// best-effort; don't block forever
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	var attempts int
	for attempts = 0; attempts < m.maxRetries; attempts++ {
		entry, getErr := m.kv.Get(ctx, m.key)
		if getErr != nil {
			if errors.Is(getErr, jetstream.ErrKeyNotFound) {
				l.Warn("already gone; call it unlocked.")
				m.acquired = false
				m.lockRevision = 0
				return nil
			}
			l.Error("failed get mutex key before delete", zap.Error(getErr))
			return getErr
		}

		if !bytes.Equal(entry.Value(), m.id) {
			l.Error("someone else owns it now")
			m.acquired = false
			m.lockRevision = 0
			return ErrLockLost
		}

		if err := m.kv.Delete(ctx, m.key, jetstream.LastRevision(entry.Revision())); err != nil {
			if errors.Is(err, jetstream.ErrKeyExists) {
				l.Warn("unlock failed, wrong last revision",
					zap.Error(err),
					zap.String("key", m.key),
					zap.String("id", m.idString),
					zap.Uint64("revision.current", currentRevision),
					zap.Uint64("revision.entry", entry.Revision()),
				)
				continue
			}
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				l.Sugar().Warnf("Unlock for key '%s' found key already deleted (or expired) (instance %s)", m.key, m.idString)
				break
			}
			l.Error("failed delete mutex error", zap.Error(err))
			continue
		} else {
			l.Sugar().Debugf("successfully unlocked key '%s' (instance %s)", m.key, m.idString)
			m.acquired = false
			m.lockRevision = 0
			return nil
		}
	}

	l.Sugar().Debugf("failed unlocked mutex key '%s' (instance %s) after %d attempts", m.key, m.idString, attempts)
	return nil
}

func (m *Mutex) CancelRenewal() error {
	m.mu.Lock()
	if m.cancelRenewal != nil {
		m.cancelRenewal()
		m.cancelRenewal = nil
	}
	m.mu.Unlock()
	return nil
}

// startRenewal is a goroutine that periodically attempts to renew the lock lease.
// It stops if its context is cancelled or if the lock is no longer marked as acquired by this instance.
func (m *Mutex) startRenewal(ctx context.Context) {
	l := m.logger.Ctx(ctx).Sugar()
	l.Debugf("startRenewal goroutine for key '%s' (instance %s) started", m.key, m.idString)

	// Determine the effective renewal tick duration. It should be significantly less than KV TTL.
	renewalTickDuration := m.renewalInterval / 3
	if renewalTickDuration <= 0 {
		renewalTickDuration = time.Second // Minimum tick duration
	}

	// Ensure renewal attempts are frequent enough relative to KV TTL
	if m.kvTTL > 0 {
		if d := m.kvTTL / 3; d > 0 && d < renewalTickDuration {
			renewalTickDuration = d
		}
	}
	ticker := time.NewTicker(renewalTickDuration)
	defer ticker.Stop()

	l.Debugf("Lock renewal routine (key '%s', instance %s) active (effective interval: %v, KV TTL: %v)", m.key, m.idString, renewalTickDuration, m.kvTTL)

	for {
		select {
		case <-ctx.Done():
			l.Debugf("Renewal routine for key '%s' (instance %s) stopping due to context cancellation: %v", m.key, m.idString, context.Cause(ctx))
			m.mu.Lock()
			// The `handlePotentialDeadlock` or `Unlock` would have already set acquired=false.
			// This is a final cleanup for this goroutine.
			m.cancelRenewal = nil // Ensure we don't try to cancel an already cancelled context again
			m.mu.Unlock()
			return
		case <-ticker.C:
			m.mu.Lock()
			if !m.acquired {
				m.mu.Unlock()
				l.Warnf("Renewal routine for key '%s' (instance %s) stopping: lock no longer marked as acquired.", m.key, m.idString)
				return
			}
			currentRevision := m.lockRevision
			ownerID := m.id
			m.mu.Unlock() // Unlock before NATS call

			l.Debugf("Renewing lock for key '%s' with revision %d (instance %s)", m.key, currentRevision, m.idString)
			newRev, err := m.kv.Update(ctx, m.key, ownerID, currentRevision)
			m.mu.Lock() // Re-lock to update state

			if !m.acquired {
				m.mu.Unlock()
				l.Warnf("Renewal routine for key '%s' (instance %s) stopping: lock no longer marked as acquired after NATS update attempt.", m.key, m.idString)
				return
			}

			if err == nil {
				m.lockRevision = newRev
				if newRev != currentRevision {
					l.Debugf("Lock successfully renewed for key '%s', new revision %d (instance %s)", m.key, newRev, m.idString)
				} else {
					l.Debugf("Lock renewal for key '%s' returned same revision %d (instance %s)", m.key, newRev, m.idString)
				}
				m.mu.Unlock()
				continue
			} else {
				if renewalErrCtx := context.Cause(ctx); renewalErrCtx != nil {
					l.Warnf("Renewal context for key '%s' cancelled during update attempt (instance %s): %v. Stopping renewal.", m.key, m.idString, renewalErrCtx)
					m.acquired = false
					if m.cancelRenewal != nil {
						m.cancelRenewal()
						m.cancelRenewal = nil
					}
					m.mu.Unlock()
					return
				}
				if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyExists) {
					errMsg := "key not found"
					if errors.Is(err, jetstream.ErrKeyExists) {
						errMsg = "revision mismatch"
					}
					l.Warnf("Renewal failed for key '%s': Lock lost (%s on update from %d, instance %s). Stopping renewal.", m.key, errMsg, currentRevision, m.idString)
					m.acquired = false
					if m.cancelRenewal != nil {
						m.cancelRenewal()
						m.cancelRenewal = nil
					}
					m.mu.Unlock()
					return
				} else {
					l.Warnf("Renewal for key '%s' failed with NATS error (revision %d, instance %s): %v. Will retry on next tick.", m.key, currentRevision, m.idString, err)
				}
			}
			m.mu.Unlock()
		}
	}
}

// IsAcquired returns true if this Mutex instance currently holds the lock.
func (m *Mutex) IsAcquired() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.acquired
}

// GetLockOwnerID returns the unique ID of this mutex instance.
func (m *Mutex) GetLockOwnerID() []byte {
	return m.id
}

// setupRenewalAndDeadlockCheck is a helper to encapsulate the common logic for starting
// the renewal goroutine and the deadlock detection timer.
// Assumes m.mu is locked by the caller.
func (m *Mutex) setupRenewalAndDeadlockCheck() {
	// If a previous renewal/deadlock check was active (e.g., from a failed Lock attempt), stop it.
	// This is defensive and might not be strictly needed if `acquired` state is always reset.
	if m.deadlockCheck != nil {
		m.deadlockCheck.Stop()
		m.deadlockCheck = nil
	}
	if m.cancelRenewal != nil {
		m.cancelRenewal()
		// Wait for the old goroutine to stop if necessary, though not strictly required if m.acquired becomes false.
	}

	if m.maxLockDuration > 0 {
		m.deadlockCheck = time.AfterFunc(m.maxLockDuration, func() {
			m.handlePotentialDeadlock(context.Background()) // Use background context for async timer callback
		})
	}

	// Use context.Background() for the renewal routine's root context.
	// Its lifetime is tied to the mutex itself, not a single API call.
	m.renewalCtx, m.cancelRenewal = context.WithCancel(context.Background())
	m.renewalWG.Add(1)
	go func() {
		defer m.renewalWG.Done()
		m.startRenewal(m.renewalCtx)
	}()
}

// handlePotentialDeadlock is called if the maxLockDuration is exceeded.
// It logs a warning and cancels the renewal context, effectively causing the lock to expire
// once the KV TTL is reached (if not renewed in time).
// This is a safer default than immediately forcing an unlock, which could lead to race conditions.
func (m *Mutex) handlePotentialDeadlock(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.acquired {
		return
	}

	lockHeldDuration := time.Since(m.lockAcquiredAt)
	l := m.logger.Ctx(ctx).Sugar()

	l.Warnf("Potential deadlock detected: Lock '%s' (instance %s) held for %v (exceeds max duration of %v). Cancelling renewal to allow lock to expire.",
		m.key, m.idString, lockHeldDuration, m.maxLockDuration)

	if m.cancelRenewal != nil {
		m.cancelRenewal()
		m.cancelRenewal = nil // Indicate it's been cancelled
	}
	m.acquired = false // Mark as lost from this instance's perspective
	m.lockRevision = 0
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
