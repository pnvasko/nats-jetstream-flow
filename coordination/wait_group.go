package coordination

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"go.opentelemetry.io/otel/trace"
	"strconv"
	"sync"
	"time"
)

const (
	defaultWaitGroupBucketPrefix = "wait_group"
	defaultWaitGroupScope        = "default"
	defaultCleanupTTL            = 7 * 24 * time.Hour
)

type WaitGroup struct {
	*baseKvStore
	ctx    context.Context
	cancel context.CancelFunc
	ctxMu  sync.RWMutex

	mu sync.Mutex // Protects RMW cycle within a single client instance
	js jetstream.JetStream
	kv jetstream.KeyValue

	// jetstream.Stream

	tracer trace.Tracer
	logger *common.Logger
}

func NewWaitGroup(ctx context.Context, js jetstream.JetStream, tracer trace.Tracer, logger *common.Logger, opts ...StoreOption[*WaitGroup]) (*WaitGroup, error) {
	w := &WaitGroup{
		baseKvStore: &baseKvStore{
			scope: defaultWaitGroupScope,
			// bucketName:       fmt.Sprintf("%s_%s", defaultWaitGroupBucketPrefix, defaultWaitGroupScope),
			retryWait:        defaultRetryWait,
			maxRetryAttempts: defaultMaxRetryAttempts,
			cleanupTTL:       defaultCleanupTTL,
		},
		js:     js,
		tracer: tracer,
		logger: logger,
	}
	w.ctx, w.cancel = context.WithCancel(ctx)

	for _, opt := range opts {
		if err := opt(w); err != nil {
			return nil, err
		}
	}

	if w.bucketName == "" {
		w.bucketName = fmt.Sprintf("%s_%s", defaultWaitGroupBucketPrefix, w.scope)
	}

	if w.cleanupTTL >= 0 && w.cleanupTTL <= time.Duration(100)*time.Millisecond {
		return nil, fmt.Errorf("cleanup TTL must be larger than 100ms")
	}

	keyValueConfig := jetstream.KeyValueConfig{
		Bucket:      w.bucketName,
		Description: "Distributed wait group state for " + w.scope,
	}

	if w.cleanupTTL >= 0 {
		keyValueConfig.TTL = w.cleanupTTL
	}

	kv, err := js.CreateKeyValue(w.ctx, keyValueConfig)
	if err == nil {
		w.kv = kv
		return w, nil
	}

	if isJSAlreadyExistsError(err) {
		kv, err = js.KeyValue(w.ctx, w.bucketName)
		if err != nil {
			return nil, fmt.Errorf("failed to bind to existing KV store '%s': %s", w.bucketName, err.Error())
		}
		w.kv = kv
		return w, nil
	} else {
		return nil, fmt.Errorf("failed to create/get KV store '%s': %s", w.bucketName, err.Error())
	}
}

func (w *WaitGroup) Scope() string {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.scope
}

func (w *WaitGroup) Bucket() string {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.bucketName
}

func (w *WaitGroup) Context() context.Context {
	w.ctxMu.RLock()
	defer w.ctxMu.RUnlock()
	return w.ctx
}

func (w *WaitGroup) Add(ctx context.Context, groupId string, delta int) error {
	if delta == 0 {
		return nil
	}

	if groupId == "" {
		return fmt.Errorf("group name cannot be empty")
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	var lastErr error

	for i := 0; i < w.maxRetryAttempts; i++ {
		newVal, revision, err := w.getNewValue(ctx, groupId, delta)
		if err != nil {
			return err
		}

		var updateErr error
		if revision == 0 {
			_, updateErr = w.kv.Create(ctx, groupId, newVal)
			// _, updateErr = w.kv.Put(ctx, w.groupId, newVal)
		} else {
			_, updateErr = w.kv.Update(ctx, groupId, newVal, revision)
		}

		lastErr = updateErr
		if updateErr == nil {
			return nil
		}
		if isJSWrongLastSequence(updateErr) {
			select {
			case <-time.After(w.retryWait):
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		return fmt.Errorf("waitgroup add failed on attempt %d: %w", i+1, updateErr)
	}
	return fmt.Errorf("waitgroup add failed after %d attempts due to conflicts: last error: %s", w.maxRetryAttempts, lastErr.Error())
}

func (w *WaitGroup) Done(ctx context.Context, groupId string) error {
	return w.Add(ctx, groupId, -1)
}

func (w *WaitGroup) Wait(ctx context.Context, groupId string) error {
	if groupId == "" {
		return fmt.Errorf("group name cannot be empty")
	}

	current, _, err := w.getState(ctx, groupId)
	if err != nil {
		w.logger.Ctx(w.ctx).Sugar().Errorf("failed get group state, group id '%s': %s", groupId, err.Error())
		return err
	}
	if current == 0 {
		return nil
	}

	var watcher jetstream.KeyWatcher

	defer func() {
		if watcher != nil {
			if err := watcher.Stop(); err != nil { // Best effort stop
				w.logger.Ctx(context.Background()).Sugar().Errorf("failed to stop WaitGroup watcher: %s", err.Error())
			}
			watcher = nil
		}
	}()

	for {
		if watcher == nil {
			watcher, err = w.kv.Watch(w.ctx, groupId, jetstream.UpdatesOnly())
			if err != nil {
				select {
				case <-w.Context().Done():
					return w.ctx.Err()
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				if errors.Is(err, jetstream.ErrBucketNotFound) || errors.Is(err, jetstream.ErrKeyNotFound) {
					time.Sleep(w.retryWait)
					continue
				}
				return fmt.Errorf("failed to watch WaitGroup state for '%s': %s", groupId, err.Error())
			}
		}
		select {
		case update, ok := <-watcher.Updates():
			if !ok {
				w.logger.Ctx(ctx).Sugar().Warnf("Wait [%s]: Watcher updates channel closed unexpectedly.", groupId)
				time.Sleep(w.retryWait)
				watcher = nil
				continue
			}
			w.logger.Ctx(ctx).Sugar().Debugf("Wait [%s]: Watcher received update: Op=%s, Rev=%d. Looping to retry Wait.", groupId, update.Operation(), update.Value())
			if string(update.Value()) == "0" {
				return nil
			}
			continue
		case <-w.Context().Done():
			return w.ctx.Err()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (w *WaitGroup) Close(ctx context.Context, groupId string) error {
	w.mu.Lock() // Ensure no other operations are mid-RMW
	defer w.mu.Unlock()

	if groupId == "" {
		return fmt.Errorf("group name cannot be empty")
	}

	// Use Purge to remove the key, ignoring not found errors
	err := w.kv.Purge(ctx, groupId)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to delete wait group state key '%s': %w", groupId, err)
	}

	return nil
}

func (w *WaitGroup) Delete(ctx context.Context) error {
	w.mu.Lock() // Ensure no other operations are mid-RMW
	defer w.mu.Unlock()

	err := w.js.DeleteKeyValue(ctx, w.bucketName)
	if err != nil {
		return fmt.Errorf("failed to delete wait group bucket '%s': %w", w.bucketName, err)
	}
	return nil
}

func (w *WaitGroup) getNewValue(ctx context.Context, groupId string, delta int) ([]byte, uint64, error) {
	current, revision, err := w.getState(ctx, groupId)
	if err != nil {
		return nil, 0, err
	}

	newVal := current + delta
	if newVal < 0 {
		return nil, 0, fmt.Errorf("negative value for group id '%s'", groupId)
	}

	return []byte(strconv.Itoa(newVal)), revision, nil
}

func (w *WaitGroup) getState(ctx context.Context, groupId string) (int, uint64, error) {
	entry, err := w.kv.Get(ctx, groupId)

	if err != nil {
		if errors.Is(err, jetstream.ErrKeyDeleted) {
			fmt.Println("getState.ErrKeyDeleted: ", err)
		}
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			// Key not found: Initialize state. Revision is 0 for Create.
			// try get revision from history
			// getRevisionFromHistory
			return 0, 0, nil
		}
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return 0, 0, fmt.Errorf("wait group bucket '%s' not found: %w", w.bucketName, err)
		}
		// Other error (network, context cancelled)
		return 0, 0, fmt.Errorf("failed to get wait group state: %w", err)
	}

	value, err := strconv.Atoi(string(entry.Value()))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse value for wait group: %w", err)
	}

	return value, entry.Revision(), nil
}

func (w *WaitGroup) getRevisionFromHistory(ctx context.Context, groupId string) (uint64, error) {
	history, err := w.kv.History(ctx, groupId)
	if err != nil {
		return 0, err
	}
	var revision uint64
	for _, v := range history {
		if v.Revision() > revision {
			revision = v.Revision()
		}
	}
	return revision, nil
}

var _ storeOptionScopable = (*WaitGroup)(nil)

// var _ StoreOption[*WaitGroup] = WithScope[*WaitGroup]("test")
