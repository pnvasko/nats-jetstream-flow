package coordination

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pnvasko/nats-jetstream-flow/common"
	proto "github.com/pnvasko/nats-jetstream-flow/proto/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultObjectStoreName         = "object_store"
	defaultObjectStoreScope        = "object"
	defaultObjectStoreBucketPrefix = "store"
)

type Object interface {
	Label(params any) (string, error)
	SpanName() string
	Empty() ([]byte, error)
	MarshalVT() ([]byte, error)
	UnmarshalVT([]byte) error
	Update(any) error
}

type LabelParams struct {
	Label  string
	Params any
}
type labelMutex struct {
	mu       sync.Mutex
	refCount int64
	// Store time.Time as UnixNano for atomic operations
	// lastUse atomic.Int64
	// Consider adding a mechanism to clean up unused mutexes from the map, perhaps based on a least-recently-used (LRU) policy or a periodic sweep using the commented-out lastUse timestamp idea.
}

func (m *labelMutex) Lock() {
	m.mu.Lock()
}

func (m *labelMutex) Unlock() {
	m.mu.Unlock()
}

type ObjectStore[T Object, R any] struct {
	*baseKvStore
	ctx    context.Context
	cancel context.CancelFunc
	ctxMu  sync.RWMutex

	mu        sync.RWMutex // Protects RMW cycle within a single client instance
	onceStart sync.Once
	closing   chan struct{}

	watchers        map[string]jetstream.KeyWatcher
	watcherHandlers map[string]map[context.Context]func(T)

	mutexLock   sync.Mutex
	mutexObject map[string]*labelMutex

	js jetstream.JetStream
	kv jetstream.KeyValue

	objectFactory func() T

	spanNameUpdate string
	spanNameRead   string
	spanNameReset  string

	tracer trace.Tracer
	logger *common.Logger
}

func NewObjectStore[T Object, UpdateInput any](ctx context.Context,
	js jetstream.JetStream,
	factory func() T,
	tracer trace.Tracer,
	logger *common.Logger,
	opts ...StoreOption[*ObjectStore[T, UpdateInput]],
) (*ObjectStore[T, UpdateInput], error) {
	object := factory()
	spanName := object.SpanName()
	m := &ObjectStore[T, UpdateInput]{
		baseKvStore: &baseKvStore{
			scope:            defaultObjectStoreScope,
			retryWait:        defaultRetryWait,
			maxRetryAttempts: defaultMaxRetryAttempts,
			cleanupTTL:       defaultCleanupTTL,
		},
		js:              js,
		watchers:        make(map[string]jetstream.KeyWatcher),
		watcherHandlers: make(map[string]map[context.Context]func(T)),

		objectFactory: factory,
		mutexObject:   make(map[string]*labelMutex),

		tracer: tracer,
		logger: logger,
	}
	m.ctx, m.cancel = context.WithCancel(ctx)

	for _, opt := range opts {
		if err := opt(m); err != nil {
			return nil, err
		}
	}

	if m.bucketName == "" {
		m.bucketName = fmt.Sprintf("%s_%s", defaultObjectStoreBucketPrefix, m.scope)
	}

	keyValueConfig := jetstream.KeyValueConfig{
		Bucket:      m.bucketName,
		Description: fmt.Sprintf("Distributed objects for %s", m.scope),
	}

	if m.cleanupTTL >= 0 {
		keyValueConfig.TTL = m.cleanupTTL
	}

	m.spanNameUpdate = fmt.Sprintf("object_store.%s.%s.update", m.scope, spanName)
	m.spanNameRead = fmt.Sprintf("object_store.%s.%s.read", m.scope, spanName)
	m.spanNameReset = fmt.Sprintf("object_store.%s.%s.reset", m.scope, spanName)

	kv, err := js.CreateKeyValue(m.ctx, keyValueConfig)
	if err == nil {
		m.kv = kv
		return m, nil
	}

	if isJSAlreadyExistsError(err) {
		kv, err = js.KeyValue(m.ctx, m.bucketName)
		if err != nil {
			return nil, fmt.Errorf("failed to bind to existing KV store '%s': %w", m.bucketName, err)
		}
		m.kv = kv
	} else {
		return nil, fmt.Errorf("failed to create/get KV store '%s': %w", m.bucketName, err)
	}
	return m, nil
}

func (m *ObjectStore[T, R]) JS() jetstream.JetStream {
	return m.js
}

func (m *ObjectStore[T, R]) KV() jetstream.KeyValue {
	return m.kv
}

func (m *ObjectStore[T, R]) Update(ctx context.Context, params *LabelParams, values R) error {
	label, err := m.resolveLabel(params) // Use the new resolver
	if err != nil {
		return fmt.Errorf("failed to resolve label: %w", err) // Add context to the error
	}

	ctx, updateSpan := m.tracer.Start(ctx, m.spanNameUpdate,
		trace.WithAttributes(attribute.String("label", label)),
		trace.WithAttributes(attribute.Key("service").String(defaultObjectStoreName)),
	)
	defer updateSpan.End()

	lm := m.getLabelMutex(label) // This now returns *labelMutex
	lm.mu.Lock()
	defer func(lm *labelMutex) {
		lm.mu.Unlock()
		m.releaseLabelMutex(label)
	}(lm)

	var lastErr error
	for i := 0; i < m.maxRetryAttempts; i++ {
		newVal, revision, err := m.getNewValue(ctx, label, values)
		if err != nil {
			return err
		}
		var updateErr error
		if revision == 0 {
			_, updateErr = m.kv.Create(ctx, label, newVal)
		} else {
			_, updateErr = m.kv.Update(ctx, label, newVal, revision)
		}
		lastErr = updateErr

		if updateErr == nil {
			return nil
		}
		if isJSWrongLastSequence(updateErr) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-m.ctx.Done():
				return m.ctx.Err()
			case <-time.After(m.retryWait):
				continue
			}
		}
		return fmt.Errorf("objects incr failed on attempt %d: %w", i+1, updateErr)
	}
	return fmt.Errorf("objects incr failed after %d attempts due to conflicts: last error: %w", m.maxRetryAttempts, lastErr)
}

func (m *ObjectStore[T, R]) Read(ctx context.Context, params *LabelParams) (T, error) {
	var zero T
	label, err := m.resolveLabel(params) // Use the new resolver
	if err != nil {
		return zero, fmt.Errorf("failed to resolve label: %w", err) // Add context to the error
	}

	ctx, readSpan := m.tracer.Start(ctx, m.spanNameRead,
		trace.WithAttributes(attribute.String("label", label)),
		trace.WithAttributes(attribute.Key("service").String(defaultObjectStoreName)),
	)
	defer readSpan.End()

	lm := m.getLabelMutex(label)
	lm.mu.Lock()
	defer func(lm *labelMutex) {
		lm.mu.Unlock()
		m.releaseLabelMutex(label)
	}(lm)

	entry, err := m.kv.Get(ctx, label)
	if err != nil {
		return zero, err
	}

	var object T = m.objectFactory()
	err = object.UnmarshalVT(entry.Value())

	if err != nil {
		return zero, fmt.Errorf("failed to unmarshal object state: %w", err)
	}

	return object, nil
}

func (m *ObjectStore[T, R]) Reset(ctx context.Context, params *LabelParams) error {
	label, err := m.resolveLabel(params) // Use the new resolver
	if err != nil {
		return fmt.Errorf("failed to resolve label: %w", err) // Add context to the error
	}

	ctx, resetSpan := m.tracer.Start(ctx, m.spanNameReset,
		trace.WithAttributes(attribute.String("label", label)),
		trace.WithAttributes(attribute.Key("service").String(defaultObjectStoreName)),
	)
	defer resetSpan.End()

	lm := m.getLabelMutex(label)
	lm.mu.Lock()
	defer func(lm *labelMutex) {
		lm.mu.Unlock()
		m.releaseLabelMutex(label)
	}(lm)

	var object T = m.objectFactory()
	emptyMetricVal, err := object.Empty()
	if err != nil {
		return fmt.Errorf("failed to create empty object: %w", err)
	}
	var lastErr error
	for i := 0; i < m.maxRetryAttempts; i++ {
		entry, err := m.kv.Get(ctx, label)
		var revision uint64

		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
				revision = 0
			} else if errors.Is(err, jetstream.ErrBucketNotFound) {
				return fmt.Errorf("bucket object store '%s' not found: %w", m.bucketName, err)
			} else {
				return fmt.Errorf("failed to get object store state: %w", err)
			}
		} else {
			revision = entry.Revision()
		}

		var updateErr error
		if revision == 0 {
			_, updateErr = m.kv.Create(ctx, label, emptyMetricVal)
		} else {
			_, updateErr = m.kv.Update(ctx, label, emptyMetricVal, revision)
		}
		lastErr = updateErr
		if updateErr == nil {
			return nil
		}
		if isJSWrongLastSequence(updateErr) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-m.ctx.Done():
				return m.ctx.Err()
			case <-time.After(m.retryWait):
				continue
			}
		}
		return fmt.Errorf("object reset failed on attempt %d: %w", i+1, updateErr)
	}

	return fmt.Errorf("object reset failed after %d attempts due to conflicts: last error: %w", m.maxRetryAttempts, lastErr)
}

func (m *ObjectStore[T, R]) Watch(ctx context.Context, params *LabelParams, fn func(string, T)) error {
	label, err := m.resolveLabel(params)
	if err != nil {
		return fmt.Errorf("failed to resolve label: %w", err)
	}
	var watcher jetstream.KeyWatcher
	defer func() {
		if watcher != nil {
			if err := watcher.Stop(); err != nil && isJSConsumerNotFound(err) {
				logCtx := m.Context()
				if logCtx == nil || logCtx.Err() != nil {
					logCtx = context.Background()
				}
				m.logger.Ctx(logCtx).Sugar().Errorf("failed to stop watcher [%s]: %v", label, err)
			}
		}
	}()

	for {
		if watcher == nil {
			watcher, err = m.kv.Watch(ctx, label)
			if err != nil {
				select {
				case <-m.Context().Done():
					return m.ctx.Err()
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				if errors.Is(err, jetstream.ErrBucketNotFound) || errors.Is(err, jetstream.ErrKeyNotFound) {
					m.logger.Ctx(ctx).Sugar().Warnf("watcher [%s] not found (bucket or key), retrying: %v", label, err)
					if err := m.sleepWithContext(ctx); err != nil {
						return err
					}
					continue
				}
				return fmt.Errorf("failed to watch state for '%s': %w", label, err)
			}
		}

		select {
		case entry, ok := <-watcher.Updates():
			if !ok {
				m.logger.Ctx(ctx).Sugar().Warnf("watcher [%s] updates channel closed unexpectedly.", label)
				if err := m.sleepWithContext(ctx); err != nil {
					return err
				}
				watcher = nil
				continue
			}
			if entry != nil {
				m.logger.Ctx(ctx).Sugar().Debugf("watcher [%s] received update: op=%s, looping to retry wait.", label, entry.Operation())
				var object T = m.objectFactory()
				if entry.Operation() == jetstream.KeyValueDelete {
					m.logger.Ctx(ctx).Sugar().Debugf("watcher [%s] received delete operation.", label)
					continue
				}
				if len(entry.Value()) == 0 && entry.Operation() != jetstream.KeyValuePut {
					m.logger.Ctx(ctx).Sugar().Debugf("watcher [%s] received non-PUT entry with empty value, skipping unmarshal.", label)
					continue
				}
				err = object.UnmarshalVT(entry.Value())
				if err != nil {
					m.logger.Ctx(ctx).Sugar().Errorf("failed to unmarshal object state, key: [%s], type: [%T]. error: %v", label, object, err)
					continue
				}
				fn(entry.Key(), object)
			} else {
				m.logger.Ctx(ctx).Sugar().Debugf("watcher [%s] received nil entry.", label)
				continue
			}
		case <-m.Context().Done():
			return m.Context().Err()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (m *ObjectStore[T, R]) sleepWithContext(ctx context.Context) error {
	select {
	case <-time.After(m.retryWait):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *ObjectStore[T, R]) Delete(ctx context.Context, params *LabelParams) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	label, err := m.resolveLabel(params) // Use the new resolver
	if err != nil {
		return fmt.Errorf("failed to resolve label: %w", err) // Add context to the error
	}
	return m.kv.Delete(ctx, label)
}

func (m *ObjectStore[T, R]) DeleteBucket(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	err := m.js.DeleteKeyValue(ctx, m.bucketName)
	if err != nil {
		return fmt.Errorf("failed to destroy objects bucket '%s': %w", m.bucketName, err)
	}
	return nil
}

func (m *ObjectStore[T, R]) Scope() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.scope
}

func (m *ObjectStore[T, R]) Bucket() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.bucketName
}

func (m *ObjectStore[T, R]) Context() context.Context {
	m.ctxMu.RLock()
	defer m.ctxMu.RUnlock()
	return m.ctx
}

func (m *ObjectStore[T, R]) Close() {
	m.cancel()
}

func (m *ObjectStore[T, R]) getLabelMutex(label string) *labelMutex {
	m.mutexLock.Lock()
	defer m.mutexLock.Unlock()

	lm, ok := m.mutexObject[label]
	if !ok {
		lm = &labelMutex{}
		m.mutexObject[label] = lm
	}
	atomic.AddInt64(&lm.refCount, 1)
	return lm
}

func (m *ObjectStore[T, R]) releaseLabelMutex(label string) {
	m.mutexLock.Lock()
	defer m.mutexLock.Unlock()

	if lm, ok := m.mutexObject[label]; ok {
		if atomic.AddInt64(&lm.refCount, -1) == 0 {
			delete(m.mutexObject, label)
		}
	}
}

func (m *ObjectStore[T, R]) getNewValue(ctx context.Context, label string, values R) ([]byte, uint64, error) {
	var entryValue []byte
	var revision uint64
	entry, err := m.kv.Get(ctx, label)
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		if errors.Is(err, jetstream.ErrKeyDeleted) {
			m.logger.Ctx(ctx).Sugar().Warnf("key [%s] deleted: %s", label, err.Error())
		}

		if errors.Is(err, jetstream.ErrKeyNotFound) {
			var emptyMetric T = m.objectFactory()
			emptyRecord, emptyRecordError := emptyMetric.Empty()
			if emptyRecordError != nil {
				return nil, 0, fmt.Errorf("failed to create empty record: %w", emptyRecordError)
			}
			return emptyRecord, 0, nil
		}
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return nil, 0, fmt.Errorf("objects bucket '%s' not found: %w", m.bucketName, err)
		}
	} else if errors.Is(err, jetstream.ErrKeyNotFound) {
		var emptyRecordError error
		var emptyMetric T = m.objectFactory()
		revision = uint64(0)
		entryValue, emptyRecordError = emptyMetric.Empty()
		if emptyRecordError != nil {
			return nil, 0, fmt.Errorf("failed to create empty record: %w", emptyRecordError)
		}
	} else {
		entryValue = entry.Value()
		revision = entry.Revision()
	}

	var object T = m.objectFactory()
	err = object.UnmarshalVT(entryValue)
	if err != nil {
		obj := &proto.BaseMetric{}
		baseMetricErr := obj.UnmarshalVT(entryValue)
		if baseMetricErr != nil {
			fmt.Printf("baseMetricErr: %s, %w\n", label, baseMetricErr)
		} else {
			fmt.Printf("it is BaseMetric:%s, %+v\n", label, obj)
		}
		return nil, 0, fmt.Errorf("failed to unmarshal object key: [%s]; type: [%T]; len [%d] state: %w", label, object, len(entryValue), err)
	}

	if err := object.Update(values); err != nil {
		return nil, 0, fmt.Errorf("failed to update objects state: %w", err)
	}
	newVal, err := object.MarshalVT()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to marshal objects state: %w", err)
	}

	return newVal, revision, nil
}

func (m *ObjectStore[T, R]) resolveLabel(params *LabelParams) (string, error) {
	if params == nil {
		return "", fmt.Errorf("label params cannot be nil")
	}

	if params.Label != "" {
		return params.Label, nil // Direct label takes precedence
	}

	obj := m.objectFactory()
	labelFromParams, err := obj.Label(params.Params)
	if err != nil {
		return "", fmt.Errorf("failed to derive label: %w", err)
	}

	if labelFromParams == "" {
		return "", fmt.Errorf("derived label cannot be empty")
	}

	return labelFromParams, nil
}

//
//func (m *ObjectStore[T, R]) todoLabelWatch(ctx context.Context, params *LabelParams, fn func(T)) error {
//	label, err := m.resolveLabel(params) // Use the new resolver
//	if err != nil {
//		return fmt.Errorf("failed to resolve label: %w", err) // Add context to the error
//	}
//	m.mu.Lock()
//	if existingWatcher, ok := m.watchers[label]; ok && existingWatcher != nil {
//		m.mu.Unlock()
//		return fmt.Errorf("watcher already exists for label: %s", label)
//		// Option: Return something else (e.g., a channel to signal stop)
//		// return &ExistingWatcherError{Label: label} // Define a specific error type
//	}
//	m.watchers[label] = nil
//	m.mu.Unlock()
//	defer func() {
//		m.mu.Lock()
//		watcher := m.watchers[label]
//		m.watchers[label] = nil
//		m.mu.Unlock()
//		if watcher != nil {
//			if err := watcher.Stop(); err != nil && isJSConsumerNotFound(err) {
//				logCtx := m.Context()
//				if logCtx == nil || logCtx.Err() != nil { // Fallback if store ctx is done
//					logCtx = context.Background()
//				}
//				m.logger.Ctx(logCtx).Sugar().Errorf("failed to stop watcher [%s]: %s", label, err.Error())
//			}
//		}
//	}()
//
//	for {
//		var currentWatcher jetstream.KeyWatcher
//		m.mu.Lock()
//		currentWatcher = m.watchers[label] // Get the current watcher assigned to this label
//		m.mu.Unlock()
//		fmt.Printf("currentWatcher: %s: %+v\n", label, currentWatcher)
//		if currentWatcher == nil {
//			var newWatcher jetstream.KeyWatcher
//			newWatcher, err = m.kv.Watch(ctx, label)
//			if err != nil {
//				select {
//				case <-m.Context().Done(): // Check store context
//					return m.Context().Err()
//				case <-ctx.Done(): // Check watch context
//					return ctx.Err()
//				default:
//				}
//				if errors.Is(err, jetstream.ErrBucketNotFound) || errors.Is(err, jetstream.ErrKeyNotFound) {
//					m.logger.Ctx(ctx).Sugar().Warnf("watcher [%s] not found (bucket or key), retrying: %s", label, err.Error())
//					if err := m.sleepWithContext(ctx); err != nil {
//						return err
//					}
//				}
//				return fmt.Errorf("failed to watch state for '%s': %s", label, err.Error())
//			}
//
//			m.mu.Lock()
//			if m.watchers[label] == nil {
//				m.watchers[label] = newWatcher
//				currentWatcher = newWatcher // Update the local variable for the select
//			} else {
//				m.mu.Unlock()
//				if stopErr := newWatcher.Stop(); stopErr != nil {
//					m.logger.Ctx(context.Background()).Sugar().Errorf("failed to stop redundant watcher for [%s]: %s", label, stopErr.Error())
//				}
//				continue
//			}
//			m.mu.Unlock()
//		}
//		select {
//		case entry, ok := <-currentWatcher.Updates():
//			if !ok {
//				m.logger.Ctx(ctx).Sugar().Warnf("watcher [%s] updates channel closed unexpectedly.", label)
//				if err := m.sleepWithContext(ctx); err != nil {
//					return err
//				}
//				m.mu.Lock()
//				m.watchers[label] = nil
//				m.mu.Unlock()
//				continue
//			}
//			if entry != nil {
//				m.logger.Ctx(ctx).Sugar().Debugf("watcher [%s] received update: op=%s, looping to retry wait.", label, entry.Operation())
//				var object T = m.objectFactory()
//				if entry.Operation() == jetstream.KeyValueDelete {
//					m.logger.Ctx(ctx).Sugar().Debugf("watcher [%s] received delete operation.", label)
//					fn(object)
//					continue
//				}
//				if len(entry.Value()) == 0 && entry.Operation() != jetstream.KeyValuePut {
//					m.logger.Ctx(ctx).Sugar().Debugf("watcher [%s] received non-PUT entry with empty value, skipping unmarshal.", label)
//					continue
//				}
//				err = object.UnmarshalVT(entry.Value())
//				if err != nil {
//					m.logger.Ctx(ctx).Sugar().Warnf("failed to unmarshal state key [%s] object to [%T]: %s", entry.Key(), object, err.Error())
//					// m.logger.Ctx(ctx).Sugar().Errorf("failed to unmarshal state key [%s] object to [%T]: %s", entry.Key(), object, err.Error())
//					continue
//				}
//				fn(object)
//			} else {
//				m.logger.Ctx(ctx).Sugar().Debugf("watcher [%s] received nil entry.", label)
//			}
//		case <-m.Context().Done():
//			return m.Context().Err()
//		case <-ctx.Done():
//			return ctx.Err()
//		}
//	}
//}

//func (m *ObjectStore[T, R]) todoManyWatcherWatch(ctx context.Context, params *LabelParams, fn func(T)) error {
//	label, err := m.resolveLabel(params) // Use the new resolver
//	if err != nil {
//		return fmt.Errorf("failed to resolve label: %w", err) // Add context to the error
//	}
//	m.mu.Lock()
//	if existingWatcher, ok := m.watchers[label]; ok && existingWatcher != nil {
//		if _, ok := m.watcherHandlers[label]; !ok {
//			m.watcherHandlers[label] = map[context.Context]func(T){}
//		}
//		if _, ok := m.watcherHandlers[label][ctx]; !ok {
//			m.watcherHandlers[label][ctx] = fn
//		}
//		m.mu.Unlock()
//		return fmt.Errorf("watcher already exists for label: %s", label)
//		// Option: Return something else (e.g., a channel to signal stop)
//		// return &ExistingWatcherError{Label: label} // Define a specific error type
//	}
//	m.watchers[label] = nil
//	if _, ok := m.watcherHandlers[label]; !ok {
//		m.watcherHandlers[label] = map[context.Context]func(T){}
//	}
//	if _, ok := m.watcherHandlers[label][ctx]; !ok {
//		m.watcherHandlers[label][ctx] = fn
//	}
//	m.mu.Unlock()
//
//	defer func() {
//		m.mu.Lock()
//		watcher := m.watchers[label]
//		m.watchers[label] = nil
//		m.watcherHandlers[label] = map[context.Context]func(T){}
//		m.mu.Unlock()
//
//		if watcher != nil {
//			if err := watcher.Stop(); err != nil && isJSConsumerNotFound(err) {
//				logCtx := m.Context()
//				if logCtx == nil || logCtx.Err() != nil { // Fallback if store ctx is done
//					logCtx = context.Background()
//				}
//				m.logger.Ctx(logCtx).Sugar().Errorf("failed to stop watcher [%s]: %s", label, err.Error())
//			}
//		}
//	}()
//
//	for {
//		var currentWatcher jetstream.KeyWatcher
//		m.mu.Lock()
//		currentWatcher = m.watchers[label] // Get the current watcher assigned to this label
//		m.mu.Unlock()
//
//		if currentWatcher == nil {
//			var newWatcher jetstream.KeyWatcher
//			newWatcher, err = m.kv.Watch(ctx, label)
//			if err != nil {
//				select {
//				case <-m.Context().Done(): // Check store context
//					return m.Context().Err()
//				case <-ctx.Done(): // Check watch context
//					return ctx.Err()
//				default:
//				}
//				if errors.Is(err, jetstream.ErrBucketNotFound) || errors.Is(err, jetstream.ErrKeyNotFound) {
//					m.logger.Ctx(ctx).Sugar().Warnf("watcher [%s] not found (bucket or key), retrying: %s", label, err.Error())
//					time.Sleep(m.retryWait)
//					continue // Retry watch creation
//				}
//				return fmt.Errorf("failed to watch state for '%s': %s", label, err.Error())
//			}
//
//			m.mu.Lock()
//			if m.watchers[label] == nil {
//				m.watchers[label] = newWatcher
//				currentWatcher = newWatcher // Update the local variable for the select
//			} else {
//				m.mu.Unlock()
//				if stopErr := newWatcher.Stop(); stopErr != nil {
//					m.logger.Ctx(context.Background()).Sugar().Errorf("failed to stop redundant watcher for [%s]: %s", label, stopErr.Error())
//				}
//				continue
//			}
//			m.mu.Unlock()
//		}
//
//		select {
//		case entry, ok := <-currentWatcher.Updates():
//			if !ok {
//				m.logger.Ctx(ctx).Sugar().Warnf("watcher [%s] updates channel closed unexpectedly.", label)
//				time.Sleep(m.retryWait)
//				m.mu.Lock()
//				m.watchers[label] = nil
//				m.mu.Unlock()
//				continue
//			}
//			if entry != nil {
//				m.logger.Ctx(ctx).Sugar().Debugf("watcher [%s] received update: op=%s, looping to retry wait.", label, entry.Operation())
//				var object T = m.objectFactory()
//				if entry.Operation() == jetstream.KeyValueDelete {
//					m.logger.Ctx(ctx).Sugar().Debugf("watcher [%s] received delete operation.", label)
//					fn(object)
//					continue
//				}
//				if len(entry.Value()) == 0 && entry.Operation() != jetstream.KeyValuePut {
//					m.logger.Ctx(ctx).Sugar().Debugf("watcher [%s] received non-PUT entry with empty value, skipping unmarshal.", label)
//					continue
//				}
//				err = object.UnmarshalVT(entry.Value())
//				if err != nil {
//					m.logger.Ctx(ctx).Sugar().Warnf("failed to unmarshal state key [%s] object to [%T]: %s", entry.Key(), object, err.Error())
//					// m.logger.Ctx(ctx).Sugar().Errorf("failed to unmarshal state key [%s] object to [%T]: %s", entry.Key(), object, err.Error())
//					continue
//				}
//				fmt.Println("watcherHandlers len", label, len(m.watcherHandlers[label]))
//				for _, f := range m.watcherHandlers[label] {
//					f(object)
//				}
//				// fn(object)
//			} else {
//				m.logger.Ctx(ctx).Sugar().Debugf("watcher [%s] received nil entry.", label)
//			}
//		case <-m.Context().Done():
//			return m.Context().Err()
//		case <-ctx.Done():
//			fmt.Printf("ctx done: %v", ctx.Err())
//			fmt.Println("watcherHandlers: ", len(m.watcherHandlers[label]))
//			m.mu.Lock()
//			delete(m.watcherHandlers[label], ctx)
//			fmt.Println("watcherHandlers.0: ", len(m.watcherHandlers[label]))
//			if len(m.watcherHandlers[label]) > 0 {
//				// next, ok := m.watcherHandlers[label]
//				for nextCtx, _ := range m.watcherHandlers[label] {
//					ctx = nextCtx
//				}
//				m.mu.Unlock()
//				continue
//			}
//			m.mu.Unlock()
//			return ctx.Err()
//		}
//	}
//}
