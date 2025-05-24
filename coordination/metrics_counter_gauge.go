package coordination

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"github.com/pnvasko/nats-jetstream-flow/proto/v1"
	"go.opentelemetry.io/otel/trace"
	"sync"
	"time"
)

const (
	defaultMetricsBucketPrefix = "metrics"
	defaultCounterGaugeScope   = "counter_gauge"
)

type CounterGauge struct {
	*baseKvStore
	ctx    context.Context
	cancel context.CancelFunc
	ctxMu  sync.RWMutex

	mu        sync.Mutex // Protects RMW cycle within a single client instance
	muCounter map[string]*sync.Mutex
	js        jetstream.JetStream
	kv        jetstream.KeyValue

	tracer trace.Tracer
	logger *common.Logger
}

func NewCounterGaugeStore(ctx context.Context, js jetstream.JetStream, tracer trace.Tracer, logger *common.Logger, opts ...StoreOption[*CounterGauge]) (*CounterGauge, error) {
	cg := &CounterGauge{
		baseKvStore: &baseKvStore{
			scope: defaultCounterGaugeScope,
			// bucketName:       fmt.Sprintf("%s_%s", defaultWaitGroupBucketPrefix, defaultWaitGroupScope),
			retryWait:        defaultRetryWait,
			maxRetryAttempts: defaultMaxRetryAttempts,
			cleanupTTL:       defaultCleanupTTL,
		},
		js:        js,
		muCounter: make(map[string]*sync.Mutex),
		tracer:    tracer,
		logger:    logger,
	}
	cg.ctx, cg.cancel = context.WithCancel(ctx)

	for _, opt := range opts {
		if err := opt(cg); err != nil {
			return nil, err
		}
	}

	if cg.bucketName == "" {
		cg.bucketName = fmt.Sprintf("%s_%s", defaultMetricsBucketPrefix, cg.scope)
	}

	if cg.cleanupTTL >= 0 && cg.cleanupTTL <= time.Duration(100)*time.Millisecond {
		return nil, fmt.Errorf("cleanup TTL must be larger than 100ms")
	}

	keyValueConfig := jetstream.KeyValueConfig{
		Bucket:      cg.bucketName,
		Description: fmt.Sprintf("Distributed counter gauge state for %s", cg.scope),
	}

	if cg.cleanupTTL >= 0 {
		keyValueConfig.TTL = cg.cleanupTTL
	}

	kv, err := js.CreateKeyValue(cg.ctx, keyValueConfig)
	if err == nil {
		cg.kv = kv
		return cg, nil
	}

	if isJSAlreadyExistsError(err) {
		kv, err = js.KeyValue(cg.ctx, cg.bucketName)
		if err != nil {
			return nil, fmt.Errorf("failed to bind to existing KV store '%s': %s", cg.bucketName, err.Error())
		}
		cg.kv = kv
	} else {
		return nil, fmt.Errorf("failed to create/get KV store '%s': %s", cg.bucketName, err.Error())
	}

	return cg, nil
}

func (cg *CounterGauge) JS() jetstream.JetStream {
	return cg.js
}

func (cg *CounterGauge) KV() jetstream.KeyValue {
	return cg.kv
}

func (cg *CounterGauge) Incr(ctx context.Context, label string, count int64) error {
	if count == 0 {
		return nil
	}
	if label == "" {
		return fmt.Errorf("label cannot be empty")
	}

	cg.mu.Lock()
	mu, ok := cg.muCounter[label]
	if !ok {
		mu = &sync.Mutex{}
		cg.muCounter[label] = mu
	}
	cg.mu.Unlock()
	mu.Lock()
	defer mu.Unlock()

	var lastErr error
	for i := 0; i < cg.maxRetryAttempts; i++ {
		newVal, revision, err := cg.getNewValue(ctx, label, count)
		if err != nil {
			return err
		}
		var updateErr error
		if revision == 0 {
			_, updateErr = cg.kv.Create(ctx, label, newVal)
			// _, updateErr = w.kv.Put(ctx, w.groupId, newVal)
		} else {
			_, updateErr = cg.kv.Update(ctx, label, newVal, revision)
		}
		lastErr = updateErr
		if updateErr == nil {
			return nil
		}
		if isJSWrongLastSequence(updateErr) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-cg.ctx.Done():
				return cg.ctx.Err()
			case <-time.After(cg.retryWait):
				continue
			}
		}
		return fmt.Errorf("counter gauge incr failed on attempt %d: %w", i+1, updateErr)
	}
	return fmt.Errorf("counter gauge incr failed after %d attempts due to conflicts: last error: %s", cg.maxRetryAttempts, lastErr.Error())
}

func (cg *CounterGauge) Read(ctx context.Context, label string) (*proto.CounterGaugeRecord, error) {
	mu, ok := cg.muCounter[label]
	if !ok {
		muP := sync.Mutex{}
		cg.muCounter[label] = &muP
	}
	mu.Lock()
	defer mu.Unlock()

	entry, err := cg.kv.Get(ctx, label)
	if err != nil {
		return nil, err
	}
	var counter proto.CounterGaugeRecord
	err = counter.UnmarshalVT(entry.Value())
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal counter gauge state: %w", err)
	}
	return &counter, nil
	//for i := 0; i < cg.maxRetryAttempts; i++ {
	//	entry, err := cg.kv.Get(ctx, label)
	//	if err != nil {
	//		return nil, err
	//	}
	//	var counter *proto.CounterGaugeRecord
	//	err = counter.UnmarshalVT(entry.Value())
	//	if err == nil {
	//		return counter, nil
	//	}
	//
	//	select {
	//	case <-time.After(cg.retryWait):
	//		continue
	//	case <-ctx.Done():
	//		return nil, ctx.Err()
	//	}
	//}
	//
	//return nil, fmt.Errorf("counter gauge read failed after %d attempts", cg.maxRetryAttempts)
}

func (cg *CounterGauge) Reset(ctx context.Context, label string) error {
	if label == "" {
		return fmt.Errorf("label cannot be empty")
	}

	mu, ok := cg.muCounter[label]
	if !ok {
		muP := sync.Mutex{}
		cg.muCounter[label] = &muP
		mu = &muP
	}
	mu.Lock()
	defer mu.Unlock()
	ts := time.Now().UnixNano()
	emptyCounterGaugeRecord := &proto.CounterGaugeRecord{
		Gauge:     0,
		Counter:   0,
		Timestamp: ts,
		Metadata:  make(map[string]string),
	}
	emptyCounterVal, marshalErr := emptyCounterGaugeRecord.MarshalVT()
	if marshalErr != nil {
		return fmt.Errorf("failed to marshal empty counter gauge state: %w", marshalErr)
	}
	var lastErr error
	for i := 0; i < cg.maxRetryAttempts; i++ {
		entry, err := cg.kv.Get(ctx, label)
		var revision uint64

		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
				revision = 0
			} else if errors.Is(err, jetstream.ErrBucketNotFound) {
				return fmt.Errorf("counter gauge bucket '%s' not found: %w", cg.bucketName, err)
			} else {
				return fmt.Errorf("failed to get counter gauge state: %v", err)
			}
		} else {
			revision = entry.Revision()
		}

		var updateErr error
		if revision == 0 {
			_, updateErr = cg.kv.Create(ctx, label, emptyCounterVal)
			// _, updateErr = w.kv.Put(ctx, w.groupId, newVal)
		} else {
			_, updateErr = cg.kv.Update(ctx, label, emptyCounterVal, revision)
		}
		lastErr = updateErr
		if updateErr == nil {
			return nil
		}
		if isJSWrongLastSequence(updateErr) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-cg.ctx.Done():
				return cg.ctx.Err()
			case <-time.After(cg.retryWait):
				continue
			}
		}
		return fmt.Errorf("counter gauge reset failed on attempt %d: %w", i+1, updateErr)
	}

	return fmt.Errorf("counter gauge reset failed after %d attempts due to conflicts: last error: %s", cg.maxRetryAttempts, lastErr.Error())
}

func (cg *CounterGauge) Delete(ctx context.Context) error {
	cg.mu.Lock() // Ensure no other operations are mid-RMW
	defer cg.mu.Unlock()

	err := cg.js.DeleteKeyValue(ctx, cg.bucketName)
	if err != nil {
		return fmt.Errorf("failed to delete counter gauge bucket '%s': %w", cg.bucketName, err)
	}
	return nil
}

func (cg *CounterGauge) getNewValue(ctx context.Context, label string, count int64) ([]byte, uint64, error) {
	entry, err := cg.kv.Get(ctx, label)
	ts := time.Now().UnixNano()
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyDeleted) {
			fmt.Println("getNewValue.ErrKeyDeleted: ", err)
		}
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			emptyCounterGaugeRecord := &proto.CounterGaugeRecord{
				Gauge:     0,
				Counter:   0,
				Timestamp: ts,
				Metadata:  make(map[string]string),
			}
			emptyCounterVal, marshalErr := emptyCounterGaugeRecord.MarshalVT()
			if marshalErr != nil {
				return nil, 0, fmt.Errorf("failed to marshal empty counter gauge state: %w", marshalErr)
			}
			return emptyCounterVal, 0, nil
		}
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return nil, 0, fmt.Errorf("counter gauge bucket '%s' not found: %w", cg.bucketName, err)
		}

		return nil, 0, fmt.Errorf("failed to get counter gauge state: %v", err)
	}

	var counter proto.CounterGaugeRecord
	err = counter.UnmarshalVT(entry.Value())
	if err != nil {
		return nil, 0, fmt.Errorf("failed to unmarshal counter gauge state: %w", err)
	}
	counter.Gauge = counter.Gauge + count
	if count > 0 {
		counter.Counter = counter.Counter + count
	}
	counter.Timestamp = ts

	newVal, err := counter.MarshalVT()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to marshal counter gauge state: %w", err)
	}

	return newVal, entry.Revision(), nil
}
