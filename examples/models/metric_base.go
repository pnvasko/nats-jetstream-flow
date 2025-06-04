package models

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"github.com/pnvasko/nats-jetstream-flow/coordination"
	"github.com/pnvasko/nats-jetstream-flow/proto/v1"
	"go.opentelemetry.io/otel/trace"
)

type BaseMetricStoreObject = *coordination.ObjectStore[*BaseMetric, *BaseMetricInput]
type BaseMetricStoreOption []coordination.StoreOption[BaseMetricStoreObject]

type BaseMetricStore struct {
	*coordination.ObjectStore[*BaseMetric, *BaseMetricInput]
}

func (b *BaseMetricStore) Get(ctx context.Context, params *coordination.LabelParams) (any, error) {
	return b.ObjectStore.Read(ctx, params)
}

func (b *BaseMetricStore) FullUpdate(ctx context.Context, params *coordination.LabelParams, vs any) error {
	input, ok := vs.(*BaseMetricInput)
	if !ok {
		return fmt.Errorf("input is not base metric input, got %T", vs)
	}
	return b.ObjectStore.Update(ctx, params, input)
}

func (b *BaseMetricStore) Incr(ctx context.Context, params *coordination.LabelParams, v uint64) error {
	return b.ObjectStore.Update(ctx, params, &BaseMetricInput{
		Counter:       int64(v),
		FailedCounter: 0,
	})
}

func (b *BaseMetricStore) Decr(ctx context.Context, params *coordination.LabelParams, v uint64) error {
	return b.ObjectStore.Update(ctx, params, &BaseMetricInput{
		Counter:       -int64(v),
		FailedCounter: 0,
	})
}

func (b *BaseMetricStore) Failed(ctx context.Context, params *coordination.LabelParams, v uint64) error {
	return b.ObjectStore.Update(ctx, params, &BaseMetricInput{
		Counter:       0,
		FailedCounter: v,
	})
}

func NewBaseMetricFactory(name string, labelFactory func(any) (string, error)) func() *BaseMetric {
	return func() *BaseMetric {
		m := NewBaseMetric(name, labelFactory)
		return m
	}
}

func NewBaseMetricStoreOption(bucketName, scope string, opts ...coordination.StoreOption[BaseMetricStoreObject]) BaseMetricStoreOption {
	var storeOpts []coordination.StoreOption[BaseMetricStoreObject]
	storeOpts = append(storeOpts, coordination.WithBucketName[BaseMetricStoreObject](bucketName))
	storeOpts = append(storeOpts, coordination.WithScope[BaseMetricStoreObject](scope))
	storeOpts = append(storeOpts, opts...)
	return storeOpts
}

func NewBaseMetricStore(ctx context.Context,
	js jetstream.JetStream,
	name string,
	labelFactory func(any) (string, error),
	tracer trace.Tracer,
	logger common.Logger,
	opts BaseMetricStoreOption,
) (*BaseMetricStore, error) {
	m, err := coordination.NewObjectStore[*BaseMetric, *BaseMetricInput](ctx, js, NewBaseMetricFactory(name, labelFactory), tracer, logger, opts...)
	if err != nil {
		return nil, err
	}
	return &BaseMetricStore{m}, nil
}

type BaseMetricInput struct {
	Counter       int64
	FailedCounter uint64
}

func (bmi *BaseMetricInput) Set(counter int64, failedCounter uint64) error {
	if bmi == nil {
		return fmt.Errorf("base metric input is nil")
	}
	bmi.Counter = counter
	bmi.FailedCounter = failedCounter
	return nil
}

type BaseMetric struct {
	name         string
	labelFactory func(any) (string, error)
	*proto.BaseMetric
}

func NewBaseMetric(name string, labelFactory func(any) (string, error)) *BaseMetric {
	return &BaseMetric{
		name:         name,
		labelFactory: labelFactory,
		BaseMetric:   &proto.BaseMetric{},
	}
}

func (bm *BaseMetric) Label(params any) (string, error) {
	if bm.labelFactory != nil {
		return bm.labelFactory(params)
	}

	return bm.name, nil
}

func (bm *BaseMetric) SpanName() string {
	return bm.name
}

func (bm *BaseMetric) Empty() ([]byte, error) {
	if bm.BaseMetric == nil {
		bm.BaseMetric = &proto.BaseMetric{}
	}
	return bm.BaseMetric.MarshalVT()
}

func (bm *BaseMetric) Update(input any) error {
	values, ok := input.(*BaseMetricInput)
	if !ok {
		return fmt.Errorf("input is not base metric input, got %T", input)
	}

	if bm.BaseMetric == nil {
		bm.BaseMetric = &proto.BaseMetric{}
	}

	bm.BaseMetric.InFlight += values.Counter

	if values.Counter > 0 {
		bm.BaseMetric.Processed += uint64(values.Counter)
	}

	if values.FailedCounter > 0 {
		bm.BaseMetric.Failed += values.FailedCounter
	}

	return nil
}

func (bm *BaseMetric) MarshalVT() ([]byte, error) {
	return bm.BaseMetric.MarshalVT()
}

func (bm *BaseMetric) UnmarshalVT(data []byte) error {
	if bm.BaseMetric == nil {
		bm.BaseMetric = &proto.BaseMetric{}
	}
	return bm.BaseMetric.UnmarshalVT(data)
}

var _ coordination.Counter = (*BaseMetricStore)(nil)
