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

type MapMetricStoreObject = *coordination.ObjectStore[*MapMetric, *MapMetricInput]
type MapMetricStoreOption []coordination.StoreOption[MapMetricStoreObject]

type MapMetricStore struct {
	*coordination.ObjectStore[*MapMetric, *MapMetricInput]
}

func NewMapMetricFactory(name string, labelFactory func(any) (string, error)) func() *MapMetric {
	return func() *MapMetric {
		m := NewMapMetric(name, labelFactory)
		return m
	}
}

func NewMapMetricStoreOption(bucketName, scope string, opts ...coordination.StoreOption[MapMetricStoreObject]) MapMetricStoreOption {
	var storeOpts []coordination.StoreOption[MapMetricStoreObject]
	storeOpts = append(storeOpts, coordination.WithBucketName[MapMetricStoreObject](bucketName))
	storeOpts = append(storeOpts, coordination.WithScope[MapMetricStoreObject](scope))
	storeOpts = append(storeOpts, opts...)
	return storeOpts
}

func NewMapMetricStore(ctx context.Context,
	js jetstream.JetStream,
	name string,
	labelFactory func(any) (string, error),
	tracer trace.Tracer,
	logger *common.Logger,
	opts MapMetricStoreOption,
) (*MapMetricStore, error) {
	m, err := coordination.NewObjectStore[*MapMetric, *MapMetricInput](ctx, js, NewMapMetricFactory(name, labelFactory), tracer, logger, opts...)
	if err != nil {
		return nil, err
	}
	return &MapMetricStore{m}, nil
}

func (b *MapMetricStore) Get(ctx context.Context, params *coordination.LabelParams) (any, error) {
	return b.ObjectStore.Read(ctx, params)
}

func (b *MapMetricStore) FullUpdate(ctx context.Context, params *coordination.LabelParams, vs any) error {
	input, ok := vs.(*MapMetricInput)
	if !ok {
		return fmt.Errorf("input is not map metric input, got %T", vs)
	}
	return b.ObjectStore.Update(ctx, params, input)
}

func (b *MapMetricStore) Incr(ctx context.Context, params *coordination.LabelParams, v uint64) error {
	return b.ObjectStore.Update(ctx, params, &MapMetricInput{
		UpdateOverall: &BaseMetricInput{
			Counter:       int64(v),
			FailedCounter: 0,
		},
	})
}

func (b *MapMetricStore) Decr(ctx context.Context, params *coordination.LabelParams, v uint64) error {
	return b.ObjectStore.Update(ctx, params, &MapMetricInput{
		UpdateOverall: &BaseMetricInput{
			Counter:       -int64(v),
			FailedCounter: 0,
		},
	})
}

func (b *MapMetricStore) Failed(ctx context.Context, params *coordination.LabelParams, v uint64) error {
	return b.ObjectStore.Update(ctx, params, &MapMetricInput{
		UpdateOverall: &BaseMetricInput{
			Counter:       0,
			FailedCounter: v,
		},
	})
}

//	BaseMetric overall = 1;
//
// map<uint64, BaseMetric> boards = 2;
type MapMetricInput struct {
	UpdateOverall *BaseMetricInput
	UpdateBoards  map[uint64]*BaseMetricInput
	//TargetBoardID    uint64
	//BoardUpdateDelta *BaseMetricInput
}

type MapMetric struct {
	name         string
	labelFactory func(any) (string, error)
	*proto.MapMetric
}

func NewMapMetric(name string, labelFactory func(any) (string, error)) *MapMetric {
	return &MapMetric{
		name:         name,
		labelFactory: labelFactory,
		MapMetric: &proto.MapMetric{
			Overall: &proto.BaseMetric{},
			Boards:  make(map[uint64]*proto.BaseMetric),
		},
	}
}

func (mm *MapMetric) Label(params any) (string, error) {
	if mm.labelFactory != nil {
		return mm.labelFactory(params)
	}

	return mm.name, nil
}

func (mm *MapMetric) SpanName() string {
	return mm.name
}

func (mm *MapMetric) Empty() ([]byte, error) {
	emptyMetric := &MapMetric{
		MapMetric: &proto.MapMetric{
			Overall: &proto.BaseMetric{},
			Boards:  make(map[uint64]*proto.BaseMetric),
		},
	}
	return emptyMetric.MarshalVT()
}

func (mm *MapMetric) Update(input any) error {
	updateInput, ok := input.(*MapMetricInput)
	if !ok {
		return fmt.Errorf("input is not map metric input, got %T", input)
	}

	if updateInput.UpdateOverall == nil && updateInput.UpdateBoards == nil {
		return fmt.Errorf("update is nil in map metric input")
	}

	if mm.Overall == nil {
		mm.Overall = &proto.BaseMetric{}
	}

	if updateInput.UpdateOverall != nil {
		applyBaseMetricInputDelta(mm.Overall, updateInput.UpdateOverall)
	}

	if updateInput.UpdateBoards != nil {
		if mm.Boards == nil {
			mm.Boards = make(map[uint64]*proto.BaseMetric)
		}

		for targetBoardID, boardUpdate := range updateInput.UpdateBoards {
			if boardUpdate == nil {
				continue
			}
			if targetBoardID == 0 {
				continue
			}
			boardMetric, ok := mm.Boards[targetBoardID]
			if !ok || boardMetric == nil {
				boardMetric = &proto.BaseMetric{}
				mm.Boards[targetBoardID] = boardMetric
			}
			applyBaseMetricInputDelta(boardMetric, boardUpdate)
		}
	}
	return nil
}

func (mm *MapMetric) MarshalVT() ([]byte, error) {
	return mm.MapMetric.MarshalVT()
}

func (mm *MapMetric) UnmarshalVT(data []byte) error {
	if mm.MapMetric == nil {
		mm.MapMetric = &proto.MapMetric{}
	}

	if err := mm.MapMetric.UnmarshalVT(data); err != nil {
		return err
	}

	if mm.Overall == nil {
		mm.Overall = &proto.BaseMetric{}
	}

	if mm.Boards == nil {
		mm.Boards = make(map[uint64]*proto.BaseMetric)
	}
	return nil
}

func applyBaseMetricInputDelta(metric *proto.BaseMetric, delta *BaseMetricInput) {
	if metric == nil || delta == nil {
		return
	}

	metric.InFlight += delta.Counter

	if delta.Counter > 0 {
		metric.Processed += uint64(delta.Counter)
	}

	if delta.FailedCounter > 0 {
		metric.Failed += delta.FailedCounter
	}
}
