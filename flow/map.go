package flow

import (
	"context"
	"github.com/panjf2000/ants/v2"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"sync"
	"time"
)

type MapFunction[T, R any] func(T) (R, error)

type MapOption[T, R any] func(*Map[T, R]) error

func WithMapPoolSize[T, R any](n int) MapOption[T, R] {
	return func(cfg *Map[T, R]) error {
		cfg.mapPoolSize = n
		return nil
	}
}

func WithMapPoolSizePerPool[T, R any](n int) MapOption[T, R] {
	return func(cfg *Map[T, R]) error {
		cfg.mapPoolSizePerPool = n
		return nil
	}
}

func WithMapAwaitCompletion[T, R any](td time.Duration) MapOption[T, R] {
	return func(cfg *Map[T, R]) error {
		cfg.awaitCompletion = td
		return nil
	}
}

type Map[T, R any] struct {
	ctx context.Context
	wg  sync.WaitGroup

	in  chan any
	out chan any

	mapPoolSize        int
	mapPoolSizePerPool int
	mapPool            *ants.MultiPoolWithFunc

	mapFunction     MapFunction[T, R]
	awaitCompletion time.Duration

	tracer trace.Tracer
	logger common.Logger
}

func NewMap[T, R any](baseCtx context.Context, mapFunction MapFunction[T, R], poolSizePerPool int, tracer trace.Tracer, logger common.Logger, opts ...MapOption[T, R]) (*Map[T, R], error) {
	var err error

	m := &Map[T, R]{
		ctx:                baseCtx,
		mapFunction:        mapFunction,
		in:                 make(chan any),
		out:                make(chan any),
		mapPoolSize:        DefaultPoolSize,
		mapPoolSizePerPool: DefaultPoolSizePerPool,
		awaitCompletion:    defaultSinkAwaitCompletion,
		tracer:             tracer,
		logger:             logger,
	}

	for _, opt := range opts {
		err = opt(m)
		if err != nil {
			return nil, err
		}
	}

	m.mapPool, err = ants.NewMultiPoolWithFunc(m.mapPoolSize, m.mapPoolSizePerPool, func(im interface{}) {
		defer m.wg.Done()
		msg, ok := im.(T)
		if !ok {
			m.logger.Ctx(m.ctx).Sugar().Errorf("failed to cast interface [%T] to T", im)
			return
		}
		result, err := m.mapFunction(msg)
		if err != nil {
			m.logger.Ctx(m.ctx).Error("map function failed", zap.Error(err))
			return
		}
		select {
		case <-m.ctx.Done():
			return
		case <-time.After(m.awaitCompletion):
			m.logger.Ctx(m.ctx).Error("map function timeout")
			return
		case m.out <- result:
			// sent successfully
		}
	}, ants.RoundRobin)

	if err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Map[T, R]) Via(flow Flow) Flow {
	go m.transmit(flow)
	return flow
}

func (m *Map[T, R]) To(sink Sink) {
	m.transmit(sink)
	if err := sink.AwaitCompletion(m.awaitCompletion); err != nil {
		m.logger.Ctx(m.ctx).Warn("failed to wait sink completion", zap.Error(err))
	}
}

func (m *Map[T, R]) Out() <-chan any {
	return m.out
}

// In returns the input channel of the Map operator.
func (m *Map[T, R]) In() chan<- any {
	return m.in
}

func (m *Map[T, R]) Run() error {
	return m.RunCtx(m.ctx)
}

func (m *Map[T, R]) RunCtx(ctx context.Context) error {
	go m.process(ctx)
	return nil
}

func (m *Map[T, R]) transmit(in Input) {
	for elm := range m.Out() {
		in.In() <- elm
	}
}

func (m *Map[T, R]) process(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			m.logger.Ctx(ctx).Sugar().Errorf("Map function panic %v:", r)
		}
		m.wg.Wait()
		close(m.out)
		if err := m.mapPool.ReleaseTimeout(2 * time.Second); err != nil {
			m.logger.Ctx(m.ctx).Error("pool release timeout", zap.Error(err))
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.ctx.Done():
			return
		case elm, ok := <-m.in:
			if !ok {
				return
			}
			m.wg.Add(1)
			if err := m.mapPool.Invoke(elm); err != nil {
				m.logger.Ctx(m.ctx).Error("submit job failed", zap.Error(err))
				m.wg.Done()
			}
		}
	}
}

var _ Flow = (*Map[any, any])(nil)
