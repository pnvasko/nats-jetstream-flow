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

type MapUnTypedOption func(*MapUnTyped) error

func WithMapUnTypedPoolSize(n int) MapUnTypedOption {
	return func(cfg *MapUnTyped) error {
		cfg.mapPoolSize = n
		return nil
	}
}

func WithMapUnTypedPoolSizePerPool(n int) MapUnTypedOption {
	return func(cfg *MapUnTyped) error {
		cfg.mapPoolSizePerPool = n
		return nil
	}
}

func WithMapUnTypedAwaitCompletion(td time.Duration) MapUnTypedOption {
	return func(cfg *MapUnTyped) error {
		cfg.awaitCompletion = td
		return nil
	}
}

type MapUnTyped struct {
	ctx context.Context
	wg  sync.WaitGroup

	in  chan any
	out chan any

	mapPoolSize        int
	mapPoolSizePerPool int
	mapPool            *ants.MultiPoolWithFunc

	mapFunction     func(any) (any, error)
	awaitCompletion time.Duration

	tracer trace.Tracer
	logger common.Logger
}

func NewMapUnTyped[T, R any](baseCtx context.Context, mapFunction func(any) (any, error), poolSizePerPool int, tracer trace.Tracer, logger common.Logger, opts ...MapUnTypedOption) (*MapUnTyped, error) {
	var err error

	m := &MapUnTyped{
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
		result, err := m.mapFunction(im)
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

func (m *MapUnTyped) Via(flow Flow) Flow {
	go m.transmit(flow)
	return flow
}

func (m *MapUnTyped) To(sink Sink) {
	m.transmit(sink)
	if err := sink.AwaitCompletion(m.awaitCompletion); err != nil {
		m.logger.Ctx(m.ctx).Warn("failed to wait sink completion", zap.Error(err))
	}
}

func (m *MapUnTyped) Out() <-chan any {
	return m.out
}

func (m *MapUnTyped) In() chan<- any {
	return m.in
}

func (m *MapUnTyped) Run() error {
	return m.RunCtx(m.ctx)
}

func (m *MapUnTyped) RunCtx(ctx context.Context) error {
	go m.process(ctx)
	return nil
}

func (m *MapUnTyped) transmit(in Input) {
	for elm := range m.Out() {
		in.In() <- elm
	}
}

func (m *MapUnTyped) process(ctx context.Context) {
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
