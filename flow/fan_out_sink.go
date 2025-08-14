package flow

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type FanOutSinkConsumeFunction[T any] func(context.Context, *Future[T]) error
type FanOutSink[T Message] struct {
	ctx    context.Context
	cancel context.CancelFunc

	closing   chan struct{} // always non-nil
	done      chan struct{} // closed when process() exits
	closeOnce sync.Once

	wg sync.WaitGroup

	in chan any

	name     string
	spanName string
	tracer   trace.Tracer
	logger   common.Logger

	consumePool     *ants.MultiPoolWithFunc
	consumeHandler  FanOutSinkConsumeFunction[T]
	poolSize        int
	poolSizePerPool int
}

const (
	defaultFanOutSinkPoolSize        = 100
	defaultFanOutSinkPoolSizePerPool = 1000
)

func NewFanOutSink[T Message](ctx context.Context,
	spanName string,
	consumeHandler FanOutSinkConsumeFunction[T],
	tracer trace.Tracer,
	logger common.Logger,
) (*FanOutSink[T], error) {
	var err error
	fos := &FanOutSink[T]{
		name:            fmt.Sprintf("%s sink.", spanName),
		spanName:        spanName,
		in:              make(chan any, 10000),
		closing:         make(chan struct{}),
		done:            make(chan struct{}),
		poolSize:        defaultFanOutSinkPoolSize,
		poolSizePerPool: defaultFanOutSinkPoolSizePerPool,
		tracer:          tracer,
		logger:          logger,
		consumeHandler:  consumeHandler,
	}
	fos.ctx, fos.cancel = context.WithCancel(ctx)

	fos.consumePool, err = ants.NewMultiPoolWithFunc(fos.poolSize, fos.poolSizePerPool, func(im interface{}) {
		defer fos.wg.Done()

		future, ok := im.(*Future[T])
		if !ok {
			fos.logger.Ctx(fos.ctx).Error("fanout sink: received non-*Future")
			return
		}
		defer future.CloseInner()

		msgSpanCtx, msgSpan := fos.tracer.Start(future.Context(), fos.spanName+".sink.consume")
		defer msgSpan.End()

		if err := fos.consumeHandler(msgSpanCtx, future); err != nil {
			future.SetError(err)
			msgSpan.SetStatus(codes.Error, err.Error())
		}
	}, ants.RoundRobin)

	if err != nil {
		fos.cancel()
		return nil, err
	}

	return fos, nil
}

func (fos *FanOutSink[T]) Name() string {
	return fos.name
}

func (fos *FanOutSink[T]) In() chan<- any {
	return fos.in
}

func (fos *FanOutSink[T]) Run() error {
	return fos.RunCtx(fos.ctx)
}

func (fos *FanOutSink[T]) RunCtx(ctx context.Context) error {
	go fos.process(ctx)
	return nil
}

func (fos *FanOutSink[T]) AwaitCompletion(timeout time.Duration) error {
	timer := time.NewTimer(timeout)

	select {
	case <-fos.done:
		return nil
	case <-timer.C:
		return fmt.Errorf("await completion timeout")
	}
}

func (fos *FanOutSink[T]) Close(ctx context.Context) error {
	fos.closeOnce.Do(func() {
		close(fos.closing)
		fos.cancel()
		ants.Release()
	})

	waitWG := make(chan struct{})

	go func() {
		fos.wg.Wait()
		close(waitWG)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitWG:
		<-fos.done
		return nil
	}
}

func (fos *FanOutSink[T]) Complete() error {
	<-fos.ctx.Done()
	return fos.ctx.Err()
}

func (fos *FanOutSink[T]) process(ctx context.Context) {
	defer close(fos.done)

	for {
		select {
		case <-fos.closing:
			return
		case <-ctx.Done():
			return
		case <-fos.ctx.Done():
			return
		case msg, ok := <-fos.in:
			if !ok {
				fos.logger.Ctx(ctx).Debug("stream sink received message from a context cancelled")
				return
			}
			message, ok := msg.(Message)
			if !ok {
				fos.logger.Ctx(ctx).Sugar().Errorf("fanout sink: unsupported type %T; expected Message", msg)
				continue
			}
			typedMsg, ok := any(message).(T)
			if !ok {
				fos.logger.Ctx(ctx).Sugar().Errorf("fanout sink: message type %T not assignable to %T", message, *new(T))
				continue
			}

			future := NewFuture[T](message.Context(), typedMsg)
			var futures Futures[T]
			futures = append(futures, future)

			fos.wg.Add(1)
			if err := fos.consumePool.Invoke(future); err != nil {
				fos.wg.Done()
				future.CloseInner()
				fos.logger.Ctx(ctx).Warn("fanout sink: submit to pool failed", zap.Error(err))
				return
			}
			errs := futures.Await()
			if len(errs) == 0 {
				fos.logger.Ctx(ctx).Debug("success processed.")
			} else {
				fos.logger.Ctx(ctx).Sugar().Errorf("failed processed with %d errors.", len(errs))
			}
		}
	}
}

var _ Sink = (*FanOutSink[Message])(nil)
