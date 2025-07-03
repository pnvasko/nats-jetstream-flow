package flow

import (
	"context"
	"fmt"
	"github.com/panjf2000/ants/v2"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"sync"
	"time"
)

type FanOutSinkConsumeFunction[T any] func(context.Context, *Future[T]) error
type FanOutSink[T Message] struct {
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once

	in   chan any
	done chan struct{}

	name     string
	spanName string

	consumePool     *ants.MultiPoolWithFunc
	consumeHandler  FanOutSinkConsumeFunction[T]
	poolSize        int
	poolSizePerPool int

	tracer trace.Tracer
	logger common.Logger
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
		spanName:        spanName,
		in:              make(chan any, 10000),
		done:            make(chan struct{}),
		poolSize:        defaultFanOutSinkPoolSize,
		poolSizePerPool: defaultFanOutSinkPoolSizePerPool,
		consumeHandler:  consumeHandler,
		tracer:          tracer,
		logger:          logger,
	}
	fos.ctx, fos.cancel = context.WithCancel(ctx)

	fos.name = fmt.Sprintf("%s sink.", fos.spanName)
	fos.consumePool, err = ants.NewMultiPoolWithFunc(fos.poolSize, fos.poolSizePerPool, func(im interface{}) {
		defer fos.wg.Done()
		future, ok := im.(*Future[T])
		if !ok {
			fos.logger.Ctx(fos.ctx).Error("fan out sink consume input not *Future type")
			return
		}
		defer future.CloseInner()

		msgSpanCtx, msgSpan := fos.tracer.Start(future.Context(), fmt.Sprintf("%s.sink.consume", fos.spanName))

		if err := fos.consumeHandler(msgSpanCtx, future); err != nil {
			future.SetError(err)
			msgSpan.SetStatus(codes.Error, err.Error())
			return
		}
	}, ants.RoundRobin)
	if err != nil {
		return nil, err
	}

	return fos, nil
}

func (fos *FanOutSink[T]) Name() string {
	return fos.name
}

func (fos *FanOutSink[T]) Run() error {
	return fos.RunCtx(fos.ctx)
}

func (fos *FanOutSink[T]) RunCtx(ctx context.Context) error {
	go fos.process(ctx)
	return nil
}

func (fos *FanOutSink[T]) In() chan<- any {
	return fos.in
}

func (fos *FanOutSink[T]) AwaitCompletion(timeout time.Duration) error {
	select {
	case <-fos.done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("await completion timeout")
	}
}

func (fos *FanOutSink[T]) Close(ctx context.Context) error {
	waitChan := make(chan struct{})
	go func() {
		fos.wg.Wait()
		close(waitChan)
	}()

	select {
	case <-ctx.Done():
		fos.closeOnce.Do(func() { fos.cancel() })
		return ctx.Err()
	case <-waitChan:
		<-fos.done
		fos.closeOnce.Do(func() { fos.cancel() })
		return nil
	}
}

func (fos *FanOutSink[T]) Complete() error {
	<-fos.ctx.Done()
	return fos.ctx.Err()
}

func (fos *FanOutSink[T]) process(ctx context.Context) {
	defer close(fos.done)

LOOP:
	for {
		select {
		case <-ctx.Done():
			return
		case <-fos.ctx.Done():
			return
		case msg, ok := <-fos.in:
			if !ok {
				fos.logger.Ctx(ctx).Warn("stream sink received message from a context cancelled")
				return
			}

			switch message := msg.(type) {
			case Message:
				var futures Futures[T]
				msgs := NewMessages(message)
				msgs.Messages = append(msgs.Messages, message)
				mm, ok := msgs.Messages[0].(T)
				if !ok {
					fos.logger.Ctx(ctx).Warn("stream sink build message")
					break LOOP
				}
				future := NewFuture[T](mm.Context(), mm)
				futures = append(futures, future)

				fos.wg.Add(1)
				if err := fos.consumePool.Invoke(future); err != nil {
					fos.logger.Ctx(ctx).Warn("stream sink failed to invoke processing function for input message", zap.Error(err))
					future.CloseInner()
					return
				}

				errs := futures.Await()
				if len(errs) == 0 {
					fos.logger.Ctx(ctx).Warn("success processed.")
				} else {
					fos.logger.Ctx(ctx).Sugar().Errorf("failed processed with %d errors.", len(errs))
				}
			default:
				fos.logger.Ctx(ctx).Sugar().Errorf("unsupported message type %T. Support only [*Messages]", message)
			}
		}
	}
}

var _ Sink = (*FanOutSink[Message])(nil)
