package flow

import (
	"context"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"go.uber.org/zap"
	"time"
)

type FanOutSlice[T any] struct {
	ctx       context.Context
	in        chan any
	out       chan any
	errorsCh  chan error
	convertFn func(any) ([]T, error)
	logger    common.Logger
}

func NewFanOutSlice[T any](ctx context.Context, convertFn func(any) ([]T, error), wg *common.SafeWaitGroup, errorsCh chan error, logger common.Logger) *FanOutSlice[T] {
	passThrough := &FanOutSlice[T]{
		ctx:       ctx,
		in:        make(chan any),
		out:       make(chan any),
		errorsCh:  errorsCh,
		convertFn: convertFn,
		logger:    logger,
	}
	go passThrough.stream()
	return passThrough
}

func (pt *FanOutSlice[T]) Via(flow Flow) Flow {
	go pt.transmit(flow)
	return flow
}

func (pt *FanOutSlice[T]) To(sink Sink) {
	pt.transmit(sink)
	err := sink.AwaitCompletion(60 * time.Second)
	if err != nil {
		pt.logger.Ctx(pt.ctx).Error("sink completion timeout", zap.Error(err))
		pt.errorsCh <- err
		return
	}
}

func (pt *FanOutSlice[T]) In() chan<- any {
	return pt.in
}

func (pt *FanOutSlice[T]) Out() <-chan any {
	return pt.out
}

func (pt *FanOutSlice[T]) transmit(inlet Input) {
	defer close(inlet.In())
	for element := range pt.Out() {
		data, err := pt.convertFn(element)
		if err != nil {
			pt.logger.Ctx(pt.ctx).Sugar().Errorf("convert data error: %v", err)
			pt.errorsCh <- err
			return
		}
		for _, elm := range data {
			select {
			case <-pt.ctx.Done():
				return
			case inlet.In() <- elm:
			}
		}
	}
}

func (pt *FanOutSlice[T]) stream() {
	for element := range pt.in {
		select {
		case <-pt.ctx.Done():
			return
		case pt.out <- element:
		}
	}
	close(pt.out)
}

var _ Flow = (*FanOutSlice[any])(nil)
