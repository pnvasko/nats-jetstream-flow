package flow

import (
	"context"
	"fmt"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"time"
)

type MapSplit[T any] struct {
	ctx      context.Context
	in       chan any
	out      chan any
	errorsCh chan error
	logger   common.Logger
}

func NewMapSplit[T any](ctx context.Context, errorsCh chan error, logger common.Logger) *MapSplit[T] {
	passThrough := &MapSplit[T]{
		ctx:      ctx,
		in:       make(chan any),
		out:      make(chan any),
		errorsCh: errorsCh,
		logger:   logger,
	}
	go passThrough.stream()
	return passThrough
}

func (pt *MapSplit[T]) Via(flow Flow) Flow {
	go pt.transmit(flow)
	return flow
}

func (pt *MapSplit[T]) To(sink Sink) {
	pt.transmit(sink)
	err := sink.AwaitCompletion(60 * time.Second)
	if err != nil {
		return
	}
}

func (pt *MapSplit[T]) In() chan<- any {
	return pt.in
}

func (pt *MapSplit[T]) Out() <-chan any {
	return pt.out
}

func (pt *MapSplit[T]) transmit(inlet Input) {
	defer close(inlet.In())
	for element := range pt.Out() {
		data, ok := element.([]T)
		if !ok {
			pt.logger.Ctx(pt.ctx).Sugar().Errorf("not a slice, but %T\n", element)
			pt.errorsCh <- fmt.Errorf("not a slice, but %T\n", element)
			return
		}
		for _, elm := range data {
			inlet.In() <- elm
		}
	}
}

func (pt *MapSplit[T]) stream() {
	for element := range pt.in {
		pt.out <- element
	}
	close(pt.out)
}

var _ Flow = (*MapSplit[any])(nil)
