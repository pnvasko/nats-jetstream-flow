package flow

import (
	"context"
	"github.com/panjf2000/ants/v2"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"go.uber.org/zap"
	"time"
)

const (
	DefaultPredicateHandlerKey = "default"
	DefaultTimeout             = 5 * time.Second
	DefaultChanBufferSize      = 100
)

type splitFlowTask struct {
	Handler *SplitFlowPredicateHandler
	Data    any
}

type SubFlowDefenition struct {
	In   Input
	Flow Flow
}
type SplitFlowHandlers map[string]*SplitFlowPredicateHandler
type SplitFlowPredicateHandler struct {
	Predicate func(any) bool
	Flow      *SubFlowDefenition
}

type SplitByPredicateFlow struct {
	ctx      context.Context
	in       chan any
	out      chan any
	handlers SplitFlowHandlers
	pool     *ants.MultiPoolWithFunc
	timeout  time.Duration
	logger   common.Logger
}

func NewSplitByPredicateFlow(ctx context.Context, handlers SplitFlowHandlers, logger common.Logger) (*SplitByPredicateFlow, error) {
	sf := &SplitByPredicateFlow{
		ctx:      ctx,
		in:       make(chan any, DefaultChanBufferSize),
		out:      make(chan any, DefaultChanBufferSize),
		handlers: handlers,
		logger:   logger,
		timeout:  DefaultTimeout,
	}
	var err error
	sf.pool, err = ants.NewMultiPoolWithFunc(DefaultPoolSize, DefaultPoolSizePerPool, func(im interface{}) {
		task, ok := im.(*splitFlowTask)
		if !ok {
			sf.logger.Ctx(sf.ctx).Sugar().Errorf("failed to cast interface [%T] to T", im)
			return
		}

		task.Handler.Flow.In.In() <- task.Data
		// task.Handler.Flow.In <- task.Data

		select {
		case out := <-task.Handler.Flow.Flow.Out():
			sf.out <- out
		case <-time.After(sf.timeout):
			sf.logger.Ctx(sf.ctx).Sugar().Errorf("timeout receiving from flow.Out() for element %v", task.Data)
		}
	}, ants.RoundRobin)
	if err != nil {
		return nil, err
	}
	go sf.stream()
	return sf, nil
}

func (sf *SplitByPredicateFlow) Out() <-chan any {
	return sf.out
}

func (sf *SplitByPredicateFlow) In() chan<- any {
	return sf.in
}

func (sf *SplitByPredicateFlow) Via(next Flow) Flow {
	go sf.transmit(next)
	return next
}

func (sf *SplitByPredicateFlow) To(sink Sink) {
	sf.transmit(sink)
	if err := sink.AwaitCompletion(60 * time.Second); err != nil {
		sf.logger.Ctx(sf.ctx).Error("sink await error", zap.Error(err))
	}
}

func (sf *SplitByPredicateFlow) transmit(target Input) {
	defer func() {
		deferFunc := func() {
			defer func() {
				recover()
			}()
			close(target.In())
		}
		deferFunc()
	}()
	for element := range sf.Out() {
		target.In() <- element
	}
}

func (sf *SplitByPredicateFlow) stream() {
	for element := range sf.in {
		dispatched := false
		for key, handler := range sf.handlers {
			if key == DefaultPredicateHandlerKey {
				continue
			}
			if handler.Predicate(element) {
				dispatched = true
				if err := sf.pool.Invoke(&splitFlowTask{
					Handler: handler,
					Data:    element,
				}); err != nil {
					sf.logger.Ctx(sf.ctx).Error("failed invoke split handler", zap.Error(err))
				}
			}
		}
		if !dispatched {
			if defaultHandler, ok := sf.handlers[DefaultPredicateHandlerKey]; ok {
				if err := sf.pool.Invoke(&splitFlowTask{
					Handler: defaultHandler,
					Data:    element,
				}); err != nil {
					sf.logger.Ctx(sf.ctx).Error("failed invoke split handler", zap.Error(err))
				}
			}
		}
	}
	close(sf.out)
}
