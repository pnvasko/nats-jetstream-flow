package flow

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"go.uber.org/zap"
)

const (
	DefaultPredicateHandlerKey = "default"
	DefaultTimeout             = 5 * time.Second
	DefaultChanBufferSize      = 100
	DefaultSinkAwaitTimeout    = 60 * time.Second
)

// MatchMode controls how predicates are evaluated against an element.
type MatchMode int

const (
	// MatchAll dispatches the element to every handler whose predicate returns true.
	MatchAll MatchMode = iota
	// MatchFirst dispatches the element to the first handler (in map iteration order)
	// whose predicate returns true, then stops.
	MatchFirst
)

type splitFlowTask struct {
	Handler *SplitFlowPredicateHandler
	Data    any
}

type SplitFlowHandlers map[string]*SplitFlowPredicateHandler

type SplitFlowPredicateHandler struct {
	Predicate func(any) bool
	Flow      *SubFlowDefinition
}

// Option configures a SplitByPredicateFlow.
type Option func(*SplitByPredicateFlow)

func WithTimeout(d time.Duration) Option {
	return func(sf *SplitByPredicateFlow) { sf.timeout = d }
}

func WithMatchMode(m MatchMode) Option {
	return func(sf *SplitByPredicateFlow) { sf.matchMode = m }
}

func WithSinkAwaitTimeout(d time.Duration) Option {
	return func(sf *SplitByPredicateFlow) { sf.sinkAwaitTimeout = d }
}

type SplitByPredicateFlow struct {
	ctx              context.Context
	in               chan any
	out              chan any
	handlers         SplitFlowHandlers
	pool             *ants.MultiPoolWithFunc
	timeout          time.Duration
	sinkAwaitTimeout time.Duration
	matchMode        MatchMode
	logger           common.Logger

	wg       sync.WaitGroup // tracks in-flight worker tasks
	closeOut sync.Once
}

func NewSplitByPredicateFlow(
	ctx context.Context,
	handlers SplitFlowHandlers,
	logger common.Logger,
	opts ...Option,
) (*SplitByPredicateFlow, error) {
	sf := &SplitByPredicateFlow{
		ctx:              ctx,
		in:               make(chan any, DefaultChanBufferSize),
		out:              make(chan any, DefaultChanBufferSize),
		handlers:         handlers,
		logger:           logger,
		timeout:          DefaultTimeout,
		sinkAwaitTimeout: DefaultSinkAwaitTimeout,
		matchMode:        MatchAll,
	}
	for _, opt := range opts {
		opt(sf)
	}

	pool, err := ants.NewMultiPoolWithFunc(
		DefaultPoolSize,
		DefaultPoolSizePerPool,
		sf.worker,
		ants.RoundRobin,
	)
	if err != nil {
		return nil, err
	}
	sf.pool = pool

	go sf.stream()
	return sf, nil
}

// worker is the per-task function executed by the ants pool.
// It must not block indefinitely: every channel op is guarded by ctx + timeout.
func (sf *SplitByPredicateFlow) worker(im interface{}) {
	defer sf.wg.Done()

	task, ok := im.(*splitFlowTask)
	if !ok {
		sf.logger.Ctx(sf.ctx).Error("failed to cast pool argument to *splitFlowTask",
			zap.String("got_type", typeName(im)))
		return
	}

	taskCtx, cancel := context.WithTimeout(sf.ctx, sf.timeout)
	defer cancel()

	// 1) Send into the sub-flow's input — guarded so we never block forever
	//    if the sub-flow is full or shut down.
	select {
	case task.Handler.Flow.Input.In() <- task.Data:
	case <-taskCtx.Done():
		sf.logger.Ctx(sf.ctx).Error("timeout/cancel sending to sub-flow input",
			zap.Error(taskCtx.Err()),
			zap.Any("element", task.Data))
		return
	}

	// 2) Receive from the sub-flow's output and forward downstream —
	//    both halves of the forward are guarded.
	select {
	case out, ok := <-task.Handler.Flow.Flow.Out():
		if !ok {
			return // sub-flow closed
		}
		select {
		case sf.out <- out:
		case <-taskCtx.Done():
			sf.logger.Ctx(sf.ctx).Error("timeout/cancel forwarding to out",
				zap.Error(taskCtx.Err()))
		}
	case <-taskCtx.Done():
		sf.logger.Ctx(sf.ctx).Error("timeout/cancel receiving from sub-flow out",
			zap.Error(taskCtx.Err()),
			zap.Any("element", task.Data))
	}
}

func (sf *SplitByPredicateFlow) Out() <-chan any { return sf.out }
func (sf *SplitByPredicateFlow) In() chan<- any  { return sf.in }

func (sf *SplitByPredicateFlow) Via(next Flow) Flow {
	go sf.transmit(next)
	return next
}

func (sf *SplitByPredicateFlow) To(sink Sink) {
	sf.transmit(sink)
	if err := sink.AwaitCompletion(sf.sinkAwaitTimeout); err != nil {
		sf.logger.Ctx(sf.ctx).Error("sink await error", zap.Error(err))
	}
}

// transmit forwards everything from sf.out into target.In() and closes
// target.In() exactly once when the source drains. The caller of Via/To owns
// the target; if multiple producers fan into the same target, that needs to
// be coordinated at a higher level — we do NOT silently swallow double-close.
func (sf *SplitByPredicateFlow) transmit(target Input) {
	defer close(target.In())
	for element := range sf.Out() {
		select {
		case target.In() <- element:
		case <-sf.ctx.Done():
			return
		}
	}
}

func (sf *SplitByPredicateFlow) stream() {
	defer func() {
		// Wait for all dispatched worker tasks to finish before closing out.
		// This is what prevents "send on closed channel" panics.
		sf.wg.Wait()
		sf.closeOut.Do(func() { close(sf.out) })
	}()

	for {
		select {
		case <-sf.ctx.Done():
			return
		case element, ok := <-sf.in:
			if !ok {
				return
			}
			sf.dispatch(element)
		}
	}
}

func (sf *SplitByPredicateFlow) dispatch(element any) {
	dispatched := false
	for key, handler := range sf.handlers {
		if key == DefaultPredicateHandlerKey {
			continue
		}
		if !handler.Predicate(element) {
			continue
		}
		sf.invoke(handler, element)
		dispatched = true
		if sf.matchMode == MatchFirst {
			return
		}
	}
	if !dispatched {
		if defaultHandler, ok := sf.handlers[DefaultPredicateHandlerKey]; ok {
			sf.invoke(defaultHandler, element)
		}
	}
}

func (sf *SplitByPredicateFlow) invoke(handler *SplitFlowPredicateHandler, element any) {
	sf.wg.Add(1)
	task := &splitFlowTask{Handler: handler, Data: element}
	if err := sf.pool.Invoke(task); err != nil {
		// Pool rejected the task — undo the WaitGroup increment so Wait() can return.
		sf.wg.Done()
		sf.logger.Ctx(sf.ctx).Error("failed to invoke split handler",
			zap.String("handler_predicate_match", "true"),
			zap.Error(err))
		// If the pool is closed, propagate by cancelling? Up to the embedder.
		if errors.Is(err, ants.ErrPoolClosed) {
			return
		}
	}
}

// typeName is a small helper that avoids importing reflect at the top level
// just for an error message.
func typeName(v any) string {
	if v == nil {
		return "<nil>"
	}
	// %T via fmt would also work; keeping zero deps in hot path.
	return ""
}
