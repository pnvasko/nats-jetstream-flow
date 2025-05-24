package nats_jetstream_flow

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/panjf2000/ants/v2"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"github.com/pnvasko/nats-jetstream-flow/flow"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"sync"
	"time"
)

const (
	defaultProducerSinkPublishAsyncWait = 10 * time.Millisecond
)

type SinkConsumeFunction[T any] func(ctx context.Context, future *flow.Future[T]) error

type StreamSinkConfig struct {
	*StreamConfig
	sendPoolSize        int
	sendPoolSizePerPool int
	drainConn           bool          // Determines whether to drain the connection when the upstream is closed.
	pubOpts             []nats.PubOpt // PubOpt configures options for publishing JetStream messages.
}

type StreamSinkConfigOption func(*StreamSinkConfig) error

func WithPoolSizeStreamSink(n int) StreamSinkConfigOption {
	return func(cfg *StreamSinkConfig) error {
		cfg.sendPoolSize = n
		return nil
	}
}

func WithPoolSizePerPoolStreamSink(n int) StreamSinkConfigOption {
	return func(cfg *StreamSinkConfig) error {
		cfg.sendPoolSizePerPool = n
		return nil
	}
}

func NewStreamSinkConfig(config StreamConfiguration, opts ...StreamSinkConfigOption) (*StreamSinkConfig, error) {
	streamConfig, ok := config.(*StreamConfig)
	if !ok {
		return nil, fmt.Errorf("stream sink configuration must be of type StreamConfig")
	}
	ssc := &StreamSinkConfig{
		StreamConfig:        streamConfig,
		sendPoolSize:        flow.DefaultPoolSize,
		sendPoolSizePerPool: flow.DefaultPoolSizePerPool,
		drainConn:           true,
		pubOpts:             []nats.PubOpt{},
	}

	for _, opt := range opts {
		if err := opt(ssc); err != nil {
			return nil, err
		}
	}

	return ssc, nil
}

func (sc *StreamSinkConfig) StreamName() string {
	return sc.StreamConfig.StreamName()
}

func (sc *StreamSinkConfig) Subjects() []string {
	return sc.StreamConfig.Subjects()
}

type StreamSinkProducer[T flow.Message] struct {
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once

	config *StreamSinkConfig
	stream jetstream.Stream

	in   chan any
	done chan struct{}

	name                string
	spanName            string
	sendPoolSize        int
	sendPoolSizePerPool int

	consumePool  *ants.MultiPoolWithFunc
	completePool *ants.MultiPoolWithFunc

	consumeHandler  SinkConsumeFunction[T]
	completeHandler SinkConsumeFunction[T]
	tracer          trace.Tracer
	logger          *common.Logger
}

func NewStreamSinkProducer[T flow.Message](ctx context.Context,
	spanName string,
	js jetstream.JetStream,
	consumeHandler SinkConsumeFunction[T],
	completeHandler SinkConsumeFunction[T],
	config *StreamSinkConfig,
	tracer trace.Tracer,
	logger *common.Logger,
) (*StreamSinkProducer[T], error) {
	var err error
	ss := &StreamSinkProducer[T]{
		spanName:            spanName,
		config:              config,
		in:                  make(chan any),
		done:                make(chan struct{}),
		sendPoolSize:        config.sendPoolSize,
		sendPoolSizePerPool: config.sendPoolSizePerPool,
		consumeHandler:      consumeHandler,
		completeHandler:     completeHandler,
		tracer:              tracer,
		logger:              logger,
	}
	ss.ctx, ss.cancel = context.WithCancel(ctx)

	if ss.config.StreamInstance() == nil {
		ss.stream, err = CreateOrUpdateStream(ss.ctx, js, ss.config.StreamConfig)
		if err != nil {
			return nil, err
		}
	} else {
		ss.stream = ss.config.StreamInstance()
	}
	ss.name = fmt.Sprintf("%s sink on stream: [%s]", ss.spanName, ss.config.StreamName())

	ss.consumePool, err = ants.NewMultiPoolWithFunc(ss.sendPoolSize, ss.sendPoolSizePerPool, func(im interface{}) {
		// fmt.Println("todo.stream_sink.NewStreamProducerSink: ", im)
		defer ss.wg.Done()
		future, ok := im.(*flow.Future[T])
		if !ok {
			ss.logger.Ctx(ss.ctx).Error("stream sink consume input not *Future type")
			return
		}
		defer future.CloseInner()

		msgSpanCtx, msgSpan := ss.tracer.Start(future.Context(), fmt.Sprintf("%s.message.consume", ss.spanName))

		defer func() {
			if x := recover(); x != nil {
				future.SetError(fmt.Errorf("consume future with error: %v", x))
				fmt.Println("error x", x)
				ss.logger.Ctx(ss.ctx).Error("consume future error", zap.Error(fmt.Errorf("%v", x)))
			}
			msgSpan.End()
		}()

		// if err := ss.consumeHandler(msgSpanCtx, js, future, ss.tracer, ss.logger); err != nil {
		if err := ss.consumeHandler(msgSpanCtx, future); err != nil {
			future.SetError(err)
			msgSpan.SetStatus(codes.Error, err.Error())
			return
		}
	}, ants.RoundRobin)

	ss.completePool, err = ants.NewMultiPoolWithFunc(ss.sendPoolSize, ss.sendPoolSizePerPool, func(im interface{}) {
		defer ss.wg.Done()
		future, ok := im.(*flow.Future[T])
		if !ok {
			ss.logger.Ctx(ss.ctx).Error("stream sink complete input not *Future type")
			return
		}
		defer future.CloseInner()

		msgSpanCtx, msgSpan := ss.tracer.Start(future.Context(), fmt.Sprintf("%s.message.complete", ss.spanName))

		defer func() {
			if x := recover(); x != nil {
				future.SetError(fmt.Errorf("complete future with error: %v", x))
				fmt.Println("error x", x)
				ss.logger.Ctx(ss.ctx).Error("complete future error", zap.Error(fmt.Errorf("%v", x)))
			}
			msgSpan.End()
		}()
		// if err := ss.completeHandler(msgSpanCtx, js, future, ss.tracer, ss.logger); err != nil {
		if err := ss.completeHandler(msgSpanCtx, future); err != nil {
			future.SetError(err)
			msgSpan.SetStatus(codes.Error, err.Error())
			return
		}
	}, ants.RoundRobin)

	return ss, nil
}

func (ss *StreamSinkProducer[T]) Name() string {
	return ss.name
}

func (ss *StreamSinkProducer[T]) Run() error {
	return ss.RunCtx(ss.ctx)
}

func (ss *StreamSinkProducer[T]) RunCtx(ctx context.Context) error {
	go ss.process(ctx)
	return nil
}

func (ss *StreamSinkProducer[T]) In() chan<- any {
	return ss.in
}

func (ss *StreamSinkProducer[T]) AwaitCompletion(timeout time.Duration) error {
	select {
	case <-ss.done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("await completion timeout")
	}
}

func (ss *StreamSinkProducer[T]) Close(ctx context.Context) error {
	waitChan := make(chan struct{})
	go func() {
		ss.wg.Wait()
		close(waitChan)
	}()

	select {
	case <-ctx.Done():
		ss.closeOnce.Do(func() { ss.cancel() })
		return ctx.Err()
	case <-waitChan:
		<-ss.done
		ss.closeOnce.Do(func() { ss.cancel() })
		return nil
	}
}

func (ss *StreamSinkProducer[T]) Complete() error {
	<-ss.ctx.Done()
	return ss.ctx.Err()
}

func (ss *StreamSinkProducer[T]) process(ctx context.Context) {
	defer close(ss.done)

LOOP:
	for {
		select {
		case <-ctx.Done():
			return
		case <-ss.ctx.Done():
			return
		case msg, ok := <-ss.in:
			if !ok {
				ss.logger.Ctx(ctx).Warn("stream sink received message from a context cancelled")
				return
			}

			var err error
			switch message := msg.(type) {
			case flow.Message:
				var futures flow.Futures[T]
				msgs := flow.NewMessages(message)
				msgs.Messages = append(msgs.Messages, message)
				mm, ok := msgs.Messages[0].(T)
				if !ok {
					ss.logger.Ctx(ctx).Warn("stream sink build message")
					break LOOP
				}
				future := flow.NewFuture[T](mm.Context(), mm)
				futures = append(futures, future)
				ss.wg.Add(1)
				if err := ss.consumePool.Invoke(future); err != nil {
					ss.logger.Ctx(ctx).Warn("stream sink failed to invoke processing function for input message", zap.Error(err))
				}
				errs := futures.Await()

				// todo. select func for Ack or Nack by error
				if len(errs) == 0 {
					if ok := message.Ack(); !ok {
						fmt.Println("StreamProducerSink.process.message:  failed to Ack message")
					}
				} else {
					if ok := message.Nack(); !ok {
						fmt.Println("StreamProducerSink.process.message: failed to Nack message")
					}
				}
			case *flow.Messages:
				var futures flow.Futures[T]
				for _, m := range message.Messages {
					mm, ok := m.(T)
					if !ok {
						ss.logger.Ctx(ctx).Warn("stream sink build message")
						break LOOP
					}

					future := flow.NewFuture[T](mm.Context(), mm)
					futures = append(futures, future)
					ss.wg.Add(1)
					if err := ss.consumePool.Invoke(future); err != nil {
						ss.logger.Ctx(ctx).Warn("stream sink failed to invoke processing function for input messages", zap.Error(err))
						future.CloseInner()
						return
					}
				}

				errs := futures.Await()
				fmt.Println("futures complete OriginalMessage ACK")
				// todo. select func for Ack or Nack by error
				if len(errs) == 0 {
					if ok := message.OriginalMessage.Ack(); !ok {
						fmt.Println("StreamProducerSink.process.messages: failed to Ack message")
					}

					ss.wg.Add(1)
					if err := ss.completePool.Invoke(futures); err != nil {
						ss.logger.Ctx(ctx).Warn("stream sink failed to invoke processing function for input messages", zap.Error(err))
						return
					}
				} else {
					if ok := message.OriginalMessage.Nack(); !ok {
						fmt.Println("StreamProducerSink.process.messages: failed to Nack message")
					}
				}
			default:
				ss.logger.Ctx(ctx).Sugar().Errorf("todo.StreamProducerSink.process unsupported message type %T. Support only [*flow.Messages]", message)
			}

			if err != nil {
				ss.logger.Ctx(ctx).Error("error processing message", zap.Error(err))
			}
		}
	}
}

var _ flow.Sink = (*StreamSinkProducer[flow.Message])(nil)
