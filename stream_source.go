package nats_jetstream_flow

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/panjf2000/ants/v2"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"github.com/pnvasko/nats-jetstream-flow/flow"
	"go.opentelemetry.io/otel/attribute"
	// "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	defaultBufferSize     = 100
	defaultNakDelay       = time.Second * 3
	defaultProcessAckWait = time.Second * 10
)

type StreamSourceConfig struct {
	*StreamConfig
	*ConsumerConfig

	js                      jetstream.JetStream
	poolSize                int
	poolSizePerPool         int
	fetchBatchSize          int
	outputChannelBufferSize int
	processAckWait          time.Duration

	ackAsync bool
	nakDelay Delay
}

type StreamSourceConfigOption func(*StreamSourceConfig) error

func WithFetchBatchSizeStreamSource(n int) StreamSourceConfigOption {
	return func(cfg *StreamSourceConfig) error {
		cfg.fetchBatchSize = n
		return nil
	}
}

func WithProcessAckWaitStreamSource(td time.Duration) StreamSourceConfigOption {
	return func(cfg *StreamSourceConfig) error {
		cfg.processAckWait = td
		return nil
	}
}

func WithOutputChannelBufferSizeStreamSource(n int) StreamSourceConfigOption {
	return func(cfg *StreamSourceConfig) error {
		cfg.outputChannelBufferSize = n
		return nil
	}
}

func WithAckAsyncStreamSource(v bool) StreamSourceConfigOption {
	return func(cfg *StreamSourceConfig) error {
		cfg.ackAsync = v
		return nil
	}
}

func WithNakDelayStreamSource(d Delay) StreamSourceConfigOption {
	return func(cfg *StreamSourceConfig) error {
		cfg.nakDelay = d
		return nil
	}
}

func WithPoolSizeStreamSource(n int) StreamSourceConfigOption {
	return func(cfg *StreamSourceConfig) error {
		cfg.poolSize = n
		return nil
	}
}

func WithPoolSizePerPoolStreamSource(n int) StreamSourceConfigOption {
	return func(cfg *StreamSourceConfig) error {
		cfg.poolSizePerPool = n
		return nil
	}
}

func NewJetStreamSourceConfig(js jetstream.JetStream, streamName string, subjects []string, streamOpts StreamConfigOptions, consumerOpts ConsumerConfigOptions, opts ...StreamSourceConfigOption) (*StreamSourceConfig, error) {
	if streamName == "" {
		return nil, fmt.Errorf("stream name is required")
	}

	if len(subjects) == 0 {
		return nil, fmt.Errorf("no subjects specified")
	}

	streamConfig, err := NewStreamConfig(streamName, subjects, streamOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create stream config: %w", err)
	}

	consumerConfig, err := NewConsumerConfig(streamName, subjects, consumerOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create stream config: %w", err)
	}

	cfg := &StreamSourceConfig{
		js:             js,
		StreamConfig:   streamConfig,
		ConsumerConfig: consumerConfig,

		// deliverPolicy: "",
		poolSize:                flow.DefaultPoolSize,
		poolSizePerPool:         flow.DefaultPoolSizePerPool,
		outputChannelBufferSize: defaultBufferSize,
		fetchBatchSize:          1,
		ackAsync:                true,
		nakDelay:                NewStaticDelay(defaultNakDelay),
		processAckWait:          defaultProcessAckWait,
	}

	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}

	if cfg.ConsumerConfig.AckWait() <= cfg.processAckWait {
		return nil, fmt.Errorf("Ack wait time must be greater than process Ack wait time")
	}

	if cfg.StreamConfig.CleanupTTL() != 0 && cfg.StreamConfig.CleanupTTL() <= time.Duration(100)*time.Millisecond {
		return nil, fmt.Errorf("cleanup TTL must be larger than 100ms")
	}

	return cfg, nil
}

type StreamSource[T flow.MessageData] struct {
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	mu        *sync.RWMutex
	closeOnce sync.Once
	closing   chan struct{}
	closed    bool

	name            string
	spanName        string
	config          *StreamSourceConfig
	stream          jetstream.Stream
	consumer        jetstream.Consumer
	poolSize        int
	poolSizePerPool int
	pool            *ants.MultiPoolWithFunc

	out    chan any
	tracer trace.Tracer
	logger common.Logger
}

type streamSourceJob struct {
	ctx   context.Context
	jsMsg jetstream.Msg
	msg   flow.Message
}

func NewStreamSource[T flow.MessageData](ctx context.Context,
	spanName string,
	js jetstream.JetStream,
	config *StreamSourceConfig,
	tracer trace.Tracer,
	logger common.Logger,
) (*StreamSource[T], error) {
	var err error
	ss := &StreamSource[T]{
		spanName:        spanName,
		mu:              &sync.RWMutex{},
		closing:         make(chan struct{}),
		closed:          false,
		config:          config,
		out:             make(chan any, config.outputChannelBufferSize),
		poolSize:        config.poolSize,
		poolSizePerPool: config.poolSizePerPool,
		tracer:          tracer,
		logger:          logger,
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

	ss.consumer, err = CreateOrUpdateConsumer(ss.ctx, js, ss.config.ConsumerConfig)
	if err != nil {
		return nil, err
	}

	nameSubjects := ss.config.StreamConfig.Subjects()
	if len(ss.config.ConsumerConfig.FilterSubjects()) > 0 {
		nameSubjects = ss.config.ConsumerConfig.FilterSubjects()
	}

	ss.name = fmt.Sprintf("%s source on stream: [%s ]; subjects: [%s]; consumer: [%s/%s]; ", ss.spanName, ss.config.StreamConfig.StreamName(), strings.Join(nameSubjects, ","), ss.config.ConsumerConfig.ConsumerName(), ss.config.ConsumerConfig.DurableName())

	ss.pool, err = ants.NewMultiPoolWithFunc(ss.poolSize, ss.poolSizePerPool, func(im interface{}) {
		defer ss.wg.Done()

		job, ok := im.(*streamSourceJob)
		if !ok {
			ss.logger.Ctx(ss.ctx).Sugar().Errorf("stream source invalid job type %T", im)
			return
		}

		if err := job.jsMsg.InProgress(); err != nil {
			ss.logger.Ctx(job.ctx).Error("failed to set message in progress", zap.Error(err), zap.String("stream_source", ss.name))
			return
		}
		msg, err := LoadNatsMessage[T](job.jsMsg)

		if err != nil {
			ss.logger.Ctx(job.ctx).Error("cannot unmarshal message", zap.Error(err), zap.String("stream_source", ss.name))
			if err := job.jsMsg.Nak(); err != nil {
				ss.logger.Ctx(job.ctx).Error("failed to NAK unprocessable message", zap.Error(err), zap.String("stream_source", ss.name))
			}
			return
		}

		msgCtx := msg.Context()
		combinedCtx, cancelCombined := context.WithCancel(msgCtx) // Use Background() as the base
		defer cancelCombined()

		var attrs []attribute.KeyValue
		attrs = append(attrs, attribute.String("stream_source", ss.name))
		msgSpanCtx, msgSpan := ss.tracer.Start(combinedCtx, fmt.Sprintf("%s.message.process", ss.spanName))
		defer func() {
			msgSpan.SetAttributes(attrs...)
			msgSpan.End() // Ensure span is ended
		}()

		msg.SetContext(msgSpanCtx)

		go func() {
			select {
			case <-job.ctx.Done():
				ss.logger.Ctx(job.ctx).Debug("Source context cancelled, signaling message worker stop", zap.String("stream_source", ss.name))
			case <-msgCtx.Done():
				ss.logger.Ctx(msgCtx).Debug("Trace context cancelled, signaling message worker stop", zap.String("stream_source", ss.name))
			case <-msgSpanCtx.Done():
				ss.logger.Ctx(job.ctx).Debug("process context cancelled, signaling message worker stop", zap.String("stream_source", ss.name))
			}
			cancelCombined() // Cancel the combinedCtx
		}()

		subject, err := msg.Subject()
		if err != nil {
			ss.logger.Ctx(msgSpanCtx).Error("failed to get message subject", zap.Error(err), zap.String("stream_source", ss.name))
			return
		}

		numDelivered := ParseStringToUInt64(msg.GetMessageMetadata(DefaultNumDeliveredKey))
		attrs = append(attrs, attribute.String("stream_source.message.id", msg.Uuid()))
		attrs = append(attrs, attribute.String("stream_source.message.received.subject", subject))
		attrs = append(attrs, attribute.String("stream_source.consumer.name", ss.config.consumerName))
		attrs = append(attrs, attribute.String("stream_source.consumer.durable", ss.config.durableName))
		attrs = append(attrs, attribute.String("stream_source.message.delivered", msg.GetMessageMetadata(DefaultNumDeliveredKey)))
		attrs = append(attrs, attribute.String("stream_source.message.pending", msg.GetMessageMetadata(DefaultNumPendingKey)))
		attrs = append(attrs, attribute.Int("stream_source.message.len", len(msg.Payload())))

		ss.mu.RLock()
		closed := ss.closed
		ss.mu.RUnlock()
		if closed {
			ss.logger.Ctx(msgSpanCtx).Debug("closed, message discarded", zap.String("stream_source", ss.name))
			return
		}

		select {
		case <-ss.closing:
			// Todo set status
			ss.logger.Ctx(msgSpanCtx).Debug("closing, message discarded", zap.String("stream_source", ss.name))
			return
		case <-msgSpanCtx.Done():
			// Todo set status
			// Context cancelled (source shutdown or trace context), discard the message.
			ss.logger.Ctx(msgSpanCtx).Debug("Context cancelled before sending to output, message discarded.", zap.String("stream_source", ss.name))
			// No need to Ack/Nak/Term here explicitly due to shutdown. NATS will handle redelivery based on MaxDeliver/AckWait.
			return // Exit worker goroutine
		case ss.out <- msg:
			ss.logger.Ctx(msgSpanCtx).Debug("message send to output channel", zap.String("stream_source", ss.name))
		}

		timeout := time.NewTimer(ss.config.processAckWait)
		defer timeout.Stop()

		select {
		case <-msgSpanCtx.Done():
			ss.logger.Ctx(msgSpanCtx).Debug("context cancelled while waiting for processing Ack/Nack, message discarded.", zap.String("stream_source", ss.name))
			return
		case <-msg.Acked():
			ss.logger.Ctx(msgSpanCtx).Debug("Received internal Ack signal", zap.String("stream_source", ss.name))
			attrs = append(attrs, attribute.String("stream_source.message.result", "acked"))

			var err error
			if ss.config.ackAsync {
				err = job.jsMsg.Ack()
			} else {
				err = job.jsMsg.DoubleAck(msgSpanCtx)
			}

			if err != nil {
				ss.logger.Ctx(msgSpanCtx).Error("cannot send JetStream Ack", zap.Error(err), zap.String("stream_source", ss.name))
			} else {
				ss.logger.Ctx(msgSpanCtx).Debug("JetStream message acked", zap.String("stream_source", ss.name))
			}
			return
		case <-msg.Nacked():
			ss.logger.Ctx(msgSpanCtx).Debug("Received internal Nack signal", zap.String("stream_source", ss.name))
			attrs = append(attrs, attribute.String("stream_source.message.result", "nacked"))

			// Determine delay for NAK
			var nakDelay time.Duration = 0 // Default to Nak immediately (delay 0)
			var retryNum uint64 = 0

			if ss.config.nakDelay != nil {
				if numDelivered > 0 {
					retryNum = numDelivered // NumDelivered is the retry count (1-based)
					nakDelay = ss.config.nakDelay.WaitTime(retryNum)
					attrs = append(attrs, attribute.String("stream_source.message.calculated_delay", nakDelay.String()))
					attrs = append(attrs, attribute.Int64("stream_source.message.retryNum", int64(retryNum)))
				} else {
					ss.logger.Ctx(msgSpanCtx).Warn("Failed to get message metadata for NakDelay calculation, NAKing immediately")
				}
			} else {
				// No NakDelay configured, default to Nak immediately
				nakDelay = 0
			}

			// Send the final NAK or TERM back to JetStream
			var err error
			if nakDelay == TermSignal { // Defined as time.Duration(-1)
				ss.logger.Ctx(msgSpanCtx).Debug("NakDelay calculated as TermSignal, sending Term", zap.String("stream_source", ss.name))
				attrs = append(attrs, attribute.String("stream_source.message.action", "term"))
				err = job.jsMsg.Term() // Send Term signal
			} else if nakDelay > 0 {
				ss.logger.Ctx(msgSpanCtx).Debug("NakDelay calculated as positive, sending NakWithDelay", zap.Duration("delay", nakDelay), zap.String("stream_source", ss.name))
				attrs = append(attrs, attribute.String("stream_source.message.action", fmt.Sprintf("nak_with_delay_%s", nakDelay.String())))
				err = job.jsMsg.NakWithDelay(nakDelay) // Send Nak with calculated delay
			} else { // nakDelay <= 0, including explicit 0
				ss.logger.Ctx(msgSpanCtx).Debug("NakDelay calculated as zero or negative, sending Nak immediately", zap.String("stream_source", ss.name))
				attrs = append(attrs, attribute.String("stream_source.message.action", "nak_immediately"))
				err = job.jsMsg.Nak() // Send Nak immediately (or 0 delay)
			}

			if err != nil {
				ss.logger.Ctx(msgSpanCtx).Error("cannot send JetStream Nak/Term", zap.Error(err), zap.String("stream_source", ss.name))
				// Log the error, worker exits. NATS might redeliver based on MaxDeliver/AckWait.
			} else {
				ss.logger.Ctx(msgSpanCtx).Debug("JetStream message Nacked/Termed", zap.String("stream_source", ss.name))
			}
			return // Exit worker goroutine after successful Nak/Term or error
		case <-timeout.C:
			// Downstream processor did not call Ack() or Nack() within processAckWait.
			ss.logger.Ctx(msgSpanCtx).Debug("Internal processAckWait timeout occurred", zap.String("stream_source", ss.name))
			attrs = append(attrs, attribute.String("stream_source.message.result", "timeout"))
			attrs = append(attrs, attribute.Float64("stream_source.message.timeout_duration", ss.config.processAckWait.Seconds()))

			// Policy on timeout: NAK the message so NATS can redeliver it.
			// Use the default NAK behavior (often no delay, relies on BackOff).
			err := job.jsMsg.Nak()
			if err != nil {
				ss.logger.Ctx(msgSpanCtx).Error("cannot send JetStream Nak after timeout", zap.Error(err), zap.String("stream_source", ss.name))
			} else {
				ss.logger.Ctx(msgSpanCtx).Debug("JetStream message Nacked due to timeout", zap.String("stream_source", ss.name))
			}
			return // Exit worker goroutine after timeout handling
		case <-ss.closing:
			ss.logger.Ctx(msgCtx).Debug("closing, message discarded before Ack", zap.String("stream_source", ss.name))
			err := job.jsMsg.Nak()
			if err != nil {
				ss.logger.Ctx(msgSpanCtx).Error("cannot send JetStream Nak after closing", zap.Error(err))
			} else {
				ss.logger.Ctx(msgSpanCtx).Debug("JetStream message Nacked due to closing", zap.String("stream_source", ss.name))
			}
			return
		}
	}, ants.RoundRobin)

	return ss, nil
}

func (ss *StreamSource[T]) Name() string {
	return ss.name
}

func (ss *StreamSource[T]) Via(operator flow.Flow) flow.Flow {
	flow.DoStream(ss, operator)
	return operator
}

func (ss *StreamSource[T]) Out() <-chan any {
	return ss.out
}

func (ss *StreamSource[T]) Run() error {
	ss.wg.Add(1)
	go func() {
		defer ss.wg.Done()
		ss.process(ss.ctx) // assuming process takes ctx
	}()

	return nil
}

func (ss *StreamSource[T]) RunCtx(ctx context.Context) error {
	ss.wg.Add(1)
	go func() {
		defer ss.wg.Done()
		ss.process(ctx) // assuming process takes ctx
	}()

	return nil
}

func (ss *StreamSource[T]) Close(ctx context.Context) error {
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
		ss.closeOnce.Do(func() { ss.cancel() })
		return nil
	}
}

func (ss *StreamSource[T]) AwaitCompletion(timeout time.Duration) error {
	fmt.Println("StreamSource.AwaitCompletion...")
	deadline := time.After(timeout)

	for {
		select {
		case <-deadline:
			fmt.Println("deadline...")
			return fmt.Errorf("timeout waiting for channel to empty")
		default:
			fmt.Println("default...")
			if len(ss.out) == 0 && ss.pool.Running() == 0 {
				return nil
			}

			runtime.Gosched()
		}
	}
}

func (ss *StreamSource[T]) PurgeStream() error {
	return ss.stream.Purge(ss.ctx)
}

func (ss *StreamSource[T]) Complete() error {
	<-ss.ctx.Done()
	return ss.ctx.Err()
}

func (ss *StreamSource[T]) process(ctx context.Context) {
	processCtx, cancelProcess := context.WithCancel(context.Background())
	defer cancelProcess()

	go func() {
		select {
		case <-ss.closing:
			ss.logger.Ctx(ctx).Debug("closing signaling process stop", zap.String("stream_source", ss.name))
		case <-ctx.Done():
			ss.logger.Ctx(ctx).Debug("RunCtx cancelled, signaling process stop", zap.String("stream_source", ss.name))
		case <-ss.ctx.Done():
			ss.logger.Ctx(ss.ctx).Debug("StreamSource internal context cancelled, signaling process stop", zap.String("stream_source", ss.name))
		}
		cancelProcess() // Cancel the processCtx
	}()

	consumeContext, err := ss.consumer.Consume(func(msg jetstream.Msg) {
		job := &streamSourceJob{
			ctx:   processCtx,
			jsMsg: msg,
		}
		ss.wg.Add(1)
		if err := ss.pool.Invoke(job); err != nil {
			ss.wg.Done()
			ss.logger.Ctx(processCtx).Error("failed to process message", zap.Error(err), zap.String("stream_source", ss.name))
			if err := msg.Nak(); err != nil {
				ss.logger.Ctx(processCtx).Error("failed to NAK unprocessable message", zap.Error(err), zap.String("stream_source", ss.name))
			}
			return
		}
	}, ss.config.pullOptions...)
	if err != nil {
		ss.logger.Ctx(processCtx).Error("failed to consume msg from consumer", zap.Error(err), zap.String("stream_source", ss.name))
		return
	}

	defer func() {
		consumeContext.Drain()
	}()

	<-processCtx.Done()
	ss.logger.Ctx(processCtx).Debug("process finished")
}

var _ flow.Source = (*StreamSource[flow.MessageData])(nil)
