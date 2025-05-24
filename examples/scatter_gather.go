package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	stream "github.com/pnvasko/nats-jetstream-flow"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"github.com/pnvasko/nats-jetstream-flow/examples/handlers"
	"github.com/pnvasko/nats-jetstream-flow/flow"
	"github.com/pnvasko/nats-jetstream-flow/proto/v1"
	"github.com/rs/xid"
	"github.com/uptrace/uptrace-go/uptrace"
	"github.com/urfave/cli/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (

	// Workflow Stream.
	defaultWorkflowStreamName       = "workflow"
	defaultWorkflowStreamSubjects   = "  distributor.>;  worker.>;collector.>"
	defaultWorkflowStreamCleanupTtl = 3600 * time.Second

	// Distributor activity.
	defaultWorkflowSearchDistributorDurableName = "distributor_searches"

	// CDC activity.
	defaultSearchCDCSubject = "distributor.search.new"

	// Worker activity
	defaultWorkflowSearchWorkerDurableName = "worker_searches"
	templateSearchWorkerSubject            = "worker.%s.search.new.%d"
	defaultWorkflowWorkerSubjects          = "worker"

	parentMsgIdKey      = "_parent_msg_id_key"
	originalSearchIdKey = "_original_search_id_key"
	originalBoardKey    = "_original_board_key"
)

var totalSearchesSend int32 = 0
var totalSearchesPull int32 = 0

type serviceContext struct {
	ctx          context.Context
	cancel       context.CancelFunc
	shutdownOnce sync.Once
	nc           *nats.Conn
	js           jetstream.JetStream
	span         trace.Span
	tracer       trace.Tracer
	logger       *common.Logger
}

func initService(ctx context.Context, tracerName, serviceName, mainSpanName string, opts ...trace.SpanStartOption) (*serviceContext, error) {
	var err error
	sc := &serviceContext{}
	otlpCfg, err := common.NewDevOtlpConfig()
	if err != nil {
		return nil, err
	}
	if err := common.InitOpentelemetry(otlpCfg); err != nil {
		return nil, err
	}

	sc.logger, err = common.NewLogger(otlpCfg)
	if err != nil {
		return nil, err
	}

	sc.nc, err = nats.Connect(nats.DefaultURL)
	if err != nil {
		return nil, err
	}
	sc.js, err = jetstream.New(sc.nc)
	if err != nil {
		return nil, err
	}
	//tracerName = "workflow.map_reduce.cdc"
	//serviceName = "workflow_map_reduce_cdc"
	//mainSpanName = "cdc_main"

	if tracerName == "" || serviceName == "" || mainSpanName == "" {
		return nil, fmt.Errorf("tracer name, service name, and span name must be provided")
	}

	sc.tracer = otel.Tracer(tracerName)
	var attributes []attribute.KeyValue
	attributes = append(attributes, attribute.Key("service").String(serviceName))

	var spanOpts []trace.SpanStartOption
	spanOpts = append(spanOpts, opts...)
	spanOpts = append(spanOpts, trace.WithSpanKind(trace.SpanKindServer))
	spanOpts = append(spanOpts, trace.WithAttributes(attributes...))

	mainCtx, mainSpan := sc.tracer.Start(ctx, mainSpanName, spanOpts...)
	sc.ctx, sc.cancel = context.WithCancel(mainCtx)
	sc.span = mainSpan

	return sc, nil
}

func (sc *serviceContext) Shutdown() error {
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	complete := make(chan struct{})
	errCh := make(chan error, 1)

	go func() {
		sc.shutdownOnce.Do(func() {
			defer close(complete)

			if sc.cancel != nil {
				sc.cancel()
			}

			if sc.nc != nil && !sc.nc.IsClosed() {
				if err := sc.nc.Drain(); err != nil {
					errCh <- fmt.Errorf("failed to drain nats connection %w", err)
					return
				}
			}

			sc.logger.Ctx(sc.ctx).Info("service shutdown")

			if sc.span != nil {
				sc.span.End()
			}

			if err := uptrace.ForceFlush(shutdownCtx); err != nil {
				errCh <- fmt.Errorf("failed to force uptrace flush %w", err)
				return
			}

			if err := uptrace.Shutdown(shutdownCtx); err != nil {
				errCh <- fmt.Errorf("failed to uptrace shutdown %w", err)
				return
			}
		})
	}()

	for {
		select {
		case <-shutdownCtx.Done():
			// sc.logger.Ctx(shutdownCtx).Sugar().Errorf("failed shutdown service: %s", shutdownCtx.Err())
			return shutdownCtx.Err()
		case err := <-errCh:
			return err
		case <-complete:
			return nil
		}
	}
}

func main() {
	cls := &cli.App{
		Name:  "scatter-gather-flow",
		Usage: "scatter-gather-flow runs scatter gather flow",
		Commands: []*cli.Command{
			serveCdcFlow,
			serveScatterGatherFlow,
			serveWorkerFlow,
		},
		Before: func(ctx *cli.Context) (err error) {
			fmt.Println("before start service.")
			sc, err := initService(context.Background(), "workflow.map_reduce.init", "workflow_map_reduce_init", "init_main")
			if err != nil {
				return err
			}
			defer func() {
				if err := sc.Shutdown(); err != nil {
					fmt.Printf("failed to shutdown service: %s\n", err.Error())
				}
			}()

			streamSearchesName := defaultWorkflowStreamName
			streamSearchesCleanupTtl := defaultWorkflowStreamCleanupTtl

			var streamSearchesSubjects []string
			for _, sub := range strings.Split(defaultWorkflowStreamSubjects, ";") {
				sub = strings.Trim(sub, " ")
				if strings.HasSuffix(sub, " ") || strings.HasPrefix(sub, " ") {
					fmt.Printf("invalid subject: %s\n", sub)
				}
				streamSearchesSubjects = append(streamSearchesSubjects, sub)
			}
			if len(streamSearchesSubjects) == 0 {
				return fmt.Errorf("subjects list for stream [%s] is empty", defaultWorkflowStreamName)
			}

			var streamOpts []stream.StreamConfigOption
			streamOpts = append(streamOpts, stream.WithCleanupTtl(streamSearchesCleanupTtl))
			streamConfig, err := stream.NewStreamConfig(streamSearchesName, streamSearchesSubjects, streamOpts...)
			_, err = stream.CreateOrUpdateStream(sc.ctx, sc.js, streamConfig)
			if err != nil {
				return err
			}

			return nil
		},
		After: func(ctx *cli.Context) error {
			fmt.Println("after stop service.")
			fmt.Printf("total searches send/pull: %d/%d\n", atomic.LoadInt32(&totalSearchesSend), atomic.LoadInt32(&totalSearchesPull))
			return nil
		},
	}

	if err := cls.Run(os.Args); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

var serveCdcFlow = &cli.Command{
	Name:  "serve-cdc",
	Usage: "serve-cdc. run cdc flow...",
	Action: func(c *cli.Context) error {
		sc, err := initService(c.Context, "workflow.map_reduce.cdc", "workflow_map_reduce_cdc", "cdc_main")
		if err != nil {
			return err
		}
		defer func() {
			if err := sc.Shutdown(); err != nil {
				fmt.Printf("failed to shutdown service: %s\n", err.Error())
			}
		}()

		cdc, err := NewCdcEmitter(sc.ctx, sc.js, sc.tracer, sc.logger)
		if err != nil {
			return err
		}

		sc.logger.Ctx(sc.ctx).Info("start emulate cdc flow...")

		runGroup := common.NewRunGroup(true, 10*time.Second)

		_ = runGroup.Add("CdcEmitter", func() error {
			sc.logger.Ctx(sc.ctx).Sugar().Debugf("started cdc emitter.")
			return cdc.Run()
		}, func(err error) {
			sc.logger.Ctx(c.Context).Sugar().Debugf("cdc emitter interrupt")
			if err != nil {
				sc.logger.Ctx(c.Context).Error("cdc emitter exit error:", zap.Error(err))
			}
			runCtx, closeCtxCancel := context.WithTimeout(c.Context, 5*time.Second)
			defer closeCtxCancel()
			if err := cdc.Close(runCtx); err != nil {
				sc.logger.Ctx(c.Context).Error("cdc emitter close error", zap.Error(err))
			}
		})

		if err := runGroup.Run(sc.ctx); err != nil && !(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			sc.logger.Ctx(sc.ctx).Sugar().Errorf("run group error: %v", err)
			return err
		}

		return nil
	},
}

var serveWorkerFlow = &cli.Command{
	Name:  "serve-worker",
	Usage: "serve-worker. run worker flow...",
	Action: func(c *cli.Context) error {
		sc, err := initService(c.Context, "workflow.map_reduce.worker", "workflow_map_reduce_worker", "worker_main")
		if err != nil {
			return err
		}
		defer func() {
			if err := sc.Shutdown(); err != nil {
				fmt.Printf("failed to shutdown service: %s\n", err.Error())
			}
		}()

		streamSearchesName := defaultWorkflowStreamName
		streamSearchesCleanupTtl := defaultWorkflowStreamCleanupTtl
		var streamSearchesSubjects []string
		for _, sub := range strings.Split(defaultWorkflowStreamSubjects, ";") {
			sub = strings.Trim(sub, " ")
			if strings.HasSuffix(sub, " ") || strings.HasPrefix(sub, " ") {
				fmt.Printf("invalid subject: %s\n", sub)
			}
			streamSearchesSubjects = append(streamSearchesSubjects, sub)
		}
		if len(streamSearchesSubjects) == 0 {
			return fmt.Errorf("subjects list for stream [%s] is empty", defaultWorkflowStreamName)
		}

		var streamOpts []stream.StreamConfigOption
		streamOpts = append(streamOpts, stream.WithCleanupTtl(streamSearchesCleanupTtl))

		var streamWorkerSourceOpts []stream.StreamSourceConfigOption
		var consumerWorkerOpts []stream.ConsumerConfigOption
		consumerWorkerOpts = append(consumerWorkerOpts, stream.WithDurableName(defaultWorkflowSearchWorkerDurableName))
		consumerWorkerOpts = append(consumerWorkerOpts, stream.WithFilterSubjects([]string{fmt.Sprintf("%s.>", defaultWorkflowWorkerSubjects)}))
		streamWorkerSourceConfig, err := stream.NewJetStreamSourceConfig(sc.js, streamSearchesName, streamSearchesSubjects, streamOpts, consumerWorkerOpts, streamWorkerSourceOpts...)
		if err != nil {
			return err
		}

		streamWorkerSource, err := stream.NewStreamSource[*proto.Search](sc.ctx, "worker_search_source", sc.js, streamWorkerSourceConfig, sc.tracer, sc.logger)
		if err != nil {
			return err
		}

		sc.logger.Ctx(sc.ctx).Info("start worker flow...")

		runGroup := common.NewRunGroup(true, 10*time.Second)

		_ = runGroup.Add("WorkerSource", func() error {
			sc.logger.Ctx(sc.ctx).Sugar().Debugf("started worker stream flow.")
			if err := streamWorkerSource.Run(); err != nil {
				return err
			}

			workerSink := flow.NewChanSink(sc.ctx)
			err = workerSink.Run()
			if err != nil {
				return err
			}

			waitWorkerSinkOut := make(chan struct{})
			go func() {
				defer close(waitWorkerSinkOut)

				for {
					select {
					case <-sc.ctx.Done():
						return
					case jsMsg, ok := <-workerSink.Out():
						if !ok {
							sc.logger.Ctx(sc.ctx).Sugar().Debugf("workerSink. worker sink closed")
						}
						msg, ok := jsMsg.(*stream.Message)
						if !ok {
							sc.logger.Ctx(sc.ctx).Sugar().Debugf("workerSink get stream message failed, not suuport message type: %T", msg)
							return
						}

						subject, err := msg.Subject()
						if err != nil {
							sc.logger.Ctx(sc.ctx).Sugar().Debugf("workerSink get stream message subject failed, not suuport message type: %T", msg)
							continue
						}
						atomic.AddInt32(&totalSearchesPull, 1)
						if ok := msg.Ack(); ok {
							fmt.Printf("workerSink. ack stream message success subject: %s\n", subject)
							sc.logger.Ctx(sc.ctx).Sugar().Debugf("workerSink. ack stream message success subject: %s", subject)
						} else {
							sc.logger.Ctx(sc.ctx).Sugar().Errorf("workerSink. ack stream message failed subject: %s", subject)
						}
					}
				}
			}()

			waitStreamWorkerFlow := make(chan struct{})
			go func() {
				defer func() {
					close(waitStreamWorkerFlow)
				}()

				streamWorkerSource.
					Via(flow.NewPassThrough()).
					To(workerSink)
			}()

			select {
			case <-waitWorkerSinkOut:
				sc.logger.Ctx(sc.ctx).Sugar().Debugf("worker sink error, waitWorkerSinkOut closed")
				return fmt.Errorf("worker sink error, worker sink out closed")
			case <-waitStreamWorkerFlow:
				sc.logger.Ctx(sc.ctx).Sugar().Debugf("worker sink error, waitStreamWorkerFlow closed")
			}
			return nil
		}, func(err error) {
			sc.logger.Ctx(c.Context).Sugar().Debugf("stream worker flow interrupt")
		})

		if err := runGroup.Run(sc.ctx); err != nil && !(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			sc.logger.Ctx(sc.ctx).Sugar().Errorf("run group error: %v", err)
			return err
		}

		return nil
	},
}

var serveScatterGatherFlow = &cli.Command{
	Name:  "serve-scatter-gather",
	Usage: "serve-scatter-gather. run scatter gather flow...",
	Action: func(c *cli.Context) error {
		sc, err := initService(c.Context, "workflow.map_reduce.cdc", "workflow_map_reduce_cdc", "cdc_main")
		if err != nil {
			return err
		}
		defer func() {
			if err := sc.Shutdown(); err != nil {
				fmt.Printf("failed to shutdown service: %s\n", err.Error())
			}
		}()

		streamSearchesName := defaultWorkflowStreamName
		var streamSearchesSubjects []string
		for _, sub := range strings.Split(defaultWorkflowStreamSubjects, ";") {
			sub = strings.Trim(sub, " ")
			if strings.HasSuffix(sub, " ") || strings.HasPrefix(sub, " ") {
				fmt.Printf("invalid subject: %s\n", sub)
			}
			streamSearchesSubjects = append(streamSearchesSubjects, sub)
		}
		if len(streamSearchesSubjects) == 0 {
			return fmt.Errorf("subjects list for stream [%s] is empty", defaultWorkflowStreamName)
		}

		streamSearchesCleanupTtl := defaultWorkflowStreamCleanupTtl
		streamSearchesDurableName := defaultWorkflowSearchDistributorDurableName

		var streamOpts []stream.StreamConfigOption
		streamOpts = append(streamOpts, stream.WithCleanupTtl(streamSearchesCleanupTtl))
		streamConfig, err := stream.NewStreamConfig(streamSearchesName, streamSearchesSubjects, streamOpts...)
		if err != nil {
			return err
		}

		var streamSourceOpts []stream.StreamSourceConfigOption
		var consumerOpts []stream.ConsumerConfigOption
		consumerOpts = append(consumerOpts, stream.WithDurableName(streamSearchesDurableName))
		consumerOpts = append(consumerOpts, stream.WithFilterSubjects([]string{fmt.Sprintf("%s.*", defaultSearchCDCSubject)}))
		streamSourceConfig, err := stream.NewJetStreamSourceConfig(sc.js, streamSearchesName, streamSearchesSubjects, streamOpts, consumerOpts, streamSourceOpts...)
		if err != nil {
			return err
		}
		streamSource, err := stream.NewStreamSource[*proto.Search](sc.ctx, "distributor_search_source", sc.js, streamSourceConfig, sc.tracer, sc.logger)
		if err != nil {
			return err
		}

		if err := streamSource.PurgeStream(); err != nil {
			sc.logger.Ctx(sc.ctx).Sugar().Errorf("purge worker stream error: %v", err)
		} else {
			sc.logger.Ctx(sc.ctx).Sugar().Warnf("worker stream purged successfully.")
		}

		sinkConfig, err := stream.NewStreamSinkConfig(streamConfig)

		sinkHandlers := handlers.NewSinkHandlers[*stream.Message](sc.js, sc.tracer, sc.logger)

		sink, err := stream.NewStreamSinkProducer[*stream.Message](sc.ctx,
			"distributor_search_sink",
			sc.js,
			sinkHandlers.ProducerHandler,
			sinkHandlers.CompletionHandler,
			sinkConfig,
			sc.tracer,
			sc.logger,
		)
		if err != nil {
			return err
		}

		splitMapTransformer := func(message *stream.Message) (*flow.Messages, error) {
			splitMapSpanCtx, splitMapSpan := sc.tracer.Start(message.Context(), fmt.Sprintf("distributor_search_map.message.split.transformer"))
			var attrs []attribute.KeyValue
			defer func() {
				splitMapSpan.SetAttributes(attrs...)
				splitMapSpan.End()
			}()

			search, ok := message.Data().(*proto.Search)
			if !ok {
				return nil, errors.New("split map transformer failed, invalid data type")
			}

			if len(search.Boards) == 0 {
				return nil, nil
			}

			//data, err := search.MarshalVT()
			//if err != nil {
			//	return nil, err
			//}

			msgs := flow.NewMessages(message)
			debugFailed := message.GetMessageMetadata(handlers.DebugFailedKey)
			for i, board := range search.Boards {
				uuid := xid.New().String()
				// msg, err := stream.NewMessage(uuid, data)
				msg, err := message.Copy()
				if err != nil {
					return nil, err
				}
				// msg.SetMessageMetadata(debugFailedKey, "true")
				if debugFailed == "true" && i == 2 {
					msg.SetMessageMetadata(handlers.DebugFailedKey, "true")
				} else {
					msg.SetMessageMetadata(handlers.DebugFailedKey, "false")
				}

				subject := fmt.Sprintf(templateSearchWorkerSubject, board, search.Id)
				msg.SetUuid(uuid)
				msg.SetSubject(subject)
				msg.SetContext(splitMapSpanCtx)
				msg.SetMessageMetadata(parentMsgIdKey, message.Uuid())
				msg.SetMessageMetadata(originalSearchIdKey, strconv.FormatInt(int64(search.Id), 10))
				msg.SetMessageMetadata(originalBoardKey, board)
				natsMsgId := fmt.Sprintf("%s-%s-%s", message.Uuid(), strconv.Itoa(int(search.Id)), board)
				msg.SetMessageMetadata(handlers.OriginalNatsMsgIdKey, natsMsgId)
				msgs.Messages = append(msgs.Messages, msg)
			}

			return msgs, nil
		}

		toSplitMapFlow, err := flow.NewMap(sc.ctx, splitMapTransformer, 100, sc.tracer, sc.logger)
		if err != nil {
			return err
		}

		sc.logger.Ctx(sc.ctx).Info("start scatter gather flow...")

		runGroup := common.NewRunGroup(true, 10*time.Second)

		_ = runGroup.Add("StreamSource", func() error {
			sc.logger.Ctx(sc.ctx).Sugar().Debugf("started stream source.")
			if err := streamSource.Run(); err != nil {
				return err
			}
			return streamSource.Complete()
		}, func(err error) {
			sc.logger.Ctx(c.Context).Sugar().Debugf("stream source interrupt")
			if err != nil {
				sc.logger.Ctx(c.Context).Error("stream source exit error:", zap.Error(err))
			}
			runCtx, closeCtxCancel := context.WithTimeout(c.Context, 60*time.Second)
			defer closeCtxCancel()
			if err := streamSource.Close(runCtx); err != nil {
				sc.logger.Ctx(c.Context).Error("stream source close error", zap.Error(err))
			}
		})

		_ = runGroup.Add("Sink", func() error {
			sc.logger.Ctx(sc.ctx).Sugar().Debugf("started stream sink.")
			if err := sink.Run(); err != nil {
				return err
			}
			return sink.Complete()
		}, func(err error) {
			sc.logger.Ctx(c.Context).Sugar().Debugf("stream sink interrupt")
			if err != nil {
				sc.logger.Ctx(c.Context).Error("stream sink exit error:", zap.Error(err))
			}
			runCtx, closeCtxCancel := context.WithTimeout(c.Context, 60*time.Second)
			defer closeCtxCancel()
			if err := sink.Close(runCtx); err != nil {
				sc.logger.Ctx(c.Context).Error("stream sink close error", zap.Error(err))
			}
		})

		_ = runGroup.Add("Flow", func() error {
			sc.logger.Ctx(sc.ctx).Sugar().Debugf("started stream flow.")
			if err := toSplitMapFlow.Run(); err != nil {
				return err
			}
			streamSource.
				Via(flow.NewPassThrough()).
				Via(toSplitMapFlow).
				To(sink)
			return nil
		}, func(err error) {
			sc.logger.Ctx(c.Context).Sugar().Debugf("stream flow interrupt")
		})

		if err := runGroup.Run(sc.ctx); err != nil && !(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			sc.logger.Ctx(sc.ctx).Sugar().Errorf("run group error: %v", err)
			return err
		}

		return nil
	},
}

type Worker interface {
	Run() error
	Close(ctx context.Context) error
	Done() <-chan struct{}
}

type CdcSource struct {
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	wg        common.SafeWaitGroup
	js        jetstream.JetStream

	tracer trace.Tracer
	logger *common.Logger
}

func NewCdcEmitter(ctx context.Context, js jetstream.JetStream, tracer trace.Tracer, logger *common.Logger) (*CdcSource, error) {
	cdc := &CdcSource{
		js:     js,
		tracer: tracer,
		logger: logger,
	}
	cdc.ctx, cdc.cancel = context.WithCancel(ctx)
	return cdc, nil
}

func (cdc *CdcSource) Run() error {
	ctxRun, runSpan := cdc.tracer.Start(cdc.ctx, "cdcSource.run")
	defer runSpan.End()

	allBoards := []string{"amazon", "ebay", "walmart", "shopify", "alibaba", "wish"}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	randomElement := func(slice []string) string {
		if len(slice) == 0 {
			return ""
		}
		return slice[rng.Intn(len(slice))]
	}

	countTicker := time.NewTicker(5 * 60 * time.Second)
	defer countTicker.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	i := 1
	var id int32 = 1
	for {
		select {
		case <-ctxRun.Done():
			return ctxRun.Err()
		case <-countTicker.C:
			fmt.Printf("cdc source ticker %d run %d\n", i, id)
			i++
		case <-ticker.C:
			complete := make(chan struct{})
			errCh := make(chan error)
			go func() {
				cdc.logger.Ctx(ctxRun).Sugar().Debugf("cdc source ticker run %d", id)
				cdcSpanCtx, cdcSpan := cdc.tracer.Start(context.Background(), fmt.Sprintf("cdc.message.producer"))
				var attrs []attribute.KeyValue
				defer func() {
					cdcSpan.SetAttributes(attrs...)
					cdcSpan.End()
				}()

				s := proto.Search{
					Id:       id,
					ClientId: 1,
					Boards:   []string{},
				}
				totalBoards := rng.Intn(len(allBoards))
				if totalBoards == 0 {
					totalBoards = 1
				}
				boards := map[string]struct{}{}

				i := 0
				for {
					if i >= totalBoards {
						break
					}
					board := randomElement(allBoards)
					if _, ok := boards[board]; ok {
						continue
					}
					boards[board] = struct{}{}
					s.Boards = append(s.Boards, board)
					i++
				}

				data, err := s.MarshalVT()
				if err != nil {
					errCh <- stream.SetLogError(cdcSpanCtx, "marshal message failed", err, cdc.logger)
					return
				}
				msgId := xid.New().String()
				msg, err := stream.NewMessage(msgId, data)
				if err != nil {
					errCh <- stream.SetLogError(cdcSpanCtx, "create new stream message failed", err, cdc.logger)
					return
				}
				msg.SetSubject(fmt.Sprintf("%s.%d", defaultSearchCDCSubject, id))
				msg.SetContext(cdcSpanCtx)
				if len(boards) >= 3 {
					msg.SetMessageMetadata(handlers.DebugFailedKey, "true")
				}
				msgData, err := stream.NewNatsMessage(msg)
				if err != nil {
					errCh <- stream.SetLogError(cdcSpanCtx, "create new nats message failed", err, cdc.logger)
					return

				}
				_, err = cdc.js.PublishMsgAsync(msgData)
				if err != nil {
					errCh <- stream.SetLogError(cdcSpanCtx, "publish message failed", err, cdc.logger)
					return
				}
				// fmt.Printf("cdc source ticker search_id: %d; msg_id %s: boards %s\n", id, msgId, strings.Join(s.Boards, ","))
				attrs = append(attrs, attribute.String("msg_id", msgId))
				attrs = append(attrs, attribute.String("msg_board", strings.Join(s.Boards, ",")))
				atomic.AddInt32(&totalSearchesSend, int32(len(s.Boards)))
				id++
				if id > 2 {
					errCh <- nil
				}
				close(complete)
			}()

			select {
			case <-ctxRun.Done():
				return ctxRun.Err()
			case err := <-errCh:
				ticker.Stop()
				time.Sleep(5000 * time.Millisecond)
				return err
			case <-complete:
				continue
			}
		}
	}
}

func (cdc *CdcSource) Close(ctx context.Context) error {
	waitChan := make(chan struct{})
	go func() {
		cdc.wg.Wait()
		close(waitChan)
	}()

	select {
	case <-ctx.Done():
		cdc.closeOnce.Do(func() { cdc.cancel() })
		return ctx.Err()
	case <-waitChan:
		cdc.closeOnce.Do(func() { cdc.cancel() })
		return nil
	}
}

func (cdc *CdcSource) Done() <-chan struct{} {
	return cdc.ctx.Done()
}

var _ Worker = (*CdcSource)(nil)
