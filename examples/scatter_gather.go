package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	stream "github.com/pnvasko/nats-jetstream-flow"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"github.com/pnvasko/nats-jetstream-flow/flow"
	"github.com/pnvasko/nats-jetstream-flow/proto/v1"
	"github.com/rs/xid"
	"github.com/uptrace/uptrace-go/uptrace"
	"github.com/urfave/cli/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
	"math/rand"
	"os"
	"os/signal"
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

	parentMsgIdKey       = "_parent_msg_id_key"
	originalSearchIdKey  = "_original_search_id_key"
	originalBoardKey     = "_original_board_key"
	originalNatsMsgIdKey = "_original_nats_msg_id_key"
	debugFailedKey       = "_debug_failed_key"
)

var totalSearchesSend int32 = 0
var totalSearchesPull int32 = 0

func main() {
	cls := &cli.App{
		Name:  "scatter-gather-flow",
		Usage: "scatter-gather-flow runs scatter gather flow",
		Commands: []*cli.Command{
			serveScatterGatherFlow,
		},
		Before: func(ctx *cli.Context) (err error) {
			fmt.Println("before start service.")
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

var serveScatterGatherFlow = &cli.Command{
	Name:  "serve",
	Usage: "serve. run scatter gather flow...",
	Action: func(c *cli.Context) error {
		actionCtx, actionCancel := context.WithCancel(c.Context)
		defer actionCancel()

		otlpCfg, err := common.NewDevOtlpConfig()
		if err != nil {
			return err
		}
		if err := common.InitOpentelemetry(otlpCfg); err != nil {
			return err
		}
		logger, err := common.NewLogger(otlpCfg)
		if err != nil {
			return err
		}

		nc, err := nats.Connect(nats.DefaultURL)
		if err != nil {
			return err
		}
		js, err := jetstream.New(nc)
		if err != nil {
			return err
		}

		tracer := otel.Tracer("flow.map_reduce")
		var attributes []attribute.KeyValue
		attributes = append(attributes, attribute.Key("service").String("map_reduce_flow"))
		mainCtx, mainSpan := tracer.Start(actionCtx, "main",
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attributes...),
		)
		mainCtx, mainCancel := context.WithCancel(mainCtx)
		defer mainCancel()

		defer func() {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer shutdownCancel()

			if err := nc.Drain(); err != nil {
				logger.Ctx(shutdownCtx).Error("failed to drain nats connection", zap.Error(err))
			}

			mainSpan.End()

			if err := uptrace.ForceFlush(shutdownCtx); err != nil {
				logger.Ctx(shutdownCtx).Sugar().Errorf("failed to force uptrace flush: %s", err.Error())
			}
			if err := uptrace.Shutdown(shutdownCtx); err != nil {
				logger.Ctx(shutdownCtx).Sugar().Errorf("failed to uptrace shutdown: %s", err.Error())
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
		_, err = stream.CreateOrUpdateStream(mainCtx, js, streamConfig)
		if err != nil {
			return err
		}

		// Debug
		cdc, err := NewCdcEmitter(mainCtx, js, tracer, logger)
		if err != nil {
			return err
		}
		var streamWorkerSourceOpts []stream.StreamSourceConfigOption
		var consumerWorkerOpts []stream.ConsumerConfigOption
		consumerWorkerOpts = append(consumerWorkerOpts, stream.WithDurableName(defaultWorkflowSearchWorkerDurableName))
		consumerWorkerOpts = append(consumerWorkerOpts, stream.WithFilterSubjects([]string{fmt.Sprintf("%s.>", defaultWorkflowWorkerSubjects)}))
		streamWorkerSourceConfig, err := stream.NewJetStreamSourceConfig(js, streamSearchesName, streamSearchesSubjects, streamOpts, consumerWorkerOpts, streamWorkerSourceOpts...)
		if err != nil {
			return err
		}
		streamWorkerSource, err := stream.NewStreamSource[*proto.Search](mainCtx, "worker_search_source", js, streamWorkerSourceConfig, tracer, logger)
		if err != nil {
			return err
		}
		// Debug

		var streamSourceOpts []stream.StreamSourceConfigOption
		var consumerOpts []stream.ConsumerConfigOption
		consumerOpts = append(consumerOpts, stream.WithDurableName(streamSearchesDurableName))
		consumerOpts = append(consumerOpts, stream.WithFilterSubjects([]string{fmt.Sprintf("%s.*", defaultSearchCDCSubject)}))
		streamSourceConfig, err := stream.NewJetStreamSourceConfig(js, streamSearchesName, streamSearchesSubjects, streamOpts, consumerOpts, streamSourceOpts...)
		if err != nil {
			return err
		}
		streamSource, err := stream.NewStreamSource[*proto.Search](mainCtx, "distributor_search_source", js, streamSourceConfig, tracer, logger)
		if err != nil {
			return err
		}

		sinkConfig, err := stream.NewStreamSinkConfig(streamConfig)

		producerSplitFunction := func(ctx context.Context, js jetstream.JetStream, future *flow.Future[*stream.Message], tracer trace.Tracer, logger *common.Logger) error {
			producerSpanCtx, producerSpan := tracer.Start(ctx, fmt.Sprintf("distributor_search_sink.message.producer"))
			var attrs []attribute.KeyValue
			defer func() {
				producerSpan.SetAttributes(attrs...)
				producerSpan.End()
			}()
			original := future.Original()
			search := proto.Search{}
			err := search.UnmarshalVT(original.Payload())
			if err != nil {
				attrs = append(attrs, attribute.String("producer_function_test_attr", "test"))
				return stream.SetLogError(producerSpanCtx, "map producer unmarshal payload failed", err, logger)
			}

			subject, err := original.Subject()
			if err != nil {
				return stream.SetLogError(producerSpanCtx, "map producer get subject failed", err, logger)
			}
			attrs = append(attrs, attribute.String("subject", subject))
			uuid := xid.New().String()
			// msg, err := stream.NewMessage(uuid, original.Payload())
			//if err != nil {
			//	return stream.SetLogError(producerSpanCtx, "map producer new message failed", err, logger)
			//}
			msg, err := original.Copy()
			if err != nil {
				return stream.SetLogError(producerSpanCtx, "map producer copy message failed", err, logger)
			}
			msg.SetUuid(uuid)
			msg.SetSubject(subject)
			msg.SetContext(producerSpanCtx)
			natsMsgId := original.GetMessageMetadata(originalNatsMsgIdKey)
			msgData, err := stream.NewNatsMessage(msg)
			if err != nil {
				return stream.SetLogError(producerSpanCtx, "map producer new nats message failed", err, logger)
			}
			//fmt.Printf("original DefaultNumDeliveredKey %+v\n", original.GetMessageMetadata(stream.DefaultNumDeliveredKey))
			numDelivered := msg.GetMessageMetadata(stream.DefaultNumDeliveredKey)
			debugFailed := msg.GetMessageMetadata(debugFailedKey)
			if debugFailed == "true" && numDelivered == "1" {
				fmt.Printf("PublishMsgAsync numDelivered: %+v\n", numDelivered)
				fmt.Println("PublishMsgAsync msg debugFailedKey:", debugFailed)
				return stream.SetLogError(producerSpanCtx, "test failed PublishMsgAsync", fmt.Errorf("test error"), logger)
			}
			ack, err := js.PublishMsgAsync(msgData, jetstream.WithMsgID(natsMsgId))
			if err != nil {
				return stream.SetLogError(producerSpanCtx, "map producer publish error", err, logger)
			}

			select {
			case <-ack.Ok():
				return nil
			case err := <-ack.Err():
				return stream.SetLogError(producerSpanCtx, "map producer ack message failed", err, logger)
			case <-producerSpanCtx.Done():
				return producerSpanCtx.Err()
			}
		}

		sink, err := stream.NewStreamSinkProducer[*stream.Message](mainCtx, "distributor_search_sink", js, producerSplitFunction, sinkConfig, tracer, logger)
		if err != nil {
			return err
		}

		splitMapTransformer := func(message *stream.Message) (*flow.Messages, error) {
			splitMapSpanCtx, splitMapSpan := tracer.Start(message.Context(), fmt.Sprintf("distributor_search_map.message.split.transformer"))
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
			debugFailed := message.GetMessageMetadata(debugFailedKey)
			for i, board := range search.Boards {
				uuid := xid.New().String()
				// msg, err := stream.NewMessage(uuid, data)
				msg, err := message.Copy()
				if err != nil {
					return nil, err
				}
				// msg.SetMessageMetadata(debugFailedKey, "true")
				if debugFailed == "true" && i == 2 {
					msg.SetMessageMetadata(debugFailedKey, "true")
				} else {
					msg.SetMessageMetadata(debugFailedKey, "false")
				}

				subject := fmt.Sprintf(templateSearchWorkerSubject, board, search.Id)
				msg.SetUuid(uuid)
				msg.SetSubject(subject)
				msg.SetContext(splitMapSpanCtx)
				msg.SetMessageMetadata(parentMsgIdKey, message.Uuid())
				msg.SetMessageMetadata(originalSearchIdKey, strconv.FormatInt(int64(search.Id), 10))
				msg.SetMessageMetadata(originalBoardKey, board)
				natsMsgId := fmt.Sprintf("%s-%s-%s", message.Uuid(), strconv.Itoa(int(search.Id)), board)
				msg.SetMessageMetadata(originalNatsMsgIdKey, natsMsgId)
				msgs.Messages = append(msgs.Messages, msg)
			}

			return msgs, nil
		}

		toSplitMapFlow, err := flow.NewMap(mainCtx, splitMapTransformer, 100, tracer, logger)
		if err != nil {
			return err
		}

		logger.Ctx(mainCtx).Info("start scatter gather flow...")

		runGroup := common.NewRunGroup(10 * time.Second)
		_ = runGroup.Add("CdcEmitter", func() error {
			logger.Ctx(mainCtx).Sugar().Debugf("started cdc emitter.")
			return cdc.Run()
		}, func(err error) {
			logger.Ctx(c.Context).Sugar().Debugf("cdc emitter interrupt")
			if err != nil {
				logger.Ctx(c.Context).Error("cdc emitter exit error:", zap.Error(err))
			}
			runCtx, closeCtxCancel := context.WithTimeout(c.Context, 5*time.Second)
			defer closeCtxCancel()
			if err := cdc.Close(runCtx); err != nil {
				logger.Ctx(c.Context).Error("cdc emitter close error", zap.Error(err))
			}
		})

		_ = runGroup.Add("StreamSource", func() error {
			logger.Ctx(mainCtx).Sugar().Debugf("started stream source.")
			if err := streamSource.Run(); err != nil {
				return err
			}
			return streamSource.Complete()
		}, func(err error) {
			logger.Ctx(c.Context).Sugar().Debugf("stream source interrupt")
			if err != nil {
				logger.Ctx(c.Context).Error("stream source exit error:", zap.Error(err))
			}
			runCtx, closeCtxCancel := context.WithTimeout(c.Context, 60*time.Second)
			defer closeCtxCancel()
			if err := streamSource.Close(runCtx); err != nil {
				logger.Ctx(c.Context).Error("stream source close error", zap.Error(err))
			}
		})

		_ = runGroup.Add("Sink", func() error {
			logger.Ctx(mainCtx).Sugar().Debugf("started stream sink.")
			if err := sink.Run(); err != nil {
				return err
			}
			return sink.Complete()
		}, func(err error) {
			logger.Ctx(c.Context).Sugar().Debugf("stream sink interrupt")
			if err != nil {
				logger.Ctx(c.Context).Error("stream sink exit error:", zap.Error(err))
			}
			runCtx, closeCtxCancel := context.WithTimeout(c.Context, 60*time.Second)
			defer closeCtxCancel()
			if err := sink.Close(runCtx); err != nil {
				logger.Ctx(c.Context).Error("stream sink close error", zap.Error(err))
			}
		})

		_ = runGroup.Add("Flow", func() error {
			logger.Ctx(mainCtx).Sugar().Debugf("started stream flow.")
			if err := toSplitMapFlow.Run(); err != nil {
				return err
			}
			streamSource.
				Via(flow.NewPassThrough()).
				Via(toSplitMapFlow).
				To(sink)
			return nil
		}, func(err error) {
			logger.Ctx(c.Context).Sugar().Debugf("stream flow interrupt")
		})

		_ = runGroup.Add("WorkerSource", func() error {
			logger.Ctx(mainCtx).Sugar().Debugf("started worker stream flow.")
			if err := streamWorkerSource.Run(); err != nil {
				return err
			}

			if err := streamSource.PurgeStream(); err != nil {
				logger.Ctx(mainCtx).Sugar().Errorf("purge worker stream error: %v", err)
			} else {
				logger.Ctx(mainCtx).Sugar().Warnf("worker stream purged successfully.")
			}

			workerSink := flow.NewChanSink(mainCtx)
			err = workerSink.Run()
			if err != nil {
				return err
			}

			waitWorkerSinkOut := make(chan struct{})
			go func() {
				defer close(waitWorkerSinkOut)

				for {
					select {
					case <-mainCtx.Done():
						return
					case jsMsg, ok := <-workerSink.Out():
						if !ok {
							logger.Ctx(mainCtx).Sugar().Debugf("workerSink. worker sink closed")
						}
						msg, ok := jsMsg.(*stream.Message)
						if !ok {
							logger.Ctx(mainCtx).Sugar().Debugf("workerSink get stream message failed, not suuport message type: %T", msg)
							return
						}

						subject, err := msg.Subject()
						if err != nil {
							logger.Ctx(mainCtx).Sugar().Debugf("workerSink get stream message subject failed, not suuport message type: %T", msg)
							continue
						}
						atomic.AddInt32(&totalSearchesPull, 1)
						if ok := msg.Ack(); ok {
							fmt.Printf("workerSink. ack stream message success subject: %s\n", subject)
							logger.Ctx(mainCtx).Sugar().Debugf("workerSink. ack stream message success subject: %s", subject)
						} else {
							logger.Ctx(mainCtx).Sugar().Errorf("workerSink. ack stream message failed subject: %s", subject)
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
				logger.Ctx(mainCtx).Sugar().Debugf("worker sink error, waitWorkerSinkOut closed")
				return fmt.Errorf("worker sink error, worker sink out closed")
			case <-waitStreamWorkerFlow:
				logger.Ctx(mainCtx).Sugar().Debugf("worker sink error, waitStreamWorkerFlow closed")
			}
			return nil
		}, func(err error) {
			logger.Ctx(c.Context).Sugar().Debugf("stream flow interrupt")
		})

		_ = runGroup.Add("SystemInterrupt", func() error {
			logger.Ctx(actionCtx).Sugar().Debugf("started system interrupt.")
			term := make(chan os.Signal, 32)
			signal.Notify(term, os.Interrupt, unix.SIGINT, unix.SIGQUIT, unix.SIGTERM)
			select {
			case <-term:
			case <-actionCtx.Done():
			}
			return nil
		}, func(err error) {
			logger.Ctx(actionCtx).Sugar().Debugf("system interrupted")
			if err != nil && !(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
				logger.Ctx(actionCtx).Error("system interrupt error:", zap.Error(err))
			}
			mainCancel()
		})

		if err := runGroup.Run(mainCtx); err != nil && !(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			logger.Ctx(actionCtx).Sugar().Errorf("run group error: %v", err)
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
					msg.SetMessageMetadata(debugFailedKey, "true")
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
