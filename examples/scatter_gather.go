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
	"github.com/pnvasko/nats-jetstream-flow/examples/workers"
	"github.com/pnvasko/nats-jetstream-flow/flow"
	"github.com/pnvasko/nats-jetstream-flow/proto/v1"
	"github.com/rs/xid"
	"github.com/uptrace/uptrace-go/uptrace"
	"github.com/urfave/cli/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	totalSearch = 50
	// Workflow Stream.

	// Worker activity

	parentMsgIdKey      = "_parent_msg_id_key"
	originalSearchIdKey = "_original_search_id_key"
	originalBoardKey    = "_original_board_key"
)

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
			serveSearchCollector,
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

			streamSearchesName := workers.DefaultWorkflowStreamName
			streamSearchesCleanupTtl := workers.DefaultWorkflowStreamCleanupTtl

			var streamSearchesSubjects []string
			for _, sub := range strings.Split(workers.DefaultWorkflowStreamSubjects, ";") {
				sub = strings.Trim(sub, " ")
				if strings.HasSuffix(sub, " ") || strings.HasPrefix(sub, " ") {
					fmt.Printf("invalid subject: %s\n", sub)
				}
				streamSearchesSubjects = append(streamSearchesSubjects, sub)
			}
			if len(streamSearchesSubjects) == 0 {
				return fmt.Errorf("subjects list for stream [%s] is empty", workers.DefaultWorkflowStreamName)
			}

			var streamOpts []stream.StreamConfigOption
			streamOpts = append(streamOpts, stream.WithCleanupTtl(streamSearchesCleanupTtl))
			streamConfig, err := stream.NewStreamConfig(streamSearchesName, streamSearchesSubjects, streamOpts...)
			_, err = stream.CreateOrUpdateStream(sc.ctx, sc.js, streamConfig)
			if err != nil {
				return err
			}

			//cg, err := coordination.NewCounterGaugeStore(sc.ctx, sc.js, sc.tracer, sc.logger)
			//if err != nil {
			//	return err
			//}
			//if err := cg.Reset(sc.ctx, handlers.LabelSearchNew); err != nil {
			//	return err
			//}
			//if err := cg.Reset(sc.ctx, handlers.LabelSearchBoard); err != nil {
			//	return err
			//}

			return nil
		},
		After: func(ctx *cli.Context) error {
			fmt.Println("after stop service.")
			// fmt.Printf("total searches send/pull: %d/%d\n", atomic.LoadInt32(&totalSearchesSend), atomic.LoadInt32(&totalSearchesPull))
			sc, err := initService(context.Background(), "workflow.map_reduce.after_test", "workflow_map_reduce_after", "after_main")
			if err != nil {
				return err
			}
			_ = sc.Shutdown()

			//cg, err := coordination.NewCounterGaugeStore(sc.ctx, sc.js, sc.tracer, sc.logger)
			//if err != nil {
			//	return err
			//}
			//rawCounterSearchNew, err := cg.Read(sc.ctx, handlers.LabelSearchNew)
			//if err != nil {
			//	return err
			//}
			//counterSearchNew, ok := rawCounterSearchNew.(*coordination.CounterGauge)
			//if !ok {
			//	return fmt.Errorf("failed to cast counter record to coordination.CounterGauge %T", rawCounterSearchNew)
			//}
			//fmt.Printf("after counter search.new expected: %d, actual: %d [%d]\n", totalSearch, counterSearchNew.Counter, counterSearchNew.Gauge)
			//
			//rawCounterSearchBoard, err := cg.Read(sc.ctx, handlers.LabelSearchBoard)
			//if err != nil {
			//	return err
			//}
			//counterSearchBoard, ok := rawCounterSearchBoard.(*coordination.CounterGauge)
			//if !ok {
			//	return fmt.Errorf("failed to cast counter record to coordination.CounterGauge %T", rawCounterSearchBoard)
			//}
			//fmt.Printf("after counter search.board expected: %d, actual: %d [%d]\n", atomic.LoadInt32(&totalSearchesSend), counterSearchBoard.Counter, counterSearchBoard.Gauge)

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

		cdc, err := workers.NewCdcEmitter(sc.ctx, sc.js, sc.tracer, sc.logger)
		if err != nil {
			return err
		}

		sc.logger.Ctx(sc.ctx).Info("start emulate cdc flow...")

		runGroup, err := common.NewRunGroup(common.WithStopTimeout(10 * time.Second))
		if err != nil {
			return err
		}

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

		worker, err := workers.NewBatchSearch(sc.ctx, sc.js, sc.tracer, sc.logger)

		runGroup, err := common.NewRunGroup(common.WithStopTimeout(10 * time.Second))
		if err != nil {
			return err
		}

		_ = runGroup.Add("WorkerSource", func() error {
			return worker.Run()
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

var serveSearchCollector = &cli.Command{
	Name:  "serve-search-collector",
	Usage: "serve-search-collector. run search collector flow...",
	Action: func(c *cli.Context) error {
		sc, err := initService(c.Context, "workflow.map_reduce.worker", "workflow_search_collector_worker", "worker_main")
		if err != nil {
			return err
		}
		defer func() {
			if err := sc.Shutdown(); err != nil {
				fmt.Printf("failed to shutdown service: %s\n", err.Error())
			}
		}()

		collectorWorker, err := workers.NewSearchCollector(sc.ctx, sc.js, sc.tracer, sc.logger)

		runGroup, err := common.NewRunGroup(common.WithStopTimeout(10 * time.Second))
		if err != nil {
			return err
		}

		_ = runGroup.Add("CollectorWorker", func() error {
			return collectorWorker.Run()
		}, func(err error) {
			sc.logger.Ctx(c.Context).Sugar().Debugf("collector worker flow interrupt")
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

		metricsBucketName := workers.DefaultWorkflowMetricsBucketName
		metricsScope := workers.DefaultWorkflowMetricsScope

		// todo debug
		// err = sc.js.DeleteKeyValue(sc.ctx, metricsBucketName)
		// if err != nil {
		//	return err
		// }

		var metricsOpts []handlers.MetricsCollectorOption
		metricsOpts = append(metricsOpts, handlers.WithCleanupTTL(64*time.Hour))
		mc, err := handlers.NewMetricsCollector(sc.ctx,
			"scatterGather",
			metricsBucketName,
			metricsScope,
			func(labelBuilder *strings.Builder, params handlers.WorkflowLabelParams) error {
				if params.Scope != "" {
					labelBuilder.WriteString(params.Scope)
					labelBuilder.WriteString(".")
				}

				return nil
			},
			sc.js,
			sc.tracer,
			sc.logger,
			metricsOpts...,
		)
		if err != nil {
			return err
		}
		defer mc.Close(sc.ctx)

		if err := mc.InitSearchMetrics(); err != nil {
			return err
		}

		streamSearchesName := workers.DefaultWorkflowStreamName
		var streamSearchesSubjects []string
		for _, sub := range strings.Split(workers.DefaultWorkflowStreamSubjects, ";") {
			sub = strings.Trim(sub, " ")
			if strings.HasSuffix(sub, " ") || strings.HasPrefix(sub, " ") {
				fmt.Printf("invalid subject: %s\n", sub)
			}
			streamSearchesSubjects = append(streamSearchesSubjects, sub)
		}
		if len(streamSearchesSubjects) == 0 {
			return fmt.Errorf("subjects list for stream [%s] is empty", workers.DefaultWorkflowStreamName)
		}

		streamSearchesCleanupTtl := workers.DefaultWorkflowStreamCleanupTtl
		streamSearchesDurableName := workers.DefaultWorkflowSearchDistributorDurableName

		var streamOpts []stream.StreamConfigOption
		streamOpts = append(streamOpts, stream.WithCleanupTtl(streamSearchesCleanupTtl))
		streamConfig, err := stream.NewStreamConfig(streamSearchesName, streamSearchesSubjects, streamOpts...)
		if err != nil {
			return err
		}

		var streamSourceOpts []stream.StreamSourceConfigOption
		var consumerOpts []stream.ConsumerConfigOption
		consumerOpts = append(consumerOpts, stream.WithDurableName(streamSearchesDurableName))
		consumerOpts = append(consumerOpts, stream.WithFilterSubjects([]string{fmt.Sprintf("%s.*", workers.DefaultSearchCDCSubject)}))
		streamSourceConfig, err := stream.NewJetStreamSourceConfig(sc.js, streamSearchesName, streamSearchesSubjects, streamOpts, consumerOpts, streamSourceOpts...)
		if err != nil {
			return err
		}
		streamSource, err := stream.NewStreamSource[*proto.Search](sc.ctx, "distributor_search_source", sc.js, streamSourceConfig, sc.tracer, sc.logger)
		if err != nil {
			return err
		}

		// Todo debug
		if err := streamSource.PurgeStream(); err != nil {
			sc.logger.Ctx(sc.ctx).Sugar().Errorf("purge worker stream error: %v", err)
		} else {
			sc.logger.Ctx(sc.ctx).Sugar().Warnf("worker stream purged successfully.")
		}
		// Todo debug

		sinkConfig, err := stream.NewStreamSinkConfig(streamConfig)
		if err != nil {
			return err
		}

		sinkHandlers, err := handlers.NewSinkHandlers[*stream.Message](sc.ctx, sc.js, mc, sc.tracer, sc.logger)
		if err != nil {
			return err
		}

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

				subject := fmt.Sprintf(workers.TemplateSearchWorkerSubject, board, search.Id)
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

		// TODO collector
		// collector

		sc.logger.Ctx(sc.ctx).Info("start scatter gather flow...")

		runGroup, err := common.NewRunGroup(common.WithStopTimeout(10 * time.Second))
		if err != nil {
			return err
		}

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
