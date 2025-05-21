package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	stream "github.com/pnvasko/nats-jetstream-flow"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"github.com/uptrace/uptrace-go/uptrace"
	"github.com/urfave/cli/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
	"os"
	"os/signal"
	"strings"
	"time"
)

func main() {
	cls := &cli.App{
		Name:  "map-reduce-flow",
		Usage: "map-reduce-flow runs map reduce flow",
		Commands: []*cli.Command{
			serveMapReducesFlow,
		},
		Before: func(ctx *cli.Context) (err error) {
			fmt.Println("before start service.")
			return nil
		},
		After: func(ctx *cli.Context) error {
			fmt.Println("after stop service.")
			return nil
		},
	}

	if err := cls.Run(os.Args); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

var serveMapReducesFlow = &cli.Command{
	Name:  "serve",
	Usage: "serve. run map reduces flow...",
	Action: func(c *cli.Context) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

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
		ctx, main := tracer.Start(ctx, "main",
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attributes...),
		)

		streamName := "wordcount"
		subjects := []string{"job.>"}
		reducerSubtectsTemplate := []string{"job.reduce.%s"}
		cleanupTtl := 3600 * time.Second

		var streamOpts []stream.StreamConfigOption
		streamOpts = append(streamOpts, stream.WithCleanupTtl(cleanupTtl))
		streamConfig, err := stream.NewStreamConfig(streamName, subjects, streamOpts...)

		_, err = stream.CreateOrUpdateStream(ctx, js, streamConfig)
		if err != nil {
			return err
		}

		mappers, err := NewMapper(ctx, js, tracer, logger)
		if err != nil {
			return err
		}

		reducer, err := NewReducer(ctx, js, streamName, reducerSubtectsTemplate, tracer, logger)
		if err != nil {
			return err
		}

		aggregator, err := NewAggregator(ctx, js, tracer, logger)
		if err != nil {
			return err
		}

		defer func() {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer shutdownCancel()
			if err := nc.Drain(); err != nil {
				logger.Ctx(shutdownCtx).Error("failed to drain nats connection", zap.Error(err))
			}

			if err := mappers.Close(shutdownCtx); err != nil {
				logger.Ctx(shutdownCtx).Error("failed to close mappers", zap.Error(err))
			}

			if err := reducer.Close(shutdownCtx); err != nil {
				logger.Ctx(shutdownCtx).Error("failed to close reducer", zap.Error(err))
			}

			if err := aggregator.Close(shutdownCtx); err != nil {
				logger.Ctx(shutdownCtx).Error("failed to close aggregator", zap.Error(err))
			}

			main.End()

			if err := uptrace.ForceFlush(shutdownCtx); err != nil {
				logger.Ctx(shutdownCtx).Sugar().Errorf("failed to force uptrace flush: %s", err.Error())
			}

			if err := uptrace.Shutdown(shutdownCtx); err != nil {
				logger.Ctx(shutdownCtx).Sugar().Errorf("failed to uptrace shutdown: %s", err.Error())
			}
		}()

		logger.Ctx(ctx).Info("start map reduce flow...")

		var runGroup common.RunGroup

		runGroup.Add("Mappers", func() error {
			logger.Ctx(ctx).Sugar().Debugf("started mappers.")
			return mappers.Run()
		}, func(err error) {
			logger.Ctx(ctx).Sugar().Debugf("mappers.interrupt")
			if err != nil {
				logger.Ctx(ctx).Error("mappers error:", zap.Error(err))
			}
			if err := mappers.Close(c.Context); err != nil {
				logger.Ctx(ctx).Error("mappers close error", zap.Error(err))
			}
		})

		runGroup.Add("Reducer", func() error {
			logger.Ctx(ctx).Sugar().Debugf("started reducer.")
			return reducer.Run()
		}, func(err error) {
			logger.Ctx(ctx).Sugar().Debugf("reducer.interrupt")
			if err != nil {
				logger.Ctx(ctx).Error("reducer error:", zap.Error(err))
			}
			if err := reducer.Close(c.Context); err != nil {
				logger.Ctx(ctx).Error("reducer close error", zap.Error(err))
			}
		})

		runGroup.Add("Aggregator", func() error {
			logger.Ctx(ctx).Sugar().Debugf("started aggregator.")
			return aggregator.Run()
		}, func(err error) {
			logger.Ctx(ctx).Sugar().Debugf("aggregator.interrupt")
			if err != nil {
				logger.Ctx(ctx).Error("aggregator error:", zap.Error(err))
			}
			if err := reducer.Close(c.Context); err != nil {
				logger.Ctx(ctx).Error("aggregator close error", zap.Error(err))
			}
		})

		runGroup.Add("SystemInterrupt", func() error {
			logger.Ctx(ctx).Sugar().Debugf("started system interrupt.")
			term := make(chan os.Signal, 32)
			signal.Notify(term, os.Interrupt, unix.SIGINT, unix.SIGQUIT, unix.SIGTERM)
			select {
			case <-term:
			case <-c.Context.Done():
			case <-ctx.Done():
			}

			return nil
		}, func(err error) {
			logger.Ctx(ctx).Sugar().Debugf("started system interrupted")
			if err != nil {
				logger.Ctx(ctx).Error("system interrupt error:", zap.Error(err))
			}
			cancel()
		})

		runCtx, runCtxCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer func() {
			runCtxCancel()
		}()
		if err := runGroup.Run(runCtx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			logger.Ctx(ctx).Sugar().Errorf("runGroup error:%v", err)
			return err
		}
		return nil
	},
}

type Mapper struct {
	ctx    context.Context
	cancel context.CancelFunc

	js jetstream.JetStream

	tracer trace.Tracer
	logger *common.Logger
}

func NewMapper(ctx context.Context, js jetstream.JetStream, tracer trace.Tracer, logger *common.Logger) (*Mapper, error) {
	m := &Mapper{
		js:     js,
		tracer: tracer,
		logger: logger,
	}

	m.ctx, m.cancel = context.WithCancel(ctx)
	return m, nil
}

func (m *Mapper) Run() error {
	ctxRun, runSpan := m.tracer.Start(m.ctx, "mapper.run")
	defer runSpan.End()

	jobID := "wordcount123"
	subtect := fmt.Sprintf("job.reduce.%s", jobID)
	text := "The quic brown fox jamp over lazy dog"
	words := strings.Fields(text)
	for _, word := range words {
		msg := fmt.Sprintf("%s:1", word)
		ack, err := m.js.Publish(m.ctx, subtect, []byte(msg))
		if err != nil {
			return err
		}
		m.logger.Ctx(m.ctx).Sugar().Infof("Mapper.Run.Sequence: %d", ack.Sequence)
	}

	select {
	case <-ctxRun.Done():
		return ctxRun.Err()
	}
}

func (m *Mapper) Close(ctx context.Context) error {
	m.cancel()
	return nil
}

type Reducer struct {
	ctx    context.Context
	cancel context.CancelFunc

	streamName string
	subjects   []string

	js       jetstream.JetStream
	consumer jetstream.Consumer
	tracer   trace.Tracer
	logger   *common.Logger
}

func NewReducer(ctx context.Context, js jetstream.JetStream, streamName string, subjects []string, tracer trace.Tracer, logger *common.Logger) (*Reducer, error) {
	r := &Reducer{
		js:         js,
		streamName: streamName,
		subjects:   subjects,
		tracer:     tracer,
		logger:     logger,
	}
	r.ctx, r.cancel = context.WithCancel(ctx)
	jobID := "wordcount123"

	queueGroup := fmt.Sprintf("reduce-worker")
	var consumerSubjects []string
	for _, subject := range subjects {
		consumerSubjects = append(consumerSubjects, fmt.Sprintf(subject, jobID))
	}

	var consumerOpts []stream.ConsumerConfigOption
	consumerOpts = append(consumerOpts, stream.WithDurableName(queueGroup))
	config, err := stream.NewConsumerConfig(streamName, consumerSubjects, consumerOpts...)
	if err != nil {
		return nil, err
	}

	r.consumer, err = stream.CreateOrUpdateConsumer(r.ctx, r.js, config)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (r *Reducer) Run() error {
	ctxRun, runSpan := r.tracer.Start(r.ctx, "reducer.run")
	defer runSpan.End()
	consumeContext, err := r.consumer.Consume(func(msg jetstream.Msg) {
		meta, err := msg.Metadata()
		if err != nil {
			return
		}
		fmt.Printf("Reducer.Run.Consume %s. %d, %d\n", msg.Subject(), meta.Sequence.Stream, meta.NumDelivered)
		if err := msg.Ack(); err != nil {
			r.logger.Ctx(r.ctx).Sugar().Errorf("failed to ack message: %s", err.Error())
		}
	})
	if err != nil {
		return err
	}
	defer func() {
		consumeContext.Drain()
	}()

	select {
	case <-ctxRun.Done():
		return ctxRun.Err()
	}
}

func (r *Reducer) Close(ctx context.Context) error {
	r.cancel()
	return nil
}

type Aggregator struct {
	ctx    context.Context
	cancel context.CancelFunc

	js     jetstream.JetStream
	tracer trace.Tracer
	logger *common.Logger
}

func NewAggregator(ctx context.Context, js jetstream.JetStream, tracer trace.Tracer, logger *common.Logger) (*Aggregator, error) {
	a := &Aggregator{
		js:     js,
		tracer: tracer,
		logger: logger,
	}
	a.ctx, a.cancel = context.WithCancel(ctx)
	return a, nil
}

func (a *Aggregator) Run() error {
	ctxRun, runSpan := a.tracer.Start(a.ctx, "aggregator.run")
	defer runSpan.End()

	select {
	case <-ctxRun.Done():
		return a.ctx.Err()
	}
}

func (a *Aggregator) Close(ctx context.Context) error {
	a.cancel()
	return nil
}
