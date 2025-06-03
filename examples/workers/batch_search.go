package workers

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	stream "github.com/pnvasko/nats-jetstream-flow"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"github.com/pnvasko/nats-jetstream-flow/flow"
	"github.com/pnvasko/nats-jetstream-flow/proto/v1"
	"go.opentelemetry.io/otel/trace"
	"strings"
	"sync"
)

type BatchSearch struct {
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	wg        common.SafeWaitGroup

	js jetstream.JetStream

	tracer trace.Tracer
	logger *common.Logger
}

func NewBatchSearch(ctx context.Context, js jetstream.JetStream, tracer trace.Tracer, logger *common.Logger) (*BatchSearch, error) {
	bs := &BatchSearch{
		js:     js,
		tracer: tracer,
		logger: logger,
	}
	bs.ctx, bs.cancel = context.WithCancel(ctx)
	return bs, nil
}

func (bs *BatchSearch) Run() error {
	ctxRun, runSpan := bs.tracer.Start(bs.ctx, "batch.search.run")
	defer runSpan.End()
	//metricsBucketName := DefaultWorkflowMetricsBucketName
	//metricsScope := DefaultWorkflowMetricsScope
	spanSource := fmt.Sprintf("%s_source", DefaultBatchSearchWorkerSpanScope)
	streamSearchesName := DefaultWorkflowStreamName
	streamSearchesCleanupTtl := DefaultWorkflowStreamCleanupTtl
	var streamSearchesSubjects []string
	for _, sub := range strings.Split(DefaultWorkflowStreamSubjects, ";") {
		sub = strings.Trim(sub, " ")
		if strings.HasSuffix(sub, " ") || strings.HasPrefix(sub, " ") {
			fmt.Printf("invalid subject: %s\n", sub)
		}
		streamSearchesSubjects = append(streamSearchesSubjects, sub)
	}
	if len(streamSearchesSubjects) == 0 {
		return fmt.Errorf("batch search subjects list for stream [%s] is empty", DefaultWorkflowStreamName)
	}
	var streamOpts []stream.StreamConfigOption
	streamOpts = append(streamOpts, stream.WithCleanupTtl(streamSearchesCleanupTtl))

	var streamWorkerSourceOpts []stream.StreamSourceConfigOption
	var consumerWorkerOpts []stream.ConsumerConfigOption
	consumerWorkerOpts = append(consumerWorkerOpts, stream.WithDurableName(DefaultWorkflowSearchWorkerDurableName))
	consumerWorkerOpts = append(consumerWorkerOpts, stream.WithFilterSubjects([]string{fmt.Sprintf("%s.>", DefaultWorkflowWorkerSubjects)}))
	streamWorkerSourceConfig, err := stream.NewJetStreamSourceConfig(bs.js, streamSearchesName, streamSearchesSubjects, streamOpts, consumerWorkerOpts, streamWorkerSourceOpts...)
	if err != nil {
		return err
	}
	streamWorkerSource, err := stream.NewStreamSource[*proto.Search](ctxRun, spanSource, bs.js, streamWorkerSourceConfig, bs.tracer, bs.logger)
	if err != nil {
		return err
	}
	bs.logger.Ctx(ctxRun).Info("start batch search flow...")

	if err := streamWorkerSource.Run(); err != nil {
		return err
	}
	workerSink := flow.NewChanSink(ctxRun)
	err = workerSink.Run()
	if err != nil {
		return err
	}

	waitWorkerSinkOut := make(chan struct{})
	go func() {
		defer close(waitWorkerSinkOut)

		for {
			select {
			case <-ctxRun.Done():
				return
			case jsMsg, ok := <-workerSink.Out():
				if !ok {
					bs.logger.Ctx(ctxRun).Sugar().Debugf("batch search worker sink closed")
				}
				msg, ok := jsMsg.(*stream.Message)
				if !ok {
					bs.logger.Ctx(ctxRun).Sugar().Debugf("batch search get stream message failed, not suuport message type: %T", msg)
					return
				}

				subject, err := msg.Subject()
				if err != nil {
					bs.logger.Ctx(ctxRun).Sugar().Debugf("batch search get stream message subject failed, not suuport message type: %T", msg)
					continue
				}

				if ok := msg.Ack(); ok {
					fmt.Printf("batch search ack stream message success subject: %s\n", subject)
					bs.logger.Ctx(ctxRun).Sugar().Debugf("batch search ack stream message success subject: %s", subject)
				} else {
					bs.logger.Ctx(ctxRun).Sugar().Errorf("batch search ack stream message failed subject: %s", subject)
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
		bs.logger.Ctx(ctxRun).Sugar().Debugf("batch search sink error, worker sink closed")
		return fmt.Errorf("worker sink error, worker sink out closed")
	case <-waitStreamWorkerFlow:
		bs.logger.Ctx(ctxRun).Sugar().Debugf("worker sink error, worker flow closed")
	}

	return nil
}

func (bs *BatchSearch) Close(ctx context.Context) error {
	waitChan := make(chan struct{})
	go func() {
		bs.wg.Wait()
		close(waitChan)
	}()

	select {
	case <-ctx.Done():
		bs.closeOnce.Do(func() { bs.cancel() })
		return ctx.Err()
	case <-waitChan:
		bs.closeOnce.Do(func() { bs.cancel() })
		return nil
	}
}

func (bs *BatchSearch) Done() <-chan struct{} {
	return bs.ctx.Done()
}
