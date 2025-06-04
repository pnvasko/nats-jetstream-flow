package workers

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"github.com/pnvasko/nats-jetstream-flow/coordination"
	"github.com/pnvasko/nats-jetstream-flow/examples/handlers"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"strings"
	"sync"
	"time"
)

type SearchCollector struct {
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	wg        common.SafeWaitGroup

	js jetstream.JetStream

	tracer trace.Tracer
	logger common.Logger
}

func NewSearchCollector(ctx context.Context, js jetstream.JetStream, tracer trace.Tracer, logger common.Logger) (*SearchCollector, error) {
	c := &SearchCollector{
		js:     js,
		tracer: tracer,
		logger: logger,
	}
	c.ctx, c.cancel = context.WithCancel(ctx)
	return c, nil
}

func (c *SearchCollector) Run() error {
	ctxRun, runSpan := c.tracer.Start(c.ctx, "collector.run")
	defer runSpan.End()

	metricsBucketName := DefaultWorkflowMetricsBucketName
	metricsScope := DefaultWorkflowMetricsScope

	var metricsOpts []handlers.MetricsCollectorOption
	metricsOpts = append(metricsOpts, handlers.WithCleanupTTL(64*time.Hour))
	mc, err := handlers.NewMetricsCollector(
		ctxRun,
		"collector",
		metricsBucketName,
		metricsScope,
		func(labelBuilder *strings.Builder, params handlers.WorkflowLabelParams) error {
			if params.Scope != "" {
				labelBuilder.WriteString(params.Scope)
				labelBuilder.WriteString(".")
			}

			return nil
		},
		c.js,
		c.tracer,
		c.logger,
		metricsOpts...,
	)
	if err != nil {
		return err
	}
	defer mc.Close(c.ctx)
	go func() {
		if err := mc.WatchMapStore(ctxRun, &coordination.LabelParams{Label: "workflow.metrics.client.*"}, func(label string, obj any) {
			fmt.Printf("collector.client.Watch :%s %+v\n", label, obj)
		}); err != nil {
			c.logger.Ctx(ctxRun).Error("client metrics collection watch error", zap.Error(err))
		}
	}()

	go func() {
		if err := mc.WatchMapStore(ctxRun, &coordination.LabelParams{Label: "workflow.metrics.search.*"}, func(label string, obj any) {
			fmt.Printf("collector.search.Watch :%s %+v\n", label, obj)
		}); err != nil {
			c.logger.Ctx(ctxRun).Error("search metrics collection watch error", zap.Error(err))
		}
	}()

	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	case <-ctxRun.Done():
		return ctxRun.Err()
	}
}

func (c *SearchCollector) Close(ctx context.Context) error {
	waitChan := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(waitChan)
	}()

	select {
	case <-ctx.Done():
		c.closeOnce.Do(func() { c.cancel() })
		return ctx.Err()
	case <-waitChan:
		c.closeOnce.Do(func() { c.cancel() })
		return nil
	}
}

func (c *SearchCollector) Done() <-chan struct{} {
	return c.ctx.Done()
}

var _ Worker = (*SearchCollector)(nil)
