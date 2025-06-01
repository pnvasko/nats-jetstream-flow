package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pnvasko/nats-jetstream-flow/coordination"
	"github.com/pnvasko/nats-jetstream-flow/examples/handlers"
	"github.com/pnvasko/nats-jetstream-flow/examples/models"
	"github.com/pnvasko/nats-jetstream-flow/proto/v1"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/uptrace-go/uptrace"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWatchMetrics(t *testing.T) {
	setEnvironment(t, serviceName)
	tc, err := getTestContext(t)
	require.NoError(t, err)
	defer tc.cancel()
	kv := prepareKVStore(t, tc, tc.ctx, bucketName)
	require.NotNil(t, kv)

	t.Run("WatchStore", func(t *testing.T) {
		t.Skip()
		mainCtx, mainSpan := tc.tracer.Start(tc.ctx, serviceName)
		defer func() {
			mainSpan.End()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = uptrace.ForceFlush(ctx)
			_ = uptrace.Shutdown(ctx)
		}()

		watchMetricsBucketName := bucketName
		watchMetricsScope := "workflow.metrics"

		labelFactory := func(params any) (string, error) {
			return "watch.metric.store", nil
		}
		opts := models.NewBaseMetricStoreOption(watchMetricsBucketName, watchMetricsScope)
		store, err := models.NewBaseMetricStore(mainCtx, tc.js, "board", labelFactory, tc.tracer, tc.logger, opts)
		require.NoError(t, err)
		require.NotNil(t, store)

		defer func() {
			store.Close()
			time.Sleep(100 * time.Millisecond)
			err = tc.js.DeleteKeyValue(mainCtx, bucketName) // Clean up bucket
			require.NoError(t, err)
		}()

		testLabel := "workflow.metrics.client"

		go func() {
			err := store.Watch(mainCtx, &coordination.LabelParams{Label: fmt.Sprintf("%s.*", testLabel)}, func(label string, metric *models.BaseMetric) {
				t.Logf("metrics_collection.Watch %s; %+v", label, metric)

			})
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Logf("watch error: %v", err)
			}
		}()

		updateIn := &models.BaseMetricInput{
			Counter:       1,
			FailedCounter: 10,
		}
		err = store.Update(mainCtx, &coordination.LabelParams{Label: fmt.Sprintf("%s.%d", testLabel, 1)}, updateIn)
		require.NoError(t, err)
		time.Sleep(1 * time.Second)
		keys, err := kv.Keys(mainCtx)
		for _, key := range keys {
			t.Logf("found key: %s", key)
		}
	})

	t.Run("WatchMetricsCollector", func(t *testing.T) {
		mainCtx, mainSpan := tc.tracer.Start(tc.ctx, serviceName)
		defer func() {
			mainSpan.End()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = uptrace.ForceFlush(ctx)
			_ = uptrace.Shutdown(ctx)
		}()

		watchMetricsBucketName := bucketName
		watchMetricsScope := "workflow.metrics"
		testLabel := "workflow.metrics"

		var metricsOpts []handlers.MetricsCollectorOption
		metricsOpts = append(metricsOpts, handlers.WithCleanupTTL(64*time.Hour))

		mcWatcher, err := handlers.NewMetricsCollector(mainCtx,
			"worker",
			watchMetricsBucketName,
			watchMetricsScope,
			func(labelBuilder *strings.Builder, params handlers.WorkflowLabelParams) error {
				if params.Scope != "" {
					labelBuilder.WriteString(params.Scope)
					labelBuilder.WriteString(".")
				}

				return nil
			},
			tc.js,
			tc.tracer,
			tc.logger,
			metricsOpts...,
		)
		require.NoError(t, err)

		mc, err := handlers.NewMetricsCollector(mainCtx,
			"scatterGather",
			watchMetricsBucketName,
			watchMetricsScope,
			func(labelBuilder *strings.Builder, params handlers.WorkflowLabelParams) error {
				if params.Scope != "" {
					labelBuilder.WriteString(params.Scope)
					labelBuilder.WriteString(".")
				}

				return nil
			},
			tc.js,
			tc.tracer,
			tc.logger,
			metricsOpts...,
		)
		require.NoError(t, err)
		err = mc.InitMetric()
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		defer func() {
			t.Logf("defer DeleteKeyValue\n")
			mcWatcher.Close(mainCtx)
			mc.Close(mainCtx)
			time.Sleep(100 * time.Millisecond)
			err = tc.js.DeleteKeyValue(mainCtx, bucketName) // Clean up bucket
			require.NoError(t, err)
		}()

		go func() {
			label := fmt.Sprintf("%s.board.>", testLabel)
			t.Logf("watching metrics collection.01 for %s", label)
			err := mcWatcher.WatchBaseStore(mainCtx, &coordination.LabelParams{Label: label}, func(label string, obj any) {
				t.Logf("metrics_collection.Watch.01: %s %+v\n", label, obj)
			})
			require.NoError(t, err)
		}()

		go func() {
			boardIICtx, cancel := context.WithTimeout(mainCtx, 200*time.Millisecond)
			defer cancel()
			label := fmt.Sprintf("%s.board.>", testLabel)
			t.Logf("watching metrics collection.02 for %s", label)
			err := mcWatcher.WatchBaseStore(boardIICtx, &coordination.LabelParams{Label: label}, func(label string, obj any) {
				t.Logf("metrics_collection.Watch.02: %s; %+v\n", label, obj)
			})
			if err != nil {
				t.Logf("metrics_collection.Watch.02 error: %+v\n", err)
			}
		}()

		go func() {
			label := fmt.Sprintf("%s.client.>", testLabel)
			t.Logf("watching metrics collection for %s", label)
			err := mcWatcher.WatchMapStore(mainCtx, &coordination.LabelParams{Label: label}, func(label string, obj any) {
				t.Logf("metrics_collection.Watch %s; %+v\n", label, obj)
			})
			require.NoError(t, err)
		}()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			search := &proto.Search{
				Id:        1,
				ClientId:  2,
				CompanyId: 3,
				UserId:    4,
				Boards:    []string{"ebay"},
			}
			for i := 0; i < 5; i++ {
				s := search
				s.Id = s.Id + 1
				err = mc.ScatterGatherSearchMetrics(mainCtx, s)
				require.NoError(t, err)
				time.Sleep(500 * time.Millisecond)
			}
		}()

		wg.Wait()

		time.Sleep(1000 * time.Millisecond)
		t.Logf("load store keys...")
		keys, err := kv.Keys(mainCtx)
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			t.Logf("no keys found")
			return
		}
		for _, key := range keys {
			t.Logf("found key: %s", key)
		}
		time.Sleep(11 * time.Second)
	})
}
