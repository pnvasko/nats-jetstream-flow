package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"github.com/pnvasko/nats-jetstream-flow/coordination"
	"github.com/pnvasko/nats-jetstream-flow/examples/handlers"
	"github.com/pnvasko/nats-jetstream-flow/examples/models"
	"github.com/pnvasko/nats-jetstream-flow/proto/v1"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/uptrace-go/uptrace"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	serviceName = "test.ObjectStore.Benchmark"
	tracerName  = "test.ObjectStore.Benchmark"
	bucketName  = "metrics_bench"
	numWorkers  = 2
)

// Idea program arguments: -test.benchmem
func BenchmarkMetrics(b *testing.B) {
	b.ReportAllocs()
	setEnvironment(b, serviceName)
	tc, err := getTestContext(b)
	require.NoError(b, err)
	defer tc.cancel()

	b.Run("TestBaseMetric", func(b *testing.B) {
		mainCtx, mainSpan := tc.tracer.Start(tc.ctx, serviceName)
		require.NotNil(b, mainSpan)
		defer func() {
			mainSpan.End()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = uptrace.ForceFlush(ctx)
			_ = uptrace.Shutdown(ctx)
		}()
		kv := prepareKVStore(b, tc, mainCtx)
		require.NotNil(b, kv)
		//companyTotal := 10
		//usersTotal := 100
		//boardTotal := 5
		bucketName := "metrics"
		scope := "test_scope_create"
		opts := models.NewBaseMetricStoreOption(bucketName, scope)
		m, err := models.NewBaseMetricStore(mainCtx, tc.js, "board", nil, tc.tracer, tc.logger, opts)
		require.NoError(b, err)
		require.NotNil(b, m)
		for i := 0; i < numWorkers; i++ {
			label := fmt.Sprintf("board.%d", i)
			err = m.Reset(tc.ctx, &coordination.LabelParams{Label: label})
			require.NoError(b, err)
		}

		for i := 0; i < numWorkers; i++ {
			label := fmt.Sprintf("board.%d", i)
			value, err := m.Read(tc.ctx, &coordination.LabelParams{Label: label})
			require.NoError(b, err)
			require.Equal(b, uint64(0), value.Processed)
			require.Equal(b, int64(0), value.InFlight)
			require.Equal(b, uint64(0), value.Failed)
		}

		var wg sync.WaitGroup

		watchCtx, watchCancel := context.WithCancel(mainCtx)
		go func() {
			err = m.Watch(watchCtx, &coordination.LabelParams{Label: "board.*"}, func(metric *models.BaseMetric) {
				// fmt.Printf("board: %+v\n", metric)
				_ = metric
			})
			require.ErrorAs(b, err, &context.Canceled)
		}()

		b.ResetTimer()
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for j := 0; j < b.N/numWorkers; j++ {
					label := fmt.Sprintf("board.%d", workerID)
					incrReq := &models.BaseMetricInput{
						Counter:       1,
						FailedCounter: 0,
					}
					err = m.Update(tc.ctx, &coordination.LabelParams{Label: label}, incrReq)
					require.NoError(b, err)
				}
			}(i)
		}

		wg.Wait()
		b.StopTimer()
		watchCancel()

		for i := 0; i < numWorkers; i++ {
			label := fmt.Sprintf("board.%d", i)
			value, err := m.Read(tc.ctx, &coordination.LabelParams{Label: label})
			require.NoError(b, err)
			b.Logf("board.%d: %+v", i, value)
		}
	})
}

func TestMetrics(t *testing.T) {
	setEnvironment(t, serviceName)
	tc, err := getTestContext(t)
	require.NoError(t, err)
	defer tc.cancel()

	t.Run("BaseMetricNewObjectStore", func(t *testing.T) {
		mainCtx, mainSpan := tc.tracer.Start(tc.ctx, serviceName)
		defer func() {
			mainSpan.End()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = uptrace.ForceFlush(ctx)
			_ = uptrace.Shutdown(ctx)
		}()

		scope := "test_scope_create"
		labelFactory := func(params any) (string, error) {
			return "NewMapMetricStoreOption", nil
		}
		opts := models.NewBaseMetricStoreOption(bucketName, scope)
		store, err := models.NewBaseMetricStore(mainCtx, tc.js, "board", labelFactory, tc.tracer, tc.logger, opts)
		require.NoError(t, err)
		require.NotNil(t, store)
		defer store.Close()
		defer func() {
			err = tc.js.DeleteKeyValue(mainCtx, bucketName) // Clean up bucket
			require.NoError(t, err)
		}()

		if store.Bucket() != bucketName {
			t.Errorf("Expected bucket name %q, got %q", bucketName, store.Bucket())
		}
		_, err = tc.js.KeyValue(mainCtx, bucketName)
		if err != nil {
			t.Errorf("KV bucket %q was not created: %v", bucketName, err)
		}

		updateReadResetBucket := fmt.Sprintf("test_urm_%d", time.Now().UnixNano())
		_, err = tc.js.CreateKeyValue(mainCtx, jetstream.KeyValueConfig{Bucket: updateReadResetBucket})
		if err != nil && !errors.Is(err, jetstream.ErrBucketExists) { // Allow binding if already exists
			t.Fatalf("Failed to pre-create KV bucket for update/read/reset tests: %v", err)
		}
		updateOpts := models.NewBaseMetricStoreOption(updateReadResetBucket, scope)

		updateStore, err := models.NewBaseMetricStore(mainCtx, tc.js, "board.x", nil, tc.tracer, tc.logger, updateOpts)
		require.NoError(t, err)
		defer updateStore.Close()
		defer func() {
			err = tc.js.DeleteKeyValue(mainCtx, updateReadResetBucket) // Clean up bucket
			require.NoError(t, err)
		}()
	})

	t.Run("BaseMetricUpdateAndReadAndReset", func(t *testing.T) {
		mainCtx, mainSpan := tc.tracer.Start(tc.ctx, serviceName)
		defer func() {
			mainSpan.End()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = uptrace.ForceFlush(ctx)
			_ = uptrace.Shutdown(ctx)
		}()

		kv := prepareKVStore(t, tc, mainCtx)
		require.NotNil(t, kv)

		scope := "test_scope_create"
		testLabel := "counter"
		metricName := "board.counter"

		opts := models.NewBaseMetricStoreOption(bucketName, scope)
		store, err := models.NewBaseMetricStore(mainCtx, tc.js, metricName, nil, tc.tracer, tc.logger, opts)
		require.NoError(t, err)
		require.NotNil(t, store)
		defer store.Close()
		defer func() {
			err = tc.js.DeleteKeyValue(mainCtx, bucketName) // Clean up bucket
			require.NoError(t, err)
		}()

		updateIn := &models.BaseMetricInput{
			Counter:       1,
			FailedCounter: 10,
		}

		err = store.Update(mainCtx, &coordination.LabelParams{Label: testLabel}, updateIn)
		require.NoError(t, err)
		obj, err := store.Read(mainCtx, &coordination.LabelParams{Label: testLabel})
		require.NoError(t, err)
		require.Equal(t, uint64(1), obj.Processed)
		require.Equal(t, int64(1), obj.InFlight)
		require.Equal(t, uint64(10), obj.Failed)

		updateIn = &models.BaseMetricInput{
			Counter:       10,
			FailedCounter: 1,
		}
		err = store.Update(mainCtx, &coordination.LabelParams{Label: testLabel}, updateIn)
		require.NoError(t, err)
		obj, err = store.Read(mainCtx, &coordination.LabelParams{Label: testLabel})
		require.NoError(t, err)
		require.Equal(t, uint64(11), obj.Processed)
		require.Equal(t, int64(11), obj.InFlight)
		require.Equal(t, uint64(11), obj.Failed)

		updateIn = &models.BaseMetricInput{
			Counter:       -1,
			FailedCounter: 0,
		}
		err = store.Update(mainCtx, &coordination.LabelParams{Label: testLabel}, updateIn)
		require.NoError(t, err)
		obj, err = store.Read(mainCtx, &coordination.LabelParams{Label: testLabel})
		require.NoError(t, err)
		require.Equal(t, uint64(11), obj.Processed)
		require.Equal(t, int64(10), obj.InFlight)
		require.Equal(t, uint64(11), obj.Failed)

		err = store.Reset(mainCtx, &coordination.LabelParams{Label: testLabel})
		require.NoError(t, err)
		obj, err = store.Read(mainCtx, &coordination.LabelParams{Label: testLabel})
		require.NoError(t, err)
		require.Equal(t, uint64(0), obj.Processed)
		require.Equal(t, int64(0), obj.InFlight)
		require.Equal(t, uint64(0), obj.Failed)

		nonExistentLabel := "non-existent-key-for-reset"
		err = store.Reset(mainCtx, &coordination.LabelParams{Label: nonExistentLabel})
		require.NoError(t, err)
		obj, err = store.Read(mainCtx, &coordination.LabelParams{Label: nonExistentLabel})
		require.NoError(t, err)
		require.Equal(t, uint64(0), obj.Processed)
	})

	t.Run("BaseMetricWatch", func(t *testing.T) {
		mainCtx, mainSpan := tc.tracer.Start(tc.ctx, serviceName)
		defer func() {
			mainSpan.End()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = uptrace.ForceFlush(ctx)
			_ = uptrace.Shutdown(ctx)
		}()

		scope := "test_scope_create"
		watchLabel := "watch-key"
		metricName := "board.counter"

		opts := models.NewBaseMetricStoreOption(bucketName, scope)
		store, err := models.NewBaseMetricStore(mainCtx, tc.js, metricName, nil, tc.tracer, tc.logger, opts)
		require.NoError(t, err)
		require.NotNil(t, store)
		defer store.Close()
		defer func() {
			err = tc.js.DeleteKeyValue(mainCtx, bucketName) // Clean up bucket
			require.NoError(t, err)
		}()

		watchCtx, cancelWatch := context.WithCancel(mainCtx)
		defer cancelWatch()
		updates := make(chan *models.BaseMetric, 5) // Buffer for updates

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			err := store.Watch(watchCtx, &coordination.LabelParams{Label: watchLabel}, func(obj *models.BaseMetric) {
				t.Logf("Watcher received update for %s: %+v", watchLabel, obj)
				// We are casting to TestCounterObject here because the callback
				// receives the specific type T, not the interface Object.
				updates <- obj
			})
			// Watch should exit cleanly on context cancellation
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("Watch goroutine exited with unexpected error: %v", err)
			}
		}()
		time.Sleep(100 * time.Millisecond)
		// Update the object - watcher should receive this

		updateIn := &models.BaseMetricInput{
			Counter:       5,
			FailedCounter: 0,
		}
		err = store.Update(mainCtx, &coordination.LabelParams{Label: watchLabel}, updateIn)
		require.NoError(t, err)
		obj, err := store.Read(mainCtx, &coordination.LabelParams{Label: watchLabel})
		require.NoError(t, err)
		require.Equal(t, uint64(5), obj.Processed)
		require.Equal(t, int64(5), obj.InFlight)
		require.Equal(t, uint64(0), obj.Failed)

		select {
		case obj := <-updates:
			require.Equal(t, uint64(5), obj.Processed)
		case <-time.After(2 * time.Second):
			t.Fatal("Watcher did not receive first update within timeout")
		}

		updateIn = &models.BaseMetricInput{
			Counter:       3,
			FailedCounter: 0,
		}
		err = store.Update(mainCtx, &coordination.LabelParams{Label: watchLabel}, updateIn)
		require.NoError(t, err)
		select {
		case obj := <-updates:
			require.Equal(t, uint64(8), obj.Processed)
		case <-time.After(2 * time.Second):
			t.Fatal("Watcher did not receive second update within timeout")
		}
		// Cancel the watch context
		cancelWatch()

		// Wait for the watcher goroutine to finish
		wg.Wait()
	})

	t.Run("UserMetricObjectStore", func(t *testing.T) {
		mainCtx, mainSpan := tc.tracer.Start(tc.ctx, serviceName)
		defer func() {
			mainSpan.End()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = uptrace.ForceFlush(ctx)
			_ = uptrace.Shutdown(ctx)
			time.Sleep(200 * time.Millisecond)
		}()

		spanCtx := trace.SpanFromContext(mainCtx)
		defer spanCtx.End()
		if spanCtx.IsRecording() {
			spanCtx.AddEvent("UserMetricObjectStore")
		}

		scope := "workflow.metrics"

		searches := []*proto.Search{
			&proto.Search{
				Id:       1000,
				ClientId: 1,
				UserId:   10,
				Boards:   randomBoards(),
			},
			&proto.Search{
				Id:       1001,
				ClientId: 1,
				UserId:   10,
				Boards:   randomBoards(),
			},
			&proto.Search{
				Id:       1002,
				ClientId: 2,
				UserId:   11,
				Boards:   randomBoards(),
			},
		}

		opts := models.NewMapMetricStoreOption(bucketName, scope)
		userMetricStore, err := models.NewMapMetricStore(mainCtx, tc.js, "user", workflowLabelFactory, tc.tracer, tc.logger, opts)
		require.NoError(t, err)
		require.NotNil(t, userMetricStore)

		defer userMetricStore.Close()
		defer func() {
			err = tc.js.DeleteKeyValue(mainCtx, bucketName) // Clean up bucket
			require.NoError(t, err)
		}()

		require.Equal(t, bucketName, userMetricStore.Bucket())

		_, err = tc.js.KeyValue(mainCtx, bucketName)
		require.NoError(t, err)

		totalSearches := uint64(0)
		totalBoards := uint64(0)
		usersId := make(map[int64]struct{})
		for _, search := range searches {
			if _, ok := usersId[search.UserId]; !ok {
				usersId[search.UserId] = struct{}{}
			}
			atomic.AddUint64(&totalSearches, 1)
			userUpdateIn := &models.MapMetricInput{
				UpdateOverall: &models.BaseMetricInput{
					Counter:       0,
					FailedCounter: 0,
				},
				UpdateBoards: make(map[uint64]*models.BaseMetricInput),
			}

			if len(search.Boards) > 0 {
				userUpdateIn.UpdateOverall.Counter += 1

				for _, board := range search.Boards {
					atomic.AddUint64(&totalBoards, 1)
					boardId, err := models.GetBoardType(board)
					if err != nil {
						tc.logger.Ctx(mainCtx).Sugar().Errorf("%v", err)
						continue
					}
					userUpdateIn.UpdateBoards[boardId] = &models.BaseMetricInput{
						Counter:       1,
						FailedCounter: 0,
					}
				}
			}

			err = userMetricStore.Update(mainCtx,
				&coordination.LabelParams{
					Params: handlers.WorkflowLabelParams{
						Scope:      userMetricStore.Scope(),
						SearchId:   int64(search.Id),
						BoardId:    0,
						ClientId:   search.ClientId,
						CompanyId:  search.ClientId,
						UserId:     search.UserId,
						LabelScope: handlers.UserType,
					},
				},
				userUpdateIn,
			)
			require.NoError(t, err)
		}

		// t.Logf("total searches/boards: %d/%d\n", totalSearches, totalBoards)
		processed := uint64(0)
		boardProcessed := uint64(0)
		for id, _ := range usersId {
			obj, err := userMetricStore.Read(mainCtx,
				&coordination.LabelParams{
					Params: handlers.WorkflowLabelParams{
						Scope:      userMetricStore.Scope(),
						UserId:     int64(id),
						BoardId:    0,
						LabelScope: handlers.UserType,
					},
				},
			)
			require.NoError(t, err)
			processed += obj.Overall.Processed
			for _, boardObj := range obj.Boards {
				boardProcessed += boardObj.Processed
			}
		}
		require.Equal(t, totalSearches, processed)
		require.Equal(t, totalBoards, boardProcessed)
	})

	t.Run("WorkflowMetricObjectStore", func(t *testing.T) {
		mainCtx, mainSpan := tc.tracer.Start(tc.ctx, serviceName)
		defer func() {
			mainSpan.End()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = uptrace.ForceFlush(ctx)
			_ = uptrace.Shutdown(ctx)
			time.Sleep(200 * time.Millisecond)
		}()

		spanCtx := trace.SpanFromContext(mainCtx)
		defer spanCtx.End()
		if spanCtx.IsRecording() {
			spanCtx.AddEvent("WorkflowMetricObjectStore")
		}

		scope := "workflow.metrics"
		searches := []*proto.Search{
			&proto.Search{
				Id:       1000,
				ClientId: 1,
				UserId:   10,
				Boards:   randomBoards(),
			},
			&proto.Search{
				Id:       1001,
				ClientId: 1,
				UserId:   10,
				Boards:   randomBoards(),
			},
			&proto.Search{
				Id:       1002,
				ClientId: 2,
				UserId:   11,
				Boards:   randomBoards(),
			},
			&proto.Search{
				Id:       1003,
				ClientId: 2,
				UserId:   11,
				Boards:   randomBoards(),
			},
		}

		expectedOverallCounts, expectedBoardSubCounts, expectedLabels, err := calculateExpectedMetrics(searches, scope)
		require.NoError(t, err, "Failed to calculate expected metrics")

		mc, err := handlers.NewMetricsCollection(mainCtx,
			"scatterGather",
			bucketName,
			scope,
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
		)
		require.NoError(t, err)
		require.NotNil(t, mc)

		defer func() {
			time.Sleep(500 * time.Millisecond)
			err = tc.js.DeleteKeyValue(mainCtx, bucketName) // Clean up bucket
			require.NoError(t, err)
		}()
		defer func() {
			time.Sleep(500 * time.Millisecond)
			mc.Close(mainCtx)
		}()

		err = mc.AddMetric(handlers.GetMetricRecordName(handlers.UserType), handlers.NewMetricHandler(handlers.MapStore, func(labelBuilder *strings.Builder, params handlers.WorkflowLabelParams) error {
			if params.UserId == 0 {
				return fmt.Errorf("invalid user id: %d", params.UserId)
			}
			labelBuilder.WriteString("user.")
			labelBuilder.WriteString(strconv.FormatInt(params.UserId, 10))
			return nil
		}))

		err = mc.AddMetric(handlers.GetMetricRecordName(handlers.ClientType), handlers.NewMetricHandler(handlers.MapStore, func(labelBuilder *strings.Builder, params handlers.WorkflowLabelParams) error {
			if params.ClientId == 0 {
				return fmt.Errorf("invalid client id: %d", params.ClientId)
			}
			labelBuilder.WriteString("client.")
			labelBuilder.WriteString(strconv.FormatInt(int64(params.ClientId), 10))
			return nil
		}))

		err = mc.AddMetric(handlers.GetMetricRecordName(handlers.SearchType), handlers.NewMetricHandler(handlers.MapStore, func(labelBuilder *strings.Builder, params handlers.WorkflowLabelParams) error {
			if params.SearchId == 0 {
				return fmt.Errorf("invalid search id: %d", params.SearchId)
			}
			labelBuilder.WriteString("search.")
			labelBuilder.WriteString(strconv.FormatInt(params.SearchId, 10))
			return nil
		}))

		err = mc.AddMetric(handlers.GetMetricRecordName(handlers.BoardType), handlers.NewMetricHandler(handlers.BaseStore, func(labelBuilder *strings.Builder, params handlers.WorkflowLabelParams) error {
			if params.BoardId == 0 {
				return fmt.Errorf("invalid board id: %d", params.BoardId)
			}
			labelBuilder.WriteString("board.")
			labelBuilder.WriteString(strconv.FormatInt(int64(params.BoardId), 10))
			return nil
		}))

		totalSearches := uint64(0)
		totalSearchesByUser := make(map[int64]*uint64)
		totalBoards := uint64(0)

		for _, search := range searches {
			var mrs []*handlers.MetricRecord
			atomic.AddUint64(&totalSearches, 1)
			if _, ok := totalSearchesByUser[search.UserId]; !ok {
				val := uint64(0)
				totalSearchesByUser[search.UserId] = &val
			}
			atomic.AddUint64(totalSearchesByUser[search.UserId], 1)
			clientMr := handlers.NewMetricRecord(handlers.GetMetricRecordName(handlers.ClientType), scope, handlers.MapStore, search)
			if err := clientMr.Set(1, 0); err != nil {
				t.Errorf("failed to set client record: %v", err)
			}

			userMr := handlers.NewMetricRecord(handlers.GetMetricRecordName(handlers.UserType), scope, handlers.MapStore, search)
			if err := userMr.Set(1, 0); err != nil {
				t.Errorf("failed to set user metric record: %v", err)
			}

			searchMr := handlers.NewMetricRecord(handlers.GetMetricRecordName(handlers.SearchType), scope, handlers.MapStore, search)
			if err := searchMr.Set(1, 0); err != nil {
				t.Errorf("failed to set search metric record: %v", err)
			}

			for _, b := range search.Boards {
				atomic.AddUint64(&totalBoards, 1)
				boardId, err := models.GetBoardType(b)
				if err != nil {
					tc.logger.Ctx(mainCtx).Sugar().Errorf("%v", err)
					continue
				}

				boardMr := handlers.NewBoardMetricRecord(handlers.GetMetricRecordName(handlers.BoardType), scope, boardId)
				if err := boardMr.Set(1, 0); err != nil {
					t.Errorf("failed to set board metric record: %v", err)
				}

				if err := clientMr.WithMapMetricInput(boardId).Set(1, 0); err != nil {
					t.Errorf("failed to set client map metric input: %v", err)
					continue
				}
				if err := userMr.WithMapMetricInput(boardId).Set(1, 0); err != nil {
					t.Errorf("failed to set user map metric input: %v", err)
					continue
				}

				if err := searchMr.WithMapMetricInput(boardId).Set(1, 0); err != nil {
					t.Errorf("failed to set search map metric input: %v", err)
					continue
				}
				// t.Logf("search %d, %d; boardMr: %+v\n", search.Id, boardId, boardMr)
				mrs = append(mrs, boardMr)
			}

			mrs = append(mrs, clientMr, userMr, searchMr)

			err = mc.Update(mainCtx, mrs)
			require.NoError(t, err)
		}
		time.Sleep(200 * time.Millisecond)

		kv, err := tc.js.KeyValue(mainCtx, bucketName)
		require.NoError(t, err)
		keys, err := kv.Keys(mainCtx)
		require.NoError(t, err)
		t.Logf("Found %d keys in bucket", len(keys))
		require.Equal(t, len(expectedLabels), len(keys), "Number of keys in KV store does not match expected number of unique labels")

		for _, key := range keys {
			expectedTypePlaceholder, ok := expectedLabels[key]
			require.True(t, ok, "Found unexpected key in KV store: %s", key)

			var actualMetric any
			var getErr error

			switch expectedTypePlaceholder.(type) {
			case *models.BaseMetric:
				actualMetric, getErr = mc.GetFromBaseStore(mainCtx, key)
				require.NoError(t, getErr, "Failed to get BaseMetric for key %s", key)
				actualBaseMetric, isBaseMetric := actualMetric.(*models.BaseMetric)
				require.True(t, isBaseMetric, "Retrieved metric for %s is not BaseMetric, got %T", key, actualMetric)

				expectedOverallCount := expectedOverallCounts[key]
				expectedProcessedCount := expectedOverallCounts[key]
				require.Equal(t, int64(expectedProcessedCount), actualBaseMetric.BaseMetric.InFlight, "InFlight count mismatch for BaseMetric %s", key)
				require.Equal(t, expectedOverallCount, actualBaseMetric.BaseMetric.Processed, "Processed count mismatch for BaseMetric %s", key)
				require.Equal(t, uint64(0), actualBaseMetric.BaseMetric.Failed, "Failed count mismatch for BaseMetric %s", key)

				tc.logger.Ctx(mainCtx).Sugar().Debugf("Verified BaseMetric %s: Processed=%d (Expected=%d)",
					key, actualBaseMetric.BaseMetric.Processed, expectedOverallCount)

			case *models.MapMetric:
				actualMetric, getErr = mc.GetFromMapStore(mainCtx, key)
				require.NoError(t, getErr, "Failed to get MapMetric for key %s", key)
				actualMapMetric, isMapMetric := actualMetric.(*models.MapMetric)
				require.True(t, isMapMetric, "Retrieved metric for %s is not MapMetric, got %T", key, actualMetric)

				// Verify Overall counts
				expectedOverallCount := expectedOverallCounts[key]
				expectedOverallProcessedCount := expectedOverallCounts[key]
				require.Equal(t, int64(expectedOverallProcessedCount), actualMapMetric.MapMetric.Overall.InFlight, "Overall InFlight count mismatch for MapMetric %s", key)
				require.Equal(t, expectedOverallCount, actualMapMetric.MapMetric.Overall.Processed, "Overall Processed count mismatch for MapMetric %s", key)
				require.Equal(t, uint64(0), actualMapMetric.MapMetric.Overall.Failed, "Overall Failed count mismatch for MapMetric %s", key)

				tc.logger.Ctx(mainCtx).Sugar().Debugf("Verified MapMetric %s Overall: Processed=%d (Expected=%d)",
					key, actualMapMetric.MapMetric.Overall.Processed, expectedOverallCount)

				// Verify Boards map and counts
				expectedBoardSubMap := expectedBoardSubCounts[key]
				require.NotNil(t, actualMapMetric.MapMetric.Boards, "Boards map is nil for MapMetric %s", key)
				require.Equal(t, len(expectedBoardSubMap), len(actualMapMetric.MapMetric.Boards), "Number of boards in MapMetric %s does not match expected", key)

				for boardID, expectedSubCount := range expectedBoardSubMap {
					actualBoardMetric, ok := actualMapMetric.MapMetric.Boards[boardID]
					require.True(t, ok, "Board ID %d not found in MapMetric %s boards", boardID, key)
					require.NotNil(t, actualBoardMetric, "Board metric for ID %d is nil in MapMetric %s", boardID, key)

					require.Equal(t, int64(expectedSubCount), actualBoardMetric.InFlight, "Board %d InFlight count mismatch in MapMetric %s", boardID, key)
					require.Equal(t, expectedSubCount, actualBoardMetric.Processed, "Board %d Processed count mismatch in MapMetric %s", boardID, key)
					require.Equal(t, uint64(0), actualBoardMetric.Failed, "Board %d Failed count mismatch in MapMetric %s", boardID, key)

					tc.logger.Ctx(mainCtx).Sugar().Debugf("Verified MapMetric %s Board %d: Processed=%d (Expected=%d)",
						key, boardID, actualBoardMetric.Processed, expectedSubCount)
				}

			default:
				t.Errorf("Unexpected expected metric type for key %s: %T", key, expectedTypePlaceholder)
			}
		}
	})
}

func calculateExpectedMetrics(searches []*proto.Search, scope string) (map[string]uint64, map[string]map[uint64]uint64, map[string]any, error) {
	expectedOverallCounts := make(map[string]uint64)             // Label -> Overall Processed Count
	expectedBoardSubCounts := make(map[string]map[uint64]uint64) // Map Label -> Board ID -> Processed Count
	expectedLabels := make(map[string]any)                       // Label -> Expected Metric Type Placeholder

	// Define label handlers matching those in the test
	scopeHandler := func(labelBuilder *strings.Builder, params handlers.WorkflowLabelParams) error {
		if params.Scope != "" {
			labelBuilder.WriteString(params.Scope)
			labelBuilder.WriteString(".")
		}
		return nil
	}

	metricHandlers := map[string]func(*strings.Builder, handlers.WorkflowLabelParams) error{
		handlers.GetMetricRecordName(handlers.UserType): func(labelBuilder *strings.Builder, params handlers.WorkflowLabelParams) error {
			if params.UserId == 0 {
				return fmt.Errorf("invalid user id: %d", params.UserId)
			}
			labelBuilder.WriteString("user.")
			labelBuilder.WriteString(strconv.FormatInt(params.UserId, 10))
			return nil
		},
		handlers.GetMetricRecordName(handlers.ClientType): func(labelBuilder *strings.Builder, params handlers.WorkflowLabelParams) error {
			if params.ClientId == 0 {
				return fmt.Errorf("invalid client id: %d", params.ClientId)
			}
			labelBuilder.WriteString("client.")
			labelBuilder.WriteString(strconv.FormatInt(int64(params.ClientId), 10))
			return nil
		},
		handlers.GetMetricRecordName(handlers.SearchType): func(labelBuilder *strings.Builder, params handlers.WorkflowLabelParams) error {
			if params.SearchId == 0 {
				return fmt.Errorf("invalid search id: %d", params.SearchId)
			}
			labelBuilder.WriteString("search.")
			labelBuilder.WriteString(strconv.FormatInt(params.SearchId, 10))
			return nil
		},
		handlers.GetMetricRecordName(handlers.BoardType): func(labelBuilder *strings.Builder, params handlers.WorkflowLabelParams) error {
			if params.BoardId == 0 {
				return fmt.Errorf("invalid board id: %d", params.BoardId)
			}
			labelBuilder.WriteString("board.")
			labelBuilder.WriteString(strconv.FormatInt(int64(params.BoardId), 10))
			return nil
		},
	}

	// Calculate expected counts
	for _, search := range searches {
		params := handlers.WorkflowLabelParams{
			Scope:     scope,
			SearchId:  int64(search.Id),
			ClientId:  search.ClientId,
			CompanyId: search.ClientId,
			UserId:    search.UserId,
		}

		mapMetricTypes := []handlers.WorkflowLabelScopeType{handlers.UserType, handlers.ClientType, handlers.SearchType}
		for _, metricType := range mapMetricTypes {
			mrName := handlers.GetMetricRecordName(metricType)
			labelBuilder := strings.Builder{}
			_ = scopeHandler(&labelBuilder, params) // Errors ignored as per test logic
			_ = metricHandlers[mrName](&labelBuilder, params)
			label := labelBuilder.String()
			if label != "" {
				expectedOverallCounts[label]++
				expectedLabels[label] = &models.MapMetric{} // Placeholder type
				if _, ok := expectedBoardSubCounts[label]; !ok {
					expectedBoardSubCounts[label] = make(map[uint64]uint64)
				}
				for _, b := range search.Boards {
					boardId, err := models.GetBoardType(b)
					if err != nil {
						return nil, nil, nil, fmt.Errorf("failed to get board type: %w", err)
					}
					expectedBoardSubCounts[label][boardId]++
				}
			}
		}

		for _, b := range search.Boards {
			boardId, err := models.GetBoardType(b)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("failed to get board type: %w", err)
			}
			params.BoardId = boardId // Update params for board handler
			mrName := handlers.GetMetricRecordName(handlers.BoardType)
			labelBuilder := strings.Builder{}
			_ = scopeHandler(&labelBuilder, params) // Errors ignored as per test logic
			_ = metricHandlers[mrName](&labelBuilder, params)
			label := labelBuilder.String()
			if label != "" {
				expectedOverallCounts[label]++
				expectedLabels[label] = &models.BaseMetric{} // Placeholder type
			}
		}
	}

	return expectedOverallCounts, expectedBoardSubCounts, expectedLabels, nil
}

func randomBoards() []string {
	allBoards := []string{"amazon", "ebay", "walmart", "shopify", "alibaba", "wish"}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	randomElement := func(slice []string) string {
		if len(slice) == 0 {
			return ""
		}
		return slice[rng.Intn(len(slice))]
	}

	totalBoards := rng.Intn(len(allBoards))
	if totalBoards == 0 {
		totalBoards += 1
	}
	var boards []string
	uqBoards := map[string]struct{}{}

	i := 0
	for {
		if i >= totalBoards {
			break
		}
		board := randomElement(allBoards)
		if _, ok := uqBoards[board]; ok {
			continue
		}
		uqBoards[board] = struct{}{}
		boards = append(boards, board)
		i++
	}

	return boards
}

type testingBT interface {
	Helper()
	Errorf(string, ...interface{})
	FailNow()
}

// func setEnvironment(b *testing.B, serviceName string) {
func setEnvironment(t testingBT, serviceName string) {
	t.Helper()

	envs := map[string]string{
		"NATS_DSN":     "nats://localhost:4222",
		"ENVIRONMENT":  "test",
		"SERVICE_NAME": serviceName,
		"KEY":          "test.key",
		"VERSION":      "0.1",
		"DSN":          DSN, // Ensure DSN is defined in your test scope
	}
	for k, v := range envs {
		require.NoError(t, os.Setenv(k, v))
	}
}

type TestContext struct {
	ctx    context.Context
	cancel context.CancelFunc
	nc     *nats.Conn
	js     jetstream.JetStream
	tracer trace.Tracer
	logger *common.Logger
}

// func getTestContext(b *testing.B) (*TestContext, error) {
func getTestContext(t testingBT) (*TestContext, error) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	logConfig, err := common.NewDevOtlpConfig()
	if err != nil {
		cancel()
		return nil, err
	}

	logger, err := common.NewLogger(logConfig)
	if err != nil {
		cancel()
		return nil, err
	}
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		cancel()
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		cancel()
		return nil, err
	}
	err = common.InitOpentelemetry(logConfig)
	if err != nil {
		cancel()
		return nil, err
	}

	tracer := otel.Tracer(tracerName)

	return &TestContext{
		ctx:    ctx,
		cancel: cancel,
		nc:     nc,
		js:     js,
		tracer: tracer,
		logger: logger,
	}, err
}

// func prepareKVStore(b *testing.B, tc *TestContext, ctx context.Context) jetstream.KeyValue {
func prepareKVStore(t testingBT, tc *TestContext, ctx context.Context) jetstream.KeyValue {
	kv, _ := tc.js.KeyValue(ctx, bucketName)
	if kv != nil {
		_ = tc.js.DeleteKeyValue(ctx, bucketName)
	}
	cfg := jetstream.KeyValueConfig{
		Bucket:      bucketName,
		Description: "Benchmarking KV store",
		TTL:         60 * time.Second,
		History:     1,
	}
	kv, err := tc.js.CreateKeyValue(ctx, cfg)
	require.NoError(t, err)
	return kv
}

func workflowLabelFactory(params any) (string, error) {
	p, ok := params.(handlers.WorkflowLabelParams)
	if !ok {
		return "", fmt.Errorf("invalid label params type: %T", params)
	}
	var labelBuf strings.Builder
	if p.Scope != "" {
		labelBuf.WriteString(p.Scope)
		labelBuf.WriteString(".")
	}

	switch p.LabelScope {
	case handlers.UserType:
		if p.UserId == 0 {
			return "", fmt.Errorf("invalid user id: %d", p.UserId)
		}
		labelBuf.WriteString("user.")
		labelBuf.WriteString(strconv.FormatInt(p.UserId, 10))
	case handlers.BoardType:
		if p.BoardId == 0 {
			return "", fmt.Errorf("invalid board id: %d", p.BoardId)
		}
		labelBuf.WriteString("board.")
		labelBuf.WriteString(strconv.FormatInt(int64(p.BoardId), 10))
	default:
		return "", fmt.Errorf("invalid label scope: %d", p.LabelScope)
	}

	return labelBuf.String(), nil
}

//func updateMetrics(ctx context.Context, name, bucketName, scope string, js jetstream.JetStream, tracer trace.Tracer, logger *common.Logger) {
//	mc, err := NewMetricsCollection(ctx, name, bucketName, scope, js, tracer, logger)
//	if err != nil {
//		logger.Ctx(ctx).Sugar().Errorf("failed to create metrics collection: %v", err)
//		return
//	}
//
//	err = mc.AddMetric("user", NewMetricHandler(MapStore, func(params workflowLabelParams) (string, error) {
//		return params.scope, nil
//	}))
//
//	mc.store.Update(ctx, &coordination.LabelParams{}, &models.BaseMetricInput{})
//	mc.mapStore.Update(ctx, &coordination.LabelParams{}, &models.MapMetricInput{})
//
//	_ = err
//}

const DSN = "https://Rw-RXLOqz1lnfkz63A7uNw@api.uptrace.dev?grpc=4317"
