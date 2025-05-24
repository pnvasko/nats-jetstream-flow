package coordination

import (
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestMetrics(t *testing.T) {
	setEnvironment(t, "test.Metrics")

	tc, err := getJSHelpers(t)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.cancel()

	t.Run("TestCounterGauge_IncrAndRead", func(t *testing.T) {
		cg, err := NewCounterGaugeStore(tc.ctx, tc.js, tc.tracer, tc.logger)
		require.NoError(t, err)

		label := "requests"
		incrAmount := int64(5)

		err = cg.Reset(tc.ctx, label)
		require.NoError(t, err)
		record, err := cg.Read(tc.ctx, label)
		require.NoError(t, err)
		require.Equal(t, int64(0), record.Counter)
		require.Equal(t, int64(0), record.Gauge)

		err = cg.Incr(tc.ctx, label, incrAmount)
		require.NoError(t, err)

		record, err = cg.Read(tc.ctx, label)
		require.NoError(t, err)
		require.Equal(t, incrAmount, record.Counter)
		require.Equal(t, incrAmount, record.Gauge)
		require.Greater(t, record.Timestamp, int64(0))

		err = cg.Incr(tc.ctx, label, incrAmount)
		require.NoError(t, err)
		record, err = cg.Read(tc.ctx, label)
		require.NoError(t, err)
		require.Equal(t, incrAmount+incrAmount, record.Counter)
		require.Equal(t, incrAmount+incrAmount, record.Gauge)

		err = cg.Incr(tc.ctx, label, -1*incrAmount)
		require.NoError(t, err)
		record, err = cg.Read(tc.ctx, label)
		require.NoError(t, err)
		require.Equal(t, incrAmount+incrAmount, record.Counter)
		require.Equal(t, incrAmount, record.Gauge)
	})

	t.Run("TestCounterGauge_Concurrent", func(t *testing.T) {
		cg, err := NewCounterGaugeStore(tc.ctx, tc.js, tc.tracer, tc.logger)
		require.NoError(t, err)

		label := "concurrent"
		incrAmount := int64(1)

		err = cg.Reset(tc.ctx, label)
		require.NoError(t, err)
		record, err := cg.Read(tc.ctx, label)
		require.NoError(t, err)
		require.Equal(t, int64(0), record.Counter)
		require.Equal(t, int64(0), record.Gauge)

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = cg.Incr(tc.ctx, label, incrAmount)
			}()
		}
		wg.Wait()

		record, err = cg.Read(tc.ctx, label)
		require.NoError(t, err)
		require.Equal(t, int64(10), record.Counter)
		require.Equal(t, int64(10), record.Gauge)
	})

	t.Run("TestCounterGauge_RetryOnConflict", func(t *testing.T) {
		cg, err := NewCounterGaugeStore(tc.ctx, tc.js, tc.tracer, tc.logger)
		require.NoError(t, err)

		label := "conflict"

		err = cg.Reset(tc.ctx, label)
		require.NoError(t, err)
		record, err := cg.Read(tc.ctx, label)
		require.NoError(t, err)
		require.Equal(t, int64(0), record.Counter)
		require.Equal(t, int64(0), record.Gauge)

		err = cg.Incr(tc.ctx, label, 1)
		require.NoError(t, err)

		kv := cg.KV()
		entry, err := kv.Get(tc.ctx, label)
		require.NoError(t, err)

		go func() {
			_ = cg.Incr(tc.ctx, label, 1)
		}()
		time.Sleep(100 * time.Millisecond)
		data := entry.Value()
		_, err = kv.Update(tc.ctx, label, data, entry.Revision())
		require.Error(t, err)
		require.True(t, isJSWrongLastSequence(err))
	})
}
