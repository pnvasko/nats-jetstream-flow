package coordination

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

type testMetrics struct {
	counter int64
	gauge   int64
}

func newTestMetricsFactory(name string) *testMetrics {
	m := &testMetrics{
		counter: 0,
		gauge:   0,
	}
	return m
}

func (t *testMetrics) SpanName() string {
	return "test_span"
}

func (t *testMetrics) Empty() ([]byte, error) {
	tm := testMetrics{
		counter: 0,
		gauge:   0,
	}
	return tm.MarshalVT()
}

func (t *testMetrics) MarshalVT() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, t.counter)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.LittleEndian, t.gauge)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (t *testMetrics) UnmarshalVT(data []byte) error {
	buf := bytes.NewReader(data)
	err := binary.Read(buf, binary.LittleEndian, &t.counter)
	if err != nil {
		return err
	}
	err = binary.Read(buf, binary.LittleEndian, &t.gauge)
	if err != nil {
		return err
	}
	return nil
}

func (t *testMetrics) Update(a any) error {
	values, ok := a.(*testMetricsInput)
	if !ok {
		return fmt.Errorf("invalid type %T", a)
	}
	if values.counter > 0 {
		t.counter += values.counter
	}
	t.gauge += values.gauge
	return nil
}

type testMetricsInput struct {
	counter int64
	gauge   int64
}

func TestMetrics(t *testing.T) {
	setEnvironment(t, "test.ObjectStore")

	tc, err := getJSHelpers(t)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.cancel()
	t.Run("TestMetrics_Create", func(t *testing.T) {
		t.Helper()
		scope := "test_scope_create"
		var opts []StoreOption[*ObjectStore[*testMetrics, *testMetricsInput]]
		opts = append(opts, WithScope[*ObjectStore[*testMetrics, *testMetricsInput]](scope))
		m, err := NewObjectStore[*testMetrics, *testMetricsInput](tc.ctx, scope, tc.js, newTestMetricsFactory, tc.tracer, tc.logger, opts...)
		require.NoError(t, err)
		require.NotNil(t, m)
		require.Equal(t, scope, m.Scope())
	})

	t.Run("TestMetrics_IncrAndRead", func(t *testing.T) {
		m, err := NewObjectStore[*testMetrics, *testMetricsInput](tc.ctx, "test", tc.js, newTestMetricsFactory, tc.tracer, tc.logger)
		require.NoError(t, err)

		label := "requests"
		incrReq := &testMetricsInput{
			counter: 5,
			gauge:   10,
		}
		err = m.Reset(tc.ctx, label)
		require.NoError(t, err)
		rawRecord, err := m.Read(tc.ctx, label)
		require.NoError(t, err)
		record, ok := any(rawRecord).(*testMetrics)
		require.True(t, ok)
		require.Equal(t, int64(0), record.gauge)
		require.Equal(t, int64(0), record.counter)

		err = m.Update(tc.ctx, label, incrReq)
		require.NoError(t, err)

		rawRecord, err = m.Read(tc.ctx, label)
		require.NoError(t, err)
		record, ok = any(rawRecord).(*testMetrics)
		require.True(t, ok)
		require.Equal(t, incrReq.counter, record.counter)
		require.Equal(t, incrReq.gauge, record.gauge)
	})
}
