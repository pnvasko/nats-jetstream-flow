package flow

import (
	"context"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"strings"
	"testing"
	"time"
)

func TestMapMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	logConfig, err := common.NewDevOtlpConfig()
	require.NoError(t, err)

	logger, _ := common.NewLogger(logConfig)
	tracer := trace.NewNoopTracerProvider().Tracer("test.map.tracer")
	in := make(chan any)

	source := NewChanSource(in)
	err = source.Run()
	require.NoError(t, err)

	mapTransformer := func(message string) (string, error) {
		return strings.ToUpper(message), nil
	}

	toUpperMapFlow, _ := NewMap(ctx, mapTransformer, 1, tracer, logger)
	err = toUpperMapFlow.Run()
	assert.NoError(t, err)

	sink := NewChanSink(ctx)
	err = sink.Run()
	assert.NoError(t, err)

	inputValues := []string{"a", "b", "c"}
	go IngestSlice(inputValues, in)
	go CloseDeferred(ctx, in)

	go func() {
		source.
			Via(toUpperMapFlow).
			To(sink)
	}()

	outputValues := ReadSlice[string](ctx, sink.Out())
	assert.Equal(t, len(inputValues), len(outputValues))

	for _, iv := range inputValues {
		isPresent := false
		for _, value := range outputValues {
			if value == strings.ToUpper(iv) {
				isPresent = true
				break
			}
		}
		assert.True(t, isPresent)
	}
}
