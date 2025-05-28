package nats_jetstream_flow

import (
	"context"
	"fmt"
	"github.com/pnvasko/nats-jetstream-flow/flow"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sys/unix"
	"os"
	"os/signal"
	"testing"
	"time"
)

func TestStreamSinkProducerWithRealNATS(t *testing.T) {
	baseCtx, logger, nc, js, err := getJS(t)
	require.NoError(t, err)
	require.NotNil(t, baseCtx)
	require.NotNil(t, logger)
	require.NotNil(t, nc)
	require.NotNil(t, js)
	defer nc.Close()
	baseCtx, baseCancel := context.WithCancel(baseCtx)
	defer baseCancel()

	tracer := trace.NewNoopTracerProvider().Tracer("test.stream_sink_producer.tracer")
	require.NotNil(t, tracer)
	testStreamName := "test_stream_01"
	testStreamSubjects := []string{"test.subject.>"}
	testCleanupTtl := 20 * time.Second

	var streamOpts []StreamConfigOption
	streamOpts = append(streamOpts, WithCleanupTtl(testCleanupTtl))

	streamConfig, err := NewStreamConfig(testStreamName, testStreamSubjects, streamOpts...)
	require.NoError(t, err)

	sinkConfig, err := NewStreamSinkConfig(streamConfig)
	require.NoError(t, err)
	require.NotNil(t, sinkConfig)

	in := make(chan any)
	source := flow.NewChanSource(in)

	jsProducerFunction := func(ctx context.Context, future *flow.Future[*Message]) error {
		fmt.Printf("jsProducerFunction:  %+v\n", future)
		return nil
		// return BaseJsProducerFunction(ctx, js, future, tracer, logger)
	}

	jsCompletionFunction := func(ctx context.Context, future *flow.Future[*Message]) error {
		fmt.Printf("jsProducerFunction:  %+v\n", future)
		return nil
		// return BaseJsProducerFunction(ctx, js, future, tracer, logger)
	}

	sink, err := NewStreamSinkProducer[*Message](baseCtx,
		"test_sink",
		js,
		jsProducerFunction,
		jsCompletionFunction,
		sinkConfig,
		tracer,
		logger,
	)
	// sink, err := NewStreamSinkProducer[*Message](baseCtx, js, BaseJsProducerFunction, sinkConfig, tracer, logger)
	require.NoError(t, err)
	require.NotNil(t, sink)
	t.Logf("start sink %s", sink.Name())

	err = source.RunCtx(baseCtx)
	require.NoError(t, err)
	err = sink.RunCtx(baseCtx)
	require.NoError(t, err)

	inputValues := []string{"a", "b", "c"}
	originalMsg, err := NewMessage("original-msg-01", []byte("original-msg-data"))
	require.NoError(t, err)
	go func() {
		in <- originalMsg
	}()

	msgs := flow.NewMessages(originalMsg)

	for _, v := range inputValues {
		msg, err := NewMessage(v, []byte(v))
		require.NoError(t, err)
		msgs.Messages = append(msgs.Messages, msg)
	}
	inputMessages := []*flow.Messages{msgs}
	go flow.IngestSlice(inputMessages, in)
	go flow.CloseDeferred(baseCtx, in)

	go func() {
		source.
			Via(flow.NewPassThrough()).
			To(sink)
	}()

	term := make(chan os.Signal, 32)
	signal.Notify(term, os.Interrupt, unix.SIGINT, unix.SIGQUIT, unix.SIGTERM)
	select {
	case <-term:
		baseCancel()
		return
	}
}
