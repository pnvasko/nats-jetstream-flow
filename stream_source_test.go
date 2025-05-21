package nats_jetstream_flow

import (
	"fmt"
	"github.com/pnvasko/nats-jetstream-flow/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"sync/atomic"
	"testing"
	"time"
)

func TestStreamSourceWithRealNATS(t *testing.T) {
	baseCtx, logger, nc, js, err := getJS(t)
	assert.NoError(t, err)
	require.NotNil(t, baseCtx)
	require.NotNil(t, logger)
	require.NotNil(t, nc)
	require.NotNil(t, js)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	tracer := trace.NewNoopTracerProvider().Tracer("test.stream_source.tracer")

	t.Run("BaseTest", func(t *testing.T) {
		// t.Skip("Skipping test...")
		// Configure StreamSource
		streamName := "test_stream_01"
		consumerName := "test_consumer_01"
		subjects := []string{"test.subject"}
		totalMessages := 5
		cleanupTtl := 101 * time.Second

		var opts []StreamSourceConfigOption

		var streamOpts []StreamConfigOption
		streamOpts = append(streamOpts, WithCleanupTtl(cleanupTtl))

		var consumerOpts []ConsumerConfigOption
		consumerOpts = append(consumerOpts, WithDurableName(consumerName))

		ss := setupStreamSource[*proto.Search](t, baseCtx, js, streamName, subjects, tracer, logger, streamOpts, consumerOpts, opts...)
		require.NotNil(t, ss)
		t.Logf("start %s", ss.Name())
		if ss == nil {
			t.Fatal(err)
		}
		assert.Equal(t, cleanupTtl, ss.config.CleanupTTL())

		// Create a message to test with
		genTestMessage := func(i int) {
			s := proto.Search{
				Id:       int32(i),
				ClientId: 1,
				Boards:   []string{"amazon", "ebay", "walmart"},
			}
			data, err := s.MarshalVT()
			assert.NoError(t, err)
			msg, err := NewMessage(fmt.Sprintf("new-search-%d", i), data)
			assert.NoError(t, err)
			msgData, err := NewNatsMessage(msg)
			assert.Error(t, err)
			msg.SetSubject(subjects[0])
			msg.SetContext(baseCtx)
			msgData, err = NewNatsMessage(msg)
			assert.NoError(t, err)
			t.Logf("todo.genTestMessage.uuid: %s ", msg.Uuid())
			_, err = js.PublishMsgAsync(msgData)
			assert.NoError(t, err)
			if err != nil {
				t.Fatal(err)
			}
			return
		}

		go func() {
			for i := 1; i <= totalMessages; i++ {
				genTestMessage(i)
			}

			select {
			case <-baseCtx.Done():
				return
			}
		}()

		// Start the StreamSource
		err = ss.Run()
		assert.NoError(t, err)
		var count int32
	Loop:
		for {
			select {
			case rawMsg := <-ss.Out():
				atomic.AddInt32(&count, int32(1))
				assert.NotNil(t, rawMsg)
				msg, ok := rawMsg.(*Message)
				assert.True(t, ok)
				data, ok := msg.Data().(*proto.Search)
				t.Logf("msg %d: %+v\n", count, data)
				ok = msg.Ack()
				assert.True(t, ok)
				if !ok {
					t.Logf("Failed to Ack message id: %d", data.Id)

				}
				// assert.Equal(t, "test message", string(msg.Payload()))
			case <-baseCtx.Done():
				break Loop
			case <-time.After(5 * time.Second):
				t.Logf("Message was not processed in time")
				break Loop
			}
		}
		// t.Logf("stop count %d", count)
		assert.Equal(t, int32(totalMessages), count)

	})
}
