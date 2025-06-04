package nats_jetstream_flow

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"github.com/pnvasko/nats-jetstream-flow/flow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"testing"
	"time"
)

type AnyTestPayload struct {
	data string
}

func (ap *AnyTestPayload) MarshalVT() ([]byte, error) {
	return []byte("test payload"), nil
}
func (ap *AnyTestPayload) UnmarshalVT(data []byte) error {
	fmt.Println("TestPayload: ", string(data))
	ap.data = "test payload"
	return nil
}

var _ flow.MessageData = (*AnyTestPayload)(nil)

func getJS(t *testing.T) (context.Context, common.Logger, *nats.Conn, jetstream.JetStream, error) {
	ctx := context.Background()
	logConfig, err := common.NewDevOtlpConfig()
	if err != nil {
		return nil, nil, nil, nil, err
	}
	logger, err := common.NewLibLogger(logConfig)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return ctx, logger, nc, js, nil
}

func setupStreamSource[T flow.MessageData](t *testing.T,
	ctx context.Context,
	js jetstream.JetStream,
	streamName string,
	subjects []string,
	tracer trace.Tracer,
	logger common.Logger,
	streamOpts StreamConfigOptions,
	consumerOpts ConsumerConfigOptions,
	opts ...StreamSourceConfigOption,
) *StreamSource[T] {
	t.Helper()

	// Initialize StreamSourceConfig
	config, err := NewJetStreamSourceConfig(js, streamName, subjects, streamOpts, consumerOpts, opts...)
	assert.NoError(t, err)
	ss, err := NewStreamSource[T](ctx, "test_source", js, config, tracer, logger)
	require.NoError(t, err)
	require.NotNil(t, ss)

	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := ss.Close(closeCtx)
		require.NoError(t, err)
	})

	return ss
}
