package coordination

import (
	"context"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pnvasko/nats-jetstream-flow/common"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"os"
	"testing"
)

type TestContext struct {
	ctx    context.Context
	cancel context.CancelFunc
	nc     *nats.Conn
	js     jetstream.JetStream
	tracer trace.Tracer
	logger *common.Logger
}

func setEnvironment(t *testing.T, serviceName string) {
	t.Helper()

	err := os.Setenv("DSN", "nats://localhost:4222")
	require.NoError(t, err)
	err = os.Setenv("ENVIRONMENT", "test")
	require.NoError(t, err)
	err = os.Setenv("SERVICE_NAME", serviceName)
	require.NoError(t, err)
	err = os.Setenv("KEY", "test.key")
	require.NoError(t, err)
	err = os.Setenv("VERSION", "0.1")
	require.NoError(t, err)
}

func getJSHelpers(t *testing.T) (*TestContext, error) {
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
	tracer := trace.NewNoopTracerProvider().Tracer("test.tracer")
	return &TestContext{
		ctx:    ctx,
		cancel: cancel,
		nc:     nc,
		js:     js,
		tracer: tracer,
		logger: logger,
	}, err
}
