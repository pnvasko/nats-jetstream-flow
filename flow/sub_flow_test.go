package flow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/pnvasko/nats-jetstream-flow/common"
	streamProto "github.com/pnvasko/nats-jetstream-flow/proto/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"os"
	"sync"
	"testing"
	"time"
)

func newMessageForTest(uuid string, payload []byte) *testMessage {
	msg, _ := NewMessage(uuid, payload)
	return msg
}

func setEnvironment(t *testing.T, serviceName string) {
	t.Helper()

	envs := map[string]string{
		"NATS_DSN":     "nats://localhost:4222",
		"ENVIRONMENT":  "test",
		"SERVICE_NAME": serviceName,
		"KEY":          "test.key",
		"VERSION":      "0.1",
		"DSN":          os.Getenv("DSN"), // Ensure OTLP DSN is defined in your test scope
	}
	for k, v := range envs {
		require.NoError(t, os.Setenv(k, v))
	}
}

func sinkHandler(t *testing.T, artifacts []*testMessage, complete chan struct{}) func(ctx context.Context, f *Future[*testMessage]) error {
	mu := &sync.Mutex{}
	count := 0
	total := len(artifacts)
	return func(ctx context.Context, f *Future[*testMessage]) error {
		t.Logf("sink uuid: %s", f.Original().Uuid())
		mu.Lock()
		artifacts[count] = f.Original()
		count++
		if count == total {
			close(complete)
		}
		mu.Unlock()
		return nil
	}
}

func TestSubFlowDefinition(t *testing.T) {
	setEnvironment(t, "TestSubFlowDefinition.v1")
	flowCtx, flowCancel := context.WithCancel(context.Background())
	// flowCtx, flowCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer flowCancel()

	logConfig, err := common.NewDevOtlpConfig()
	require.NoError(t, err)
	logger, _ := common.NewLibLogger(logConfig)
	tracer := trace.NewNoopTracerProvider().Tracer("test.map.tracer")
	in := make(chan any)

	isComplete := make(chan struct{})
	go func() {
		<-isComplete
		flowCancel()
	}()

	inputValues := []*testMessage{
		newMessageForTest("a", []byte("a")),
		newMessageForTest("b", []byte("b")),
		newMessageForTest("c", []byte("c")),
	}

	source := NewChanSource(in)
	totalMsgs := len(inputValues)
	artifacts := make([]*testMessage, totalMsgs)

	sink, err := NewFanOutSink[*testMessage](flowCtx, "TestSubFlowDefinition", sinkHandler(t, artifacts, isComplete), tracer, logger)
	require.NoError(t, err)

	err = source.Run()
	require.NoError(t, err)
	err = sink.Run()
	assert.NoError(t, err)

	var subFlow *SubFlowDefinition
	defer func() {
		if subFlow != nil && subFlow.Close != nil {
			err = subFlow.Close()
			require.NoError(t, err)
		}
		err = sink.AwaitCompletion(10 * time.Second)
		require.NoError(t, err)
		err = sink.Close(context.Background())
		require.NoError(t, err)
		err = source.Close(context.Background())
		require.NoError(t, err)
		t.Logf("defer.close")
	}()

	go func() {
		for _, v := range inputValues {
			select {
			case <-flowCtx.Done():
				return
			case in <- v:
			}
		}
		close(in)
		t.Logf("close end")
	}()

	normalizeStep, err := NewMap(flowCtx, func(msg *testMessage) (*testMessage, error) {
		msg.SetUuid(fmt.Sprintf("normalize.%s", msg.Uuid()))
		return msg, nil
	}, 10, tracer, logger)
	require.NoError(t, err)
	err = normalizeStep.Run()
	require.NoError(t, err)

	enrichStep, err := NewMap(flowCtx, func(msg *testMessage) (*testMessage, error) {
		msg.SetUuid(fmt.Sprintf("enrich.%s", msg.Uuid()))
		return msg, nil
	}, 10, tracer, logger)
	require.NoError(t, err)

	err = enrichStep.Run()
	require.NoError(t, err)

	transformStep, err := NewMap(flowCtx, func(msg *testMessage) (*testMessage, error) {
		msg.SetUuid(fmt.Sprintf("transform.%s", msg.Uuid()))
		return msg, nil
	}, 10, tracer, logger)
	require.NoError(t, err)

	err = transformStep.Run()
	require.NoError(t, err)

	subFlow = &SubFlowDefinition{
		Input: normalizeStep,
		Flow:  normalizeStep.Via(enrichStep).Via(transformStep),
	}

	go func() {
		source.
			Via(NewPassThrough()).
			Via(subFlow).To(sink)
		t.Logf("flow.finish")
	}()
	t.Logf("wait")
	<-flowCtx.Done()
	require.Equal(t, len(inputValues), len(artifacts))
	for _, row := range artifacts {
		t.Logf("row: %s\n", row.uuid)
	}

	t.Logf("finished")
}

type testMessage struct {
	ctx      context.Context
	uuid     string
	payload  []byte
	data     interface{}
	metadata map[string]string
	// ack is closed when acknowledge is received.
	ack chan struct{}
	// noACk is closed when negative acknowledge is received.
	noAck chan struct{}

	mu sync.Mutex

	ackMutex    sync.Mutex
	ackSentType AckType
	nackDelay   time.Duration
	errMutex    sync.Mutex
	err         error
}

type MessageOption func(*testMessage) error

func WithAckSentType(at AckType) MessageOption {
	return func(msg *testMessage) error {
		msg.ackSentType = at
		return nil
	}
}

func NewMessage(uuid string, payload []byte, opts ...MessageOption) (*testMessage, error) {
	msg := &testMessage{
		uuid:     uuid,
		metadata: make(map[string]string),
		payload:  payload,
		ack:      make(chan struct{}),
		noAck:    make(chan struct{}),
	}

	for _, opt := range opts {
		if err := opt(msg); err != nil {
			return nil, err
		}
	}

	return msg, nil
}

func (m *testMessage) Marshal() ([]byte, error) {
	header := make(nats.Header)

	header.Set("DefaultUUIDKey", m.uuid)
	subject, err := m.Subject()
	if err != nil {
		return nil, err
	}
	header.Set("DefaultSubjectKey", subject)

	propagator := propagation.TraceContext{}
	propagator.Inject(m.ctx, propagation.HeaderCarrier(header))

	for k, v := range m.metadata {
		header.Set(k, v)
	}

	// old
	anyValue := &anypb.Any{}
	bytesValue := &wrapperspb.BytesValue{
		Value: m.payload,
	}

	//data := &wrappers.BytesValue{}
	//if err := anypb.UnmarshalTo(m.payload, data, proto.UnmarshalOptions{}); err != nil {
	//	return nil, err
	//}
	if err := anypb.MarshalFrom(anyValue, bytesValue, proto.MarshalOptions{}); err != nil {
		return nil, err
	}
	sm, err := m.StreamMessage()
	if err != nil {
		return nil, err
	}
	return sm.MarshalVT()
}

func (m *testMessage) Unmarshal(rawData []byte) error {
	sm := &streamProto.Message{}
	err := sm.UnmarshalVT(rawData)
	if err != nil {
		return err
	}
	m.uuid = sm.Uuid

	m.metadata = make(map[string]string, len(sm.Metadata))
	for k, v := range sm.Metadata {
		m.metadata[k] = v
	}

	payloadBytesValue := &wrapperspb.BytesValue{}
	if err := anypb.UnmarshalTo(sm.Payload, payloadBytesValue, proto.UnmarshalOptions{}); err != nil {
		return err
	}

	m.payload = payloadBytesValue.Value
	m.ack = make(chan struct{})
	m.noAck = make(chan struct{})

	return nil
}

// Equals compare, that two messages are equal. Acks/Nacks are not compared.
func (m *testMessage) Equals(toCompare *testMessage) (bool, error) {
	if m.uuid != toCompare.uuid {
		return false, nil
	}
	if len(m.metadata) != len(toCompare.metadata) {
		return false, nil
	}
	for key, value := range m.metadata {
		if value != toCompare.metadata[key] {
			return false, nil
		}
	}
	return bytes.Equal(m.payload, toCompare.payload), nil
}

// Ack sends message's acknowledgement.
//
// Ack is not blocking.
// Ack is idempotent.
// False is returned, if Nack is already sent.
func (m *testMessage) Ack() bool {
	m.ackMutex.Lock()
	defer m.ackMutex.Unlock()

	if m.ackSentType == Nack {
		return false
	}
	if m.ackSentType != NoAckSent {
		return true
	}

	m.ackSentType = Ack
	if m.ack == nil {
		m.ack = closedchan
	} else {
		close(m.ack)
	}

	return true
}

// Nack sends message's negative acknowledgement.
//
// Nack is not blocking.
// Nack is idempotent.
// False is returned, if Ack is already sent.
func (m *testMessage) Nack() bool {
	m.ackMutex.Lock()
	defer m.ackMutex.Unlock()

	if m.ackSentType == Ack {
		return false
	}
	if m.ackSentType != NoAckSent {
		return true
	}

	m.ackSentType = Nack

	if m.noAck == nil {
		m.noAck = closedchan
	} else {
		close(m.noAck)
	}

	return true
}

// Acked returns channel which is closed when acknowledgement is sent.
//
// Usage:
//
//	select {
//	case <-message.Acked():
//		// autoAck received
//	case <-message.Nacked():
//		// Nack received
//	}

func (m *testMessage) Acked() <-chan struct{} {
	return m.ack
}

// Nacked returns channel which is closed when negative acknowledgement is sent.
//
// Usage:
//
//	select {
//	case <-message.Acked():
//		// autoAck received
//	case <-message.Nacked():
//		// Nack received
//	}
func (m *testMessage) Nacked() <-chan struct{} {
	return m.noAck
}

func (m *testMessage) Done() <-chan struct{} {
	m.ackMutex.Lock()
	defer m.ackMutex.Unlock()

	doneOnce.Do(func() {
		doneChan = make(chan struct{})
		go func() {
			select {
			case <-m.Acked():
			case <-m.Nacked():
			}
			close(doneChan)
		}()
	})

	return doneChan
}

func (m *testMessage) Uuid() string {
	return m.uuid
}

func (m *testMessage) SetUuid(uuid string) {
	m.uuid = uuid
}

func (m *testMessage) Data() interface{} {
	return m.data
}

func (m *testMessage) SetData(data interface{}) {
	m.mu.Lock()
	m.data = data
	m.mu.Unlock()
}

func (m *testMessage) Subject() (string, error) {
	s, ok := m.metadata["DefaultSubjectKey"]
	if !ok {
		return "", errors.New("subject not found")
	}
	return s, nil
}

func (m *testMessage) SetSubject(subject string) {
	m.metadata["DefaultSubjectKey"] = subject
}

func (m *testMessage) Payload() []byte {
	return m.payload
}

func (m *testMessage) SetPayload(payload []byte) {
	m.mu.Lock()
	m.payload = payload
	m.mu.Unlock()

}

func (m *testMessage) Context() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}

func (m *testMessage) SetContext(ctx context.Context) {
	m.ctx = ctx
}

func (m *testMessage) SetNackDelay(t time.Duration) {
	m.nackDelay = t
}

func (m *testMessage) GetNackDelay() time.Duration {
	return m.nackDelay
}

func (m *testMessage) Metadata() map[string]string {
	return m.metadata
}

func (m *testMessage) SetError(err error) {
	m.errMutex.Lock()
	m.err = err
	m.errMutex.Unlock()
}

func (m *testMessage) Copy() (*testMessage, error) {
	msg, err := NewMessage(m.uuid, m.payload)
	if err != nil {
		return nil, err
	}

	msg.SetContext(m.Context())

	for k, v := range m.metadata {
		if k == "traceparent" {
			continue
		}
		msg.SetMessageMetadata(k, v)
	}

	return msg, nil
}

func (m *testMessage) StreamMessage() (*streamProto.Message, error) {
	anyValue := &anypb.Any{}
	bytesValue := &wrapperspb.BytesValue{
		Value: m.payload,
	}

	if err := anypb.MarshalFrom(anyValue, bytesValue, proto.MarshalOptions{}); err != nil {
		return nil, err
	}
	msg := &streamProto.Message{
		Uuid:     m.uuid,
		Payload:  anyValue,
		Metadata: make(map[string]string, len(m.metadata)),
	}

	for k, v := range m.metadata {
		msg.Metadata[k] = v
	}

	return msg, nil
}

func (m *testMessage) GetMessageMetadata(key string) string {
	if v, ok := m.metadata[key]; ok {
		return v
	}

	return ""
}

func (m *testMessage) SetMessageMetadata(key, value string) {
	m.metadata[key] = value
}

func (m *testMessage) InjectTrace(ctx context.Context) {
	propagator := otel.GetTextMapPropagator()
	propagator.Inject(ctx, propagation.MapCarrier(m.metadata))
}

func (m *testMessage) ExtractTrace(ctx context.Context) context.Context {
	propagator := otel.GetTextMapPropagator()
	return propagator.Extract(ctx, propagation.MapCarrier(m.metadata))
}

func (m *testMessage) StartConsumerSpan(ctx context.Context, tracerName, operationName string) (context.Context, trace.Span) {
	ctx = m.ExtractTrace(ctx)
	tracer := otel.Tracer(tracerName)
	return tracer.Start(ctx, operationName)
}

var doneOnce sync.Once
var doneChan chan struct{}

var closedchan = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

type AckType int

const (
	NoAckSent AckType = iota
	Ack
	Nack
)
