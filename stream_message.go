package nats_jetstream_flow

import (
	"bytes"
	"context"
	"errors"
	"github.com/nats-io/nats.go"
	"github.com/pnvasko/nats-jetstream-flow/flow"
	streamProto "github.com/pnvasko/nats-jetstream-flow/proto/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"sync"
	"time"
)

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

type Message struct {
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

type MessageOption func(*Message) error

func WithAckSentType(at AckType) MessageOption {
	return func(msg *Message) error {
		msg.ackSentType = at
		return nil
	}
}

func NewMessage(uuid string, payload []byte, opts ...MessageOption) (*Message, error) {
	msg := &Message{
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

//Subject() (string, error)
//SetSubject(string)
//Uuid() string

func (m *Message) Marshal() ([]byte, error) {
	header := make(nats.Header)

	header.Set(DefaultUUIDKey, m.uuid)
	subject, err := m.Subject()
	if err != nil {
		return nil, err
	}
	header.Set(DefaultSubjectKey, subject)

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

func (m *Message) Unmarshal(rawData []byte) error {
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
func (m *Message) Equals(toCompare *Message) (bool, error) {
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
func (m *Message) Ack() bool {
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
func (m *Message) Nack() bool {
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

func (m *Message) Acked() <-chan struct{} {
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
func (m *Message) Nacked() <-chan struct{} {
	return m.noAck
}

func (m *Message) Done() <-chan struct{} {
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

func (m *Message) Uuid() string {
	return m.uuid
}

func (m *Message) SetUuid(uuid string) {
	m.uuid = uuid
}

func (m *Message) Data() interface{} {
	return m.data
}

func (m *Message) SetData(data interface{}) {
	m.mu.Lock()
	m.data = data
	m.mu.Unlock()
}

func (m *Message) Subject() (string, error) {
	s, ok := m.metadata[DefaultSubjectKey]
	if !ok {
		return "", errors.New("subject not found")
	}
	return s, nil
}

func (m *Message) SetSubject(subject string) {
	m.metadata[DefaultSubjectKey] = subject
}

func (m *Message) Payload() []byte {
	return m.payload
}

func (m *Message) SetPayload(payload []byte) {
	m.mu.Lock()
	m.payload = payload
	m.mu.Unlock()

}

func (m *Message) Context() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}

func (m *Message) SetContext(ctx context.Context) {
	m.ctx = ctx
}

func (m *Message) SetNackDelay(t time.Duration) {
	m.nackDelay = t
}

func (m *Message) GetNackDelay() time.Duration {
	return m.nackDelay
}

func (m *Message) Metadata() map[string]string {
	return m.metadata
}

func (m *Message) SetError(err error) {
	m.errMutex.Lock()
	m.err = err
	m.errMutex.Unlock()
}

func (m *Message) Copy() (*Message, error) {
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

func (m *Message) StreamMessage() (*streamProto.Message, error) {
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

func (m *Message) GetMessageMetadata(key string) string {
	if v, ok := m.metadata[key]; ok {
		return v
	}

	return ""
}

func (m *Message) SetMessageMetadata(key, value string) {
	m.metadata[key] = value
}

func (m *Message) InjectTrace(ctx context.Context) {
	propagator := otel.GetTextMapPropagator()
	propagator.Inject(ctx, propagation.MapCarrier(m.metadata))
}

func (m *Message) ExtractTrace(ctx context.Context) context.Context {
	propagator := otel.GetTextMapPropagator()
	return propagator.Extract(ctx, propagation.MapCarrier(m.metadata))
}

func (m *Message) StartConsumerSpan(ctx context.Context, tracerName, operationName string) (context.Context, trace.Span) {
	ctx = m.ExtractTrace(ctx)
	tracer := otel.Tracer(tracerName)
	return tracer.Start(ctx, operationName)
}

var _ flow.Message = (*Message)(nil)

// Sender side propagator
// msg, _ := NewMessage("uuid-123", []byte("payload"))
// msg.InjectTrace(context.Background())  // Inject current trace

// Receiver side
//	ctx := msg.ExtractTrace(context.Background())
//	spanCtx := trace.SpanContextFromContext(ctx)
//	if spanCtx.IsValid() { // Continue trace
//		tr := otel.Tracer("your.tracer.name")
//		ctx, span := tr.Start(ctx, "processing message")
//		defer span.End()
// 		// logic here
// }

// Example usage in your message consumer:
//func handleMessage(msg *Message) {
//	// Start span, automatically continuing the trace
//	ctx, span := msg.StartConsumerSpan(context.Background(), "handle_message")
//	defer span.End()
//
//	// Your processing logic here
//	process(msg)
//
//	// Example: annotate the span
//	span.SetAttributes(attribute.String("msg.id", msg.UUID))
//}

// Auto-propagate trace on publish/send

//func handleMessage(msg *Message) {
//	ctx, span := msg.StartConsumerSpan(context.Background(), "handle_message")
//	defer span.End()
//
//	// Process message
//	process(msg)
//
//	// Publish downstream message with trace
//	newMsg, _ := NewMessage("uuid-456", []byte("next payload"))
//	newMsg.InjectTrace(ctx)
//	send(newMsg)
//}
