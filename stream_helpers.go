package nats_jetstream_flow

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pnvasko/nats-jetstream-flow/flow"
	"github.com/rs/xid"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"reflect"
	"strings"
)

func newT[T any]() T {
	return *new(T)
}

func NewNatsMessage(msg flow.Message) (*nats.Msg, error) {
	headers := make(nats.Header)

	headers.Set(DefaultUUIDKey, msg.Uuid())
	subject, err := msg.Subject()
	if err != nil {
		return nil, err
	}
	headers.Set(DefaultSubjectKey, subject)
	propagator := propagation.TraceContext{}
	propagator.Inject(msg.Context(), propagation.HeaderCarrier(headers))

	for k, v := range msg.Metadata() {
		headers.Set(k, v)
	}
	return &nats.Msg{
		Subject: subject,
		Data:    msg.Payload(),
		Header:  headers,
	}, nil
}

func LoadNatsMessage[T flow.MessageData](jsMsg jetstream.Msg) (flow.Message, error) {
	headers := jsMsg.Headers()
	uuid := headers.Get(DefaultUUIDKey)
	if uuid == "" {
		uuid = xid.New().String()
	}

	msg, err := NewMessage(uuid, jsMsg.Data())
	if err != nil {
		return nil, err
	}

	var tv T
	tvElm := reflect.ValueOf(&tv).Elem()
	if tvElm.IsNil() {
		// If it's a pointer type, initialize it
		tvElm.Set(reflect.New(tvElm.Type().Elem()))
	}
	// tv := newT[T]()
	// tv := new(T)
	err = tv.UnmarshalVT(msg.Payload())
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal payload to messge type: %w", err)
	}
	msg.data = tv
	subject := headers.Get(DefaultSubjectKey)
	msg.SetSubject(subject)

	// propagator := propagation.TraceContext{}
	// msgCtx := propagator.Extract(context.Background(), propagation.HeaderCarrier(headers))
	//  todo. look why propagator.Extract don't work
	msgCtx := ExtractContext(headers)
	msg.SetContext(msgCtx)

	for k, v := range headers {
		switch k {
		case UuidHeaderKey, nats.MsgIdHdr, nats.ExpectedLastMsgIdHdr, nats.ExpectedStreamHdr, nats.ExpectedLastSubjSeqHdr, nats.ExpectedLastSeqHdr:
			continue
		default:
			if len(v) == 1 {
				msg.SetMessageMetadata(k, v[0])
			} else {
				return nil, fmt.Errorf("multiple values received in NATS header for %q: (%+v)", k, v)
			}
		}
	}

	metadata, err := jsMsg.Metadata()
	if err != nil {
		return nil, err
	}

	msg.SetMessageMetadata(DefaultTimestampKey, Int64ToString(metadata.Timestamp.UnixNano()))
	msg.SetMessageMetadata(DefaultStreamKey, metadata.Stream)
	msg.SetMessageMetadata(DefaultSequenceConsumerKey, Int64ToString(int64(metadata.Sequence.Consumer)))
	msg.SetMessageMetadata(DefaultSequenceStreamKey, Int64ToString(int64(metadata.Sequence.Stream)))
	msg.SetMessageMetadata(DefaultNumDeliveredKey, Int64ToString(int64(metadata.NumDelivered)))
	msg.SetMessageMetadata(DefaultNumPendingKey, Int64ToString(int64(metadata.NumPending)))

	return msg, nil
}

func ExtractContext(headers nats.Header) context.Context {
	h := headers.Get("traceparent")
	baseCtx := context.Background()
	_ = baseCtx
	if h == "" {
		return baseCtx
	}
	var ver [1]byte
	if !extractPart(ver[:], &h, 2) {
		return baseCtx
	}
	version := int(ver[0])
	if version > 254 {
		return baseCtx
	}

	var scc trace.SpanContextConfig
	if !extractPart(scc.TraceID[:], &h, 32) {
		return baseCtx
	}
	if !extractPart(scc.SpanID[:], &h, 16) {
		return baseCtx
	}

	var opts [1]byte
	if !extractPart(opts[:], &h, 2) {
		return baseCtx
	}
	if version == 0 && (h != "" || opts[0] > 2) {
		// version 0 not allow extra
		// version 0 not allow other flag
		return baseCtx
	}
	scc.TraceFlags = trace.TraceFlags(opts[0]) & trace.FlagsSampled
	scc.TraceState, _ = trace.ParseTraceState(headers.Get("tracestate"))
	scc.Remote = true

	sc := trace.NewSpanContext(scc)
	if !sc.IsValid() {
		return baseCtx
	}
	return trace.ContextWithRemoteSpanContext(baseCtx, sc)
}

func upperHex(v string) bool {
	for _, c := range v {
		if c >= 'A' && c <= 'F' {
			return true
		}
	}
	return false
}

func extractPart(dst []byte, h *string, n int) bool {
	part, left, _ := strings.Cut(*h, "-")
	*h = left
	// hex.Decode decodes unsupported upper-case characters, so exclude explicitly.
	if len(part) != n || upperHex(part) {
		return false
	}
	if p, err := hex.Decode(dst, []byte(part)); err != nil || p != n/2 {
		return false
	}
	return true
}
