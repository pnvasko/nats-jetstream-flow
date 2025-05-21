package flow

import "context"

type MessageData interface {
	MarshalVT() ([]byte, error)
	UnmarshalVT([]byte) error
}

type Message interface {
	Subject() (string, error)
	SetSubject(string)
	Uuid() string
	SetUuid(string)
	Payload() []byte
	SetPayload([]byte)
	Context() context.Context
	SetContext(context.Context)
	Metadata() map[string]string
	GetMessageMetadata(key string) string
	SetMessageMetadata(key, value string)
	Ack() bool
	Acked() <-chan struct{}
	Nack() bool
	Nacked() <-chan struct{}
	Done() <-chan struct{}
	Data() interface{}
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}
