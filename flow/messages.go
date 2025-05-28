package flow

import "context"

type Messages struct {
	Messages        []Message
	OriginalMessage Message
}

func NewMessages(msg Message) *Messages {
	ms := &Messages{
		Messages:        []Message{},
		OriginalMessage: msg,
	}

	return ms
}

func (ms *Messages) Copy() *Messages {
	messagesCopy := make([]Message, len(ms.Messages))
	copy(messagesCopy, ms.Messages)
	copyCtx := context.WithoutCancel(ms.OriginalMessage.Context())

	copyMsg := &Messages{
		Messages:        messagesCopy,
		OriginalMessage: ms.OriginalMessage,
	}
	copyMsg.OriginalMessage.SetContext(copyCtx)
	copyMsg.OriginalMessage.SetUuid(ms.OriginalMessage.Uuid())

	return copyMsg
}
