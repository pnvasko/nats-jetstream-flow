package flow

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
