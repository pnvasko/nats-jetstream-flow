package flow

import "time"

type SubFlowDefinition struct {
	Input Input
	Flow  Flow
	Close func() error
}

func (sf *SubFlowDefinition) Out() <-chan any {
	return sf.Flow.Out()
}

func (sf *SubFlowDefinition) In() chan<- any {
	return sf.Input.In()
}

func (sf *SubFlowDefinition) To(sink Sink) {
	sf.transmit(sink)
	err := sink.AwaitCompletion(60 * time.Second)
	if err != nil {
		return
	}
}

func (sf *SubFlowDefinition) Via(next Flow) Flow {
	go sf.transmit(next)
	return next
}

func (sf *SubFlowDefinition) transmit(inlet Input) {
	for element := range sf.Out() {
		inlet.In() <- element
	}
	close(inlet.In())
}
