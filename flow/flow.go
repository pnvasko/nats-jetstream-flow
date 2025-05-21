package flow

import (
	"context"
	"time"
)

type Input interface {
	In() chan<- any
}

type Output interface {
	Out() <-chan any
}

type Flow interface {
	Input
	Output
	Via(Flow) Flow
	To(Sink)
}

type Source interface {
	Output
	Name() string
	Via(Flow) Flow
	Run() error
	RunCtx(context.Context) error
	Close(context.Context) error
}

type Sink interface {
	Input
	Name() string
	Run() error
	RunCtx(context.Context) error
	AwaitCompletion(time.Duration) error
}

func DoStream(out Output, in Input) {
	go func() {
		for element := range out.Out() {
			in.In() <- element
		}
	}()
}
