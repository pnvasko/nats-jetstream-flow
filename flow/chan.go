package flow

import (
	"context"
	"fmt"
	"runtime"
	"time"
)

type ChanSource struct {
	in chan any
}

func (cs *ChanSource) Name() string {
	return "ChanSource"
}

func (cs *ChanSource) Run() error {
	return nil
}

func (cs *ChanSource) RunCtx(ctx context.Context) error {
	return nil
}

func (cs *ChanSource) Close(ctx context.Context) error {
	return nil
}

func NewChanSource(in chan any) *ChanSource {
	return &ChanSource{in}
}

func (cs *ChanSource) Via(operator Flow) Flow {
	DoStream(cs, operator)
	return operator
}

func (cs *ChanSource) Out() <-chan any {
	return cs.in
}

var _ Source = (*ChanSource)(nil)

type ChanSink struct {
	ctx context.Context
	out chan any
}

func NewChanSink(ctx context.Context) *ChanSink {
	return &ChanSink{ctx: ctx, out: make(chan any, 100)}
}

func (cs *ChanSink) In() chan<- any {
	return cs.out
}

func (cs *ChanSink) Out() <-chan any {
	return cs.out
}

func (cs *ChanSink) Run() error {
	return cs.RunCtx(cs.ctx)
}

func (cs *ChanSink) RunCtx(ctx context.Context) error {
	go cs.process(ctx)
	return nil
}

func (cs *ChanSink) AwaitCompletion(timeout time.Duration) error {
	fmt.Println("ChanSink.AwaitCompletion...")
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			return fmt.Errorf("timeout waiting for channel to empty")
		default:
			if len(cs.out) == 0 {
				return nil
			}
			runtime.Gosched()
		}
	}
}

func (cs *ChanSink) Name() string {
	return "ChanSink"
}

func (cs *ChanSink) process(ctx context.Context) {
	//defer func() {
	//	for len(cs.out) > 0 {
	//		runtime.Gosched()
	//	}
	//	close(cs.out)
	//}()
	defer func() {
		deferCtx, deferCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer deferCancel()
		for {
			select {
			case <-deferCtx.Done():
				close(cs.out)
				return
			default:
				if len(cs.out) == 0 {
					close(cs.out)
					return
				}
				// Yield CPU briefly instead of fixed sleep
				runtime.Gosched()
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-cs.ctx.Done():
			return
		}
	}
}

var _ Sink = (*ChanSink)(nil)
