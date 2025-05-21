package flow

import "time"

// PassThrough retransmits incoming elements downstream as they are.
//
// in  -- 1 -- 2 ---- 3 -- 4 ------ 5 --
//
// out -- 1 -- 2 ---- 3 -- 4 ------ 5 --
type PassThrough struct {
	in  chan any
	out chan any
}

// NewPassThrough returns a new PassThrough operator.
func NewPassThrough() *PassThrough {
	passThrough := &PassThrough{
		in:  make(chan any),
		out: make(chan any),
	}

	// start processing stream elements
	go passThrough.stream()

	return passThrough
}

// Via asynchronously streams data to the given Flow and returns it.
func (pt *PassThrough) Via(flow Flow) Flow {
	go pt.transmit(flow)
	return flow
}

// To streams data to the given Sink and blocks until the Sink has completed
// processing all data.
func (pt *PassThrough) To(sink Sink) {
	pt.transmit(sink)
	err := sink.AwaitCompletion(60 * time.Second)
	if err != nil {
		return
	}
}

// Out returns the output channel of the PassThrough operator.
func (pt *PassThrough) Out() <-chan any {
	return pt.out
}

// In returns the input channel of the PassThrough operator.
func (pt *PassThrough) In() chan<- any {
	return pt.in
}

func (pt *PassThrough) transmit(inlet Input) {
	for element := range pt.Out() {
		inlet.In() <- element
	}
	close(inlet.In())
}

func (pt *PassThrough) stream() {
	for element := range pt.in {
		pt.out <- element
	}
	close(pt.out)
}

var _ Flow = (*PassThrough)(nil)
