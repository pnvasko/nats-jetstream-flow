package flow

import (
	"context"
	"go.uber.org/atomic"
)

func zero[T any]() (_ T) { return }

type future interface {
	Wait()
	OK() bool
	Err() error
}

type Futures[T any] []*Future[T]

func (futures *Futures[T]) Await() []error {
	var errs []error

	for _, f := range *futures {
		if err := f.Err(); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func (futures *Futures[T]) Close() {
	for _, f := range *futures {
		f.CloseInner()
	}
}

type Future[T any] struct {
	ctx      context.Context
	original T
	ch       chan struct{}
	value    T
	err      error
	done     *atomic.Bool
}

func NewFuture[T any](ctx context.Context, original T) *Future[T] {
	return &Future[T]{
		ctx:      ctx,
		ch:       make(chan struct{}),
		done:     atomic.NewBool(false),
		original: original,
	}
}

func (future *Future[T]) SetValue(value T) {
	if future.done.Swap(true) {
		return
	}
	future.value = value
	close(future.ch)
}

func (future *Future[T]) SetError(err error) {
	if future.done.Swap(true) {
		return
	}
	future.err = err
	close(future.ch)
}

func (future *Future[T]) Context() context.Context {
	return future.ctx
}

func (future *Future[T]) Original() T {
	return future.original
}

func (future *Future[T]) Wait() {
	select {
	case <-future.ctx.Done():
		return
	case <-future.ch:
		return
	}
}

// Return the result and error of the async task.
func (future *Future[T]) Await() (T, error) {
	future.Wait()
	return future.value, future.err
}

// Return the result of the async task,
// nil if no result or error occurred.
func (future *Future[T]) Value() (T, error) {
	select {
	case <-future.ctx.Done():
		return zero[T](), future.ctx.Err()
	case <-future.ch:
		return future.value, future.err
	}
}

// Done indicates if the fn has finished.
func (future *Future[T]) Done() bool {
	return future.done.Load()
}

// False if error occurred,
// true otherwise.
func (future *Future[T]) OK() bool {
	select {
	case <-future.ch:
		return future.err == nil
	case <-future.ctx.Done():
		return false
	}
}

// Return the error of the async task,
// nil if no error.
func (future *Future[T]) Err() error {
	select {
	case <-future.ch:
		return future.err
	case <-future.ctx.Done():
		return future.ctx.Err()
	}
}

// Return a read-only channel,
// which will be closed if the async task completes.
// Use this if you need to wait the async task in a select statement.
func (future *Future[T]) Inner() <-chan struct{} {
	return future.ch
}

func (future *Future[T]) CloseInner() {
	if future.done.Swap(true) {
		return
	}
	close(future.ch)
}
