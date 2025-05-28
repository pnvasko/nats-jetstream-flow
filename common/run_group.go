package common

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"os/signal"
	"sync"
	"time"
)

type actor struct {
	name      string
	execute   func() error
	interrupt func(error)
}

type RunGroupOption func(*RunGroup) error

func WithSystemInterrupt(ok bool) RunGroupOption {
	return func(rg *RunGroup) error {
		rg.systemInterrupt = ok
		return nil
	}
}

func WithStopTimeout(td time.Duration) RunGroupOption {
	return func(rg *RunGroup) error {
		rg.stopTimeout = td
		return nil
	}
}

type RunGroup struct {
	ctx             context.Context
	cancel          context.CancelFunc
	mu              sync.Mutex
	actors          []actor
	systemInterrupt bool
	stopTimeout     time.Duration
	started         bool
}

const (
	defaultSystemInterrupt = true
	defaultStopTimeout     = 10 * time.Second
)

func NewRunGroup(opts ...RunGroupOption) (*RunGroup, error) {
	rg := &RunGroup{
		systemInterrupt: defaultSystemInterrupt,
		stopTimeout:     defaultStopTimeout,
	}

	for _, opt := range opts {
		if err := opt(rg); err != nil {
			return nil, err
		}
	}
	return rg, nil
}

func (g *RunGroup) Add(name string, execute func() error, interrupt func(error)) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.started {
		return fmt.Errorf("cannot add actor after Run has started")
	}
	g.actors = append(g.actors, actor{name, execute, interrupt})
	return nil
}

func (g *RunGroup) Run(baseCtx context.Context) error {
	g.mu.Lock()
	g.started = true
	g.ctx, g.cancel = context.WithCancel(baseCtx)
	g.mu.Unlock()

	ctx := g.ctx

	if g.systemInterrupt {
		if err := g.interrupt(); err != nil {
			return err
		}
	}
	var err error
	var closeOnceDone sync.Once

	executeErrors := make(chan error, len(g.actors))
	executeComplete := make(chan struct{}, len(g.actors))

	defer func() {
		closeOnceDone.Do(func() {
			g.started = false
		})
	}()

	if len(g.actors) == 0 {
		return nil
	}

	for _, a := range g.actors {
		go func(a actor) {
			if err := a.execute(); err != nil && !errors.Is(err, context.Canceled) {
				executeErrors <- err
			} else {
				executeComplete <- struct{}{}
			}
		}(a)
	}

	// Wait for the first actor to stop or context cancel.
	select {
	case err = <-executeErrors:
	case <-executeComplete:
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.Canceled) {
			err = ctx.Err()
		}
	case <-baseCtx.Done():
		err = baseCtx.Err()
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), g.stopTimeout)
	defer stopCancel()
	var wg sync.WaitGroup
	interruptDone := make(chan struct{})
	for _, a := range g.actors {
		wg.Add(1)
		go func(a actor) {
			defer wg.Done()
			a.interrupt(err)
		}(a)
	}

	go func() {
		wg.Wait()
		close(interruptDone)
	}()
	select {
	case <-interruptDone:
		return err
	case <-stopCtx.Done():
		return stopCtx.Err()
	}
}

func (g *RunGroup) interrupt() error {
	if !g.started || g.cancel == nil {
		return fmt.Errorf("cannot interrupt RunGroup before Run has started")
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintf(os.Stderr, "panic in signal handler: %v\n", r)
			}
		}()

		defer g.cancel()
		term := make(chan os.Signal, 32)
		signal.Notify(term, os.Interrupt, unix.SIGINT, unix.SIGQUIT, unix.SIGTERM)
		defer func() {
			signal.Stop(term)
			close(term)
		}()
		select {
		case <-term:
		case <-g.ctx.Done():
		}
		return
	}()

	return nil
}
