package common

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type actor struct {
	name      string
	execute   func() error
	interrupt func(error)
}

type RunGroup struct {
	mu          sync.Mutex
	actors      []actor
	stopTimeout time.Duration
	started     bool
}

func NewRunGroup(stopTimeout time.Duration) *RunGroup {
	return &RunGroup{
		stopTimeout: stopTimeout,
	}
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
	g.mu.Unlock()

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

	// Wait for the first actor to stop or contex cancel.
	select {
	case err = <-executeErrors:
	case <-executeComplete:
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
