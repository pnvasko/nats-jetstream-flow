package flow

import (
	"context"
	"fmt"
	"sync"
	"time"
)

var mtx sync.Mutex

func IngestSlice[T any](source []T, in chan any) {
	mtx.Lock()
	defer mtx.Unlock()
	for _, e := range source {
		in <- e
	}
}

func CloseDeferred[T any](ctx context.Context, in chan T) {
	for {
		select {
		case <-ctx.Done():
			mtx.Lock()
			defer mtx.Unlock()
			close(in)
			fmt.Println("close deferred chan ok.")
			return
		}
	}
}

func ReadSlice[T any](ctx context.Context, ch <-chan any) []T {
	var result []T
	for {
		select {
		//case <-ctx.Done():
		//	return result
		case v, ok := <-ch:
			if !ok {
				fmt.Println("readSlice chan close.")
				return result
			}
			time.Sleep(500 * time.Millisecond)
			result = append(result, v.(T))
		}
	}
}
