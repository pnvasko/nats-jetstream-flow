package common

import (
	"sync"
	"sync/atomic"
)

type SafeWaitGroup struct {
	sync.WaitGroup
	count int32
}

func (wg *SafeWaitGroup) Add(delta int) {
	atomic.AddInt32(&wg.count, int32(delta))
	wg.WaitGroup.Add(delta)
}

func (wg *SafeWaitGroup) Done() {
	//if atomic.AddInt32(&wg.count, -1) < 0 {
	//	panic("negative WaitGroup counter")
	//}
	if atomic.AddInt32(&wg.count, -1) >= 0 {
		wg.WaitGroup.Done()
	}
}
