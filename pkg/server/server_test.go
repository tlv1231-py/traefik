package server

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

func TestServerMemoryLeak(t *testing.T) {
	s := NewServer()
	defer s.Close()

	// Run a few warm-up iterations to stabilize Go runtime/GC
	for i := 0; i < 50; i++ {
		s.SwitchConfigs(&Configuration{
			Data: map[string]string{
				"route": fmt.Sprintf("route-%d", i),
			},
		})
	}

	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	runtime.GC()

	var msBefore runtime.MemStats
	runtime.ReadMemStats(&msBefore)
	goroutinesBefore := runtime.NumGoroutine()

	// Run 500 iterations
	for i := 0; i < 500; i++ {
		s.SwitchConfigs(&Configuration{
			Data: map[string]string{
				"route": fmt.Sprintf("route-%d", i),
			},
		})
	}

	// Trigger GC to reclaim freed memory
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	runtime.GC()

	var msAfter runtime.MemStats
	runtime.ReadMemStats(&msAfter)
	goroutinesAfter := runtime.NumGoroutine()

	// Assert goroutines did not leak
	goroutineDelta := goroutinesAfter - goroutinesBefore
	if goroutineDelta > 5 {
		t.Errorf("Goroutine leak detected: before %d, after %d (delta %d)", goroutinesBefore, goroutinesAfter, goroutineDelta)
	}

	// Assert heap objects/allocations did not grow linearly
	allocDelta := int64(msAfter.Alloc) - int64(msBefore.Alloc)
	if allocDelta > 200*1024 { // 200 KB limit
		t.Errorf("Memory leak detected: Alloc increased by %d bytes", allocDelta)
	}
}
