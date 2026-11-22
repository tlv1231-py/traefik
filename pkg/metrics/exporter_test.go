package metrics

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// recordingSink copies every batch it receives: the exporter reuses the slice
// it hands over.
type recordingSink struct {
	mu      sync.Mutex
	batches [][]Sample
	block   chan struct{}
	err     error
}

func (s *recordingSink) Send(batch []Sample) error {
	if s.block != nil {
		<-s.block
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.err != nil {
		return s.err
	}

	cp := make([]Sample, len(batch))
	copy(cp, batch)
	s.batches = append(s.batches, cp)

	return nil
}

func (s *recordingSink) samples() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	var n int
	for _, b := range s.batches {
		n += len(b)
	}

	return n
}

func (s *recordingSink) maxBatchSize() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	var max int
	for _, b := range s.batches {
		if len(b) > max {
			max = len(b)
		}
	}

	return max
}

func TestPushExporterBatchesAndFlushesOnClose(t *testing.T) {
	sink := &recordingSink{}
	exporter := NewPushExporter(sink, ExporterOptions{
		QueueSize:     64,
		BatchSize:     8,
		FlushInterval: time.Hour, // only Close and the batch size trigger a flush
	})

	for i := 0; i < 20; i++ {
		if !exporter.Push(Sample{Name: "requests_total", Value: 1}) {
			t.Fatalf("Push() dropped sample %d, the queue should have room", i)
		}
	}

	exporter.Close()

	if got := sink.samples(); got != 20 {
		t.Errorf("sink received %d samples, want 20", got)
	}
	if got := sink.maxBatchSize(); got > 8 {
		t.Errorf("batch of %d samples, want <= 8", got)
	}
	if got := exporter.Sent(); got != 20 {
		t.Errorf("Sent() = %d, want 20", got)
	}
}

// TestPushExporterDropsWhenQueueFull covers the backpressure requirement: a
// backend that cannot keep up must not make the queue grow without limit.
func TestPushExporterDropsWhenQueueFull(t *testing.T) {
	sink := &recordingSink{block: make(chan struct{})}
	exporter := NewPushExporter(sink, ExporterOptions{
		QueueSize:     16,
		BatchSize:     4,
		FlushInterval: time.Hour,
	})

	var accepted int
	for i := 0; i < 10000; i++ {
		if exporter.Push(Sample{Name: "requests_total", Value: 1}) {
			accepted++
		}
	}

	if accepted > 16+4 {
		t.Errorf("exporter accepted %d samples, want at most queue+batch (%d)", accepted, 16+4)
	}
	if exporter.Dropped() == 0 {
		t.Error("Dropped() = 0, want the samples over the queue size to be dropped")
	}

	close(sink.block)
	exporter.Close()
}

func TestPushExporterFlushesOnTicker(t *testing.T) {
	sink := &recordingSink{}
	exporter := NewPushExporter(sink, ExporterOptions{
		QueueSize:     64,
		BatchSize:     1000, // never reached, only the ticker can flush
		FlushInterval: 10 * time.Millisecond,
	})
	defer exporter.Close()

	for i := 0; i < 5; i++ {
		exporter.Push(Sample{Name: "requests_total", Value: 1})
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if sink.samples() == 5 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Errorf("sink received %d samples after the flush interval, want 5", sink.samples())
}

func TestPushExporterCountsFailedBatchesAsDropped(t *testing.T) {
	sink := &recordingSink{err: errors.New("backend unavailable")}
	exporter := NewPushExporter(sink, ExporterOptions{
		QueueSize:     64,
		BatchSize:     4,
		FlushInterval: time.Hour,
	})

	for i := 0; i < 4; i++ {
		exporter.Push(Sample{Name: "requests_total", Value: 1})
	}

	exporter.Close()

	if got := exporter.Sent(); got != 0 {
		t.Errorf("Sent() = %d, want 0 when the backend fails", got)
	}
	if got := exporter.Dropped(); got != 4 {
		t.Errorf("Dropped() = %d, want 4", got)
	}
}

func TestPushExporterCloseIsIdempotent(t *testing.T) {
	exporter := NewPushExporter(&recordingSink{}, ExporterOptions{})

	exporter.Close()
	exporter.Close()
}

func BenchmarkMetricsPush(b *testing.B) {
	exporter := NewPushExporter(nopSink{}, ExporterOptions{QueueSize: 4096})
	defer exporter.Close()

	sample := Sample{Name: "requests_total", Value: 1}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		exporter.Push(sample)
	}
}

type nopSink struct{}

func (nopSink) Send(batch []Sample) error { return nil }
