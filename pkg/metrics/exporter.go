package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultQueueSize is the number of samples a push exporter buffers before
	// it starts shedding load.
	DefaultQueueSize = 2048

	// DefaultBatchSize is the number of samples sent to the backend at once.
	DefaultBatchSize = 256

	// DefaultFlushInterval is how often a partially filled batch is pushed.
	DefaultFlushInterval = 10 * time.Second
)

// Sample is a single value pushed to a backend.
type Sample struct {
	Name   string
	Labels []string
	Value  float64
}

// Sink is the backend of a push based provider (Datadog, InfluxDB, StatsD).
// Send must not retain batch: the exporter reuses it for the next flush.
type Sink interface {
	Send(batch []Sample) error
}

// ExporterOptions configures a PushExporter.
type ExporterOptions struct {
	// QueueSize bounds the number of buffered samples. Once the queue is full,
	// samples are dropped instead of letting the backlog grow with the backend
	// being unable to keep up. Zero selects DefaultQueueSize.
	QueueSize int

	// BatchSize is the number of samples flushed at once. Zero selects
	// DefaultBatchSize.
	BatchSize int

	// FlushInterval is the period of the flush ticker. Zero selects
	// DefaultFlushInterval.
	FlushInterval time.Duration
}

// PushExporter forwards samples to a Sink from a single background goroutine,
// batching them and bounding the memory held by the queue.
type PushExporter struct {
	// dropped and sent are updated atomically and must stay 64-bit aligned.
	dropped int64
	sent    int64

	sink  Sink
	opts  ExporterOptions
	queue chan Sample

	closeOnce sync.Once
	done      chan struct{}
	stopped   chan struct{}
}

// NewPushExporter starts an exporter forwarding samples to sink. Close must be
// called to release its goroutine.
func NewPushExporter(sink Sink, opts ExporterOptions) *PushExporter {
	if opts.QueueSize <= 0 {
		opts.QueueSize = DefaultQueueSize
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = DefaultBatchSize
	}
	if opts.BatchSize > opts.QueueSize {
		opts.BatchSize = opts.QueueSize
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = DefaultFlushInterval
	}

	e := &PushExporter{
		sink:    sink,
		opts:    opts,
		queue:   make(chan Sample, opts.QueueSize),
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
	}

	go e.run()

	return e
}

// Push queues a sample. It never blocks: when the queue is full the sample is
// dropped and false is returned, which keeps the exporter memory bounded under
// a traffic spike or a slow backend.
func (e *PushExporter) Push(s Sample) bool {
	select {
	case e.queue <- s:
		return true
	default:
		atomic.AddInt64(&e.dropped, 1)
		return false
	}
}

// Dropped returns the number of samples dropped because the queue was full.
func (e *PushExporter) Dropped() int64 { return atomic.LoadInt64(&e.dropped) }

// Sent returns the number of samples handed over to the sink.
func (e *PushExporter) Sent() int64 { return atomic.LoadInt64(&e.sent) }

// Close stops the exporter, flushing whatever is still queued.
func (e *PushExporter) Close() {
	e.closeOnce.Do(func() {
		close(e.done)
	})
	<-e.stopped
}

func (e *PushExporter) run() {
	defer close(e.stopped)

	ticker := time.NewTicker(e.opts.FlushInterval)
	defer ticker.Stop()

	// The batch is allocated once and reused across flushes.
	batch := make([]Sample, 0, e.opts.BatchSize)

	for {
		select {
		case s := <-e.queue:
			batch = append(batch, s)
			if len(batch) >= e.opts.BatchSize {
				batch = e.flush(batch)
			}

		case <-ticker.C:
			batch = e.flush(batch)

		case <-e.done:
			// Drain what is left, in batches, then stop.
			for {
				select {
				case s := <-e.queue:
					batch = append(batch, s)
					if len(batch) >= e.opts.BatchSize {
						batch = e.flush(batch)
					}
				default:
					e.flush(batch)
					return
				}
			}
		}
	}
}

func (e *PushExporter) flush(batch []Sample) []Sample {
	if len(batch) == 0 {
		return batch
	}

	if err := e.sink.Send(batch); err == nil {
		atomic.AddInt64(&e.sent, int64(len(batch)))
	} else {
		// The backend is unavailable: drop the batch rather than retaining it,
		// the queue already provides the buffering window.
		atomic.AddInt64(&e.dropped, int64(len(batch)))
	}

	// Clear the retained strings and slices so they can be collected, then
	// reuse the backing array.
	for i := range batch {
		batch[i] = Sample{}
	}

	return batch[:0]
}
