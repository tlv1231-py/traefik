// Package metrics implements the metrics registry shared by the metrics
// middleware and the metrics providers (Prometheus, Datadog, InfluxDB, StatsD).
//
// The registry is built for the request hot path: recording an observation on
// an already known label set performs no heap allocation, and the number of
// distinct label sets (the cardinality) is capped so that traffic carrying
// unbounded label values cannot grow the heap without limit.
package metrics

import (
	"io"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
)

const (
	// DefaultMaxCardinality is the number of distinct label sets a registry
	// tracks before folding further ones into an overflow series.
	DefaultMaxCardinality = 10000

	// OverflowLabelValue is substituted for every label value of the series
	// aggregating the observations recorded once MaxCardinality is reached.
	OverflowLabelValue = "__overflow__"
)

// DefaultBuckets matches the default histogram buckets of the Prometheus
// provider.
var DefaultBuckets = []float64{0.1, 0.3, 1.2, 5.0}

// Options configures a Registry.
type Options struct {
	// MaxCardinality caps the number of distinct label sets kept in memory.
	// Zero selects DefaultMaxCardinality, a negative value disables the cap
	// (the previous, unbounded behaviour).
	MaxCardinality int

	// Buckets holds the histogram upper bounds, in seconds.
	Buckets []float64

	AddEntryPointsLabels bool
	AddRoutersLabels     bool
	AddServicesLabels    bool
}

// Series is a single time series: one metric name with one label set.
type Series struct {
	// count and sumBits are updated atomically and must stay 64-bit aligned,
	// hence their position at the top of the struct.
	count   int64
	sumBits uint64

	name      string
	labels    []string
	bounds    []float64
	buckets   []uint64
	histogram bool
}

// Name returns the metric name of the series.
func (s *Series) Name() string { return s.name }

// Labels returns the flattened key/value label pairs of the series. The
// returned slice must not be modified.
func (s *Series) Labels() []string { return s.labels }

// Count returns the number of observations recorded on the series.
func (s *Series) Count() int64 { return atomic.LoadInt64(&s.count) }

// Sum returns the sum of the observed values.
func (s *Series) Sum() float64 {
	return math.Float64frombits(atomic.LoadUint64(&s.sumBits))
}

func (s *Series) observe(value float64) {
	atomic.AddInt64(&s.count, 1)
	addFloat64(&s.sumBits, value)

	if !s.histogram {
		return
	}

	for i, bound := range s.bounds {
		if value <= bound {
			atomic.AddUint64(&s.buckets[i], 1)
			return
		}
	}
	atomic.AddUint64(&s.buckets[len(s.bounds)], 1)
}

func addFloat64(bits *uint64, delta float64) {
	for {
		old := atomic.LoadUint64(bits)
		updated := math.Float64bits(math.Float64frombits(old) + delta)
		if atomic.CompareAndSwapUint64(bits, old, updated) {
			return
		}
	}
}

// Registry stores the series collected while serving requests.
type Registry struct {
	// dropped is updated atomically and must stay 64-bit aligned.
	dropped int64

	opts Options

	mu       sync.RWMutex
	series   map[string]*Series
	overflow map[string]*Series

	// full is set to 1 once the cardinality cap has been reached, so the hot
	// path can route to the overflow series without taking the write lock.
	full int32
}

// NewRegistry returns a Registry configured with opts.
func NewRegistry(opts Options) *Registry {
	if opts.MaxCardinality == 0 {
		opts.MaxCardinality = DefaultMaxCardinality
	}
	if len(opts.Buckets) == 0 {
		opts.Buckets = DefaultBuckets
	}

	return &Registry{
		opts:     opts,
		series:   make(map[string]*Series),
		overflow: make(map[string]*Series),
	}
}

// Options returns the options the registry was built with.
func (r *Registry) Options() Options { return r.opts }

// keyBufPool holds the scratch buffers used to build series keys. Reusing them
// keeps the lookup path allocation free: a map lookup on string(buf) does not
// copy the bytes.
var keyBufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 256)
		return &buf
	},
}

func appendKey(buf []byte, name string, labels []string) []byte {
	buf = append(buf, name...)
	for _, label := range labels {
		buf = append(buf, 0)
		buf = append(buf, label...)
	}

	return buf
}

// CounterAdd adds delta to the counter identified by name and labels.
//
// labels holds flattened key/value pairs and must be supplied in a stable
// order; callers build them from a fixed label schema. The slice is only read,
// so callers can reuse (and pool) it.
func (r *Registry) CounterAdd(name string, labels []string, delta float64) {
	r.lookup(name, labels, false).observe(delta)
}

// ObserveDuration records a duration, in seconds, on the histogram identified
// by name and labels.
func (r *Registry) ObserveDuration(name string, labels []string, seconds float64) {
	r.lookup(name, labels, true).observe(seconds)
}

func (r *Registry) lookup(name string, labels []string, histogram bool) *Series {
	bufp := keyBufPool.Get().(*[]byte)
	buf := appendKey((*bufp)[:0], name, labels)

	r.mu.RLock()
	s := r.series[string(buf)]
	r.mu.RUnlock()

	if s == nil {
		if atomic.LoadInt32(&r.full) == 1 {
			s = r.overflowSeries(name, labels, histogram)
		} else {
			s = r.createSeries(string(buf), name, labels, histogram)
		}
	}

	*bufp = buf
	keyBufPool.Put(bufp)

	return s
}

func (r *Registry) createSeries(key, name string, labels []string, histogram bool) *Series {
	r.mu.Lock()
	defer r.mu.Unlock()

	if s, ok := r.series[key]; ok {
		return s
	}

	if r.opts.MaxCardinality > 0 && len(r.series) >= r.opts.MaxCardinality {
		atomic.StoreInt32(&r.full, 1)
		return r.lockedOverflowSeries(name, labels, histogram)
	}

	s := r.newSeries(name, cloneLabels(labels), histogram)
	r.series[key] = s

	return s
}

func (r *Registry) overflowSeries(name string, labels []string, histogram bool) *Series {
	r.mu.RLock()
	s := r.overflow[name]
	r.mu.RUnlock()

	if s != nil {
		atomic.AddInt64(&r.dropped, 1)
		return s
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	return r.lockedOverflowSeries(name, labels, histogram)
}

// lockedOverflowSeries returns the aggregate series for name. It must be called
// with the write lock held.
func (r *Registry) lockedOverflowSeries(name string, labels []string, histogram bool) *Series {
	atomic.AddInt64(&r.dropped, 1)

	if s, ok := r.overflow[name]; ok {
		return s
	}

	// Keep the label names, so the series stays readable by the providers, and
	// mask the values that would have blown up the cardinality.
	masked := make([]string, len(labels))
	for i := range labels {
		if i%2 == 0 {
			masked[i] = labels[i]
		} else {
			masked[i] = OverflowLabelValue
		}
	}

	s := r.newSeries(name, masked, histogram)
	r.overflow[name] = s

	return s
}

func (r *Registry) newSeries(name string, labels []string, histogram bool) *Series {
	s := &Series{name: name, labels: labels, histogram: histogram}
	if histogram {
		s.bounds = r.opts.Buckets
		s.buckets = make([]uint64, len(s.bounds)+1)
	}

	return s
}

func cloneLabels(labels []string) []string {
	out := make([]string, len(labels))
	copy(out, labels)

	return out
}

// Cardinality returns the number of distinct label sets currently tracked,
// overflow series excluded.
func (r *Registry) Cardinality() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.series)
}

// DroppedSeries returns the number of observations folded into an overflow
// series because the cardinality cap had been reached.
func (r *Registry) DroppedSeries() int64 { return atomic.LoadInt64(&r.dropped) }

// Observations returns the total number of recorded observations, the ones
// folded into overflow series included.
func (r *Registry) Observations() int64 {
	var total int64

	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, s := range r.series {
		total += s.Count()
	}
	for _, s := range r.overflow {
		total += s.Count()
	}

	return total
}

// Snapshot appends every series of the registry to dst and returns it. Passing
// back the slice returned by a previous call reuses its capacity.
func (r *Registry) Snapshot(dst []*Series) []*Series {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, s := range r.series {
		dst = append(dst, s)
	}
	for _, s := range r.overflow {
		dst = append(dst, s)
	}

	return dst
}

// textBufPool holds the buffers used to serialize the registry. Scrapes happen
// while requests are being served, so the payload is built in a pooled buffer
// instead of a freshly allocated one.
var textBufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 8192)
		return &buf
	},
}

// WriteText writes the registry to w in the Prometheus text exposition format.
func (r *Registry) WriteText(w io.Writer) (int, error) {
	bufp := textBufPool.Get().(*[]byte)
	buf := (*bufp)[:0]

	r.mu.RLock()
	for _, s := range r.series {
		buf = appendSeriesText(buf, s)
	}
	for _, s := range r.overflow {
		buf = appendSeriesText(buf, s)
	}
	r.mu.RUnlock()

	n, err := w.Write(buf)

	*bufp = buf
	textBufPool.Put(bufp)

	return n, err
}

func appendSeriesText(buf []byte, s *Series) []byte {
	if !s.histogram {
		buf = appendNameAndLabels(buf, s.name, s.labels, "", "")
		buf = strconv.AppendFloat(buf, s.Sum(), 'g', -1, 64)

		return append(buf, '\n')
	}

	var cumulative uint64
	for i, bound := range s.bounds {
		cumulative += atomic.LoadUint64(&s.buckets[i])
		buf = appendNameAndLabels(buf, s.name+"_bucket", s.labels, "le", strconv.FormatFloat(bound, 'g', -1, 64))
		buf = strconv.AppendUint(buf, cumulative, 10)
		buf = append(buf, '\n')
	}

	cumulative += atomic.LoadUint64(&s.buckets[len(s.bounds)])
	buf = appendNameAndLabels(buf, s.name+"_bucket", s.labels, "le", "+Inf")
	buf = strconv.AppendUint(buf, cumulative, 10)
	buf = append(buf, '\n')

	buf = appendNameAndLabels(buf, s.name+"_sum", s.labels, "", "")
	buf = strconv.AppendFloat(buf, s.Sum(), 'g', -1, 64)
	buf = append(buf, '\n')

	buf = appendNameAndLabels(buf, s.name+"_count", s.labels, "", "")
	buf = strconv.AppendInt(buf, s.Count(), 10)

	return append(buf, '\n')
}

func appendNameAndLabels(buf []byte, name string, labels []string, extraKey, extraValue string) []byte {
	buf = append(buf, name...)
	buf = append(buf, '{')

	for i := 0; i+1 < len(labels); i += 2 {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, labels[i]...)
		buf = append(buf, '=', '"')
		buf = append(buf, labels[i+1]...)
		buf = append(buf, '"')
	}

	if extraKey != "" {
		if len(labels) > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, extraKey...)
		buf = append(buf, '=', '"')
		buf = append(buf, extraValue...)
		buf = append(buf, '"')
	}

	return append(buf, '}', ' ')
}
