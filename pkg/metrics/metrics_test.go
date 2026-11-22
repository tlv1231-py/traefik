package metrics

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// naiveRegistry reproduces the hot path as it was before the fix: a label map
// built per observation and a key built by string concatenation. It is the
// baseline the allocation reduction is measured against.
type naiveRegistry struct {
	mu     sync.Mutex
	series map[string]*naiveSeries
}

type naiveSeries struct {
	count int64
	sum   float64
}

func newNaiveRegistry() *naiveRegistry {
	return &naiveRegistry{series: map[string]*naiveSeries{}}
}

func (n *naiveRegistry) observe(name string, labels map[string]string, value float64) {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	key := name
	for _, k := range keys {
		key += "," + k + "=" + labels[k]
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	s := n.series[key]
	if s == nil {
		s = &naiveSeries{}
		n.series[key] = s
	}
	s.count++
	s.sum += value
}

var benchLabels = []string{
	"entrypoint", "web",
	"service", "my-service@docker",
	"method", "GET",
	"code", "200",
	"protocol", "http",
}

func benchLabelMap() map[string]string {
	return map[string]string{
		"entrypoint": "web",
		"service":    "my-service@docker",
		"method":     "GET",
		"code":       "200",
		"protocol":   "http",
	}
}

func TestCounterAndHistogram(t *testing.T) {
	reg := NewRegistry(Options{Buckets: []float64{0.1, 1}})

	for i := 0; i < 3; i++ {
		reg.CounterAdd("requests_total", benchLabels, 1)
		reg.ObserveDuration("request_duration_seconds", benchLabels, 0.5)
	}

	if got := reg.Cardinality(); got != 2 {
		t.Fatalf("Cardinality() = %d, want 2", got)
	}

	var counter, histogram *Series
	for _, s := range reg.Snapshot(nil) {
		switch s.Name() {
		case "requests_total":
			counter = s
		case "request_duration_seconds":
			histogram = s
		}
	}

	if counter == nil || histogram == nil {
		t.Fatalf("missing series: counter=%v histogram=%v", counter, histogram)
	}
	if counter.Sum() != 3 {
		t.Errorf("counter Sum() = %v, want 3", counter.Sum())
	}
	if histogram.Count() != 3 {
		t.Errorf("histogram Count() = %d, want 3", histogram.Count())
	}
	if histogram.Sum() != 1.5 {
		t.Errorf("histogram Sum() = %v, want 1.5", histogram.Sum())
	}

	var buf bytes.Buffer
	if _, err := reg.WriteText(&buf); err != nil {
		t.Fatalf("WriteText() error = %v", err)
	}

	out := buf.String()
	for _, want := range []string{
		`requests_total{entrypoint="web"`,
		`request_duration_seconds_bucket{`,
		`le="1"} 3`,
		`request_duration_seconds_count{`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("WriteText() output missing %q:\n%s", want, out)
		}
	}
}

// TestHotPathDoesNotAllocate covers the main source of the heap growth: the
// per-request allocations of the collection path.
func TestHotPathDoesNotAllocate(t *testing.T) {
	reg := NewRegistry(Options{})

	allocs := testing.AllocsPerRun(1000, func() {
		reg.CounterAdd("requests_total", benchLabels, 1)
		reg.ObserveDuration("request_duration_seconds", benchLabels, 0.42)
	})

	if allocs != 0 {
		t.Errorf("AllocsPerRun() = %v, want 0 allocation per observation", allocs)
	}
}

// TestAllocationReduction asserts the acceptance criterion of the issue: at
// least 40% fewer allocations than the previous implementation.
func TestAllocationReduction(t *testing.T) {
	reg := NewRegistry(Options{})
	naive := newNaiveRegistry()

	pooled := testing.AllocsPerRun(500, func() {
		reg.CounterAdd("requests_total", benchLabels, 1)
	})

	before := testing.AllocsPerRun(500, func() {
		naive.observe("requests_total", benchLabelMap(), 1)
	})

	if before == 0 {
		t.Fatal("baseline allocated nothing, the comparison is meaningless")
	}

	if reduction := 1 - pooled/before; reduction < 0.4 {
		t.Errorf("allocation reduction = %.2f (%v -> %v allocs/op), want >= 0.40",
			reduction, before, pooled)
	}
}

func TestCardinalityCap(t *testing.T) {
	const (
		max        = 100
		iterations = 10000
	)

	reg := NewRegistry(Options{MaxCardinality: max})

	labels := []string{"service", "", "code", "200"}
	for i := 0; i < iterations; i++ {
		labels[1] = fmt.Sprintf("/users/%d", i)
		reg.CounterAdd("requests_total", labels, 1)
	}

	if got := reg.Cardinality(); got > max {
		t.Errorf("Cardinality() = %d, want <= %d", got, max)
	}
	if reg.DroppedSeries() == 0 {
		t.Error("DroppedSeries() = 0, want the observations over the cap to be reported")
	}

	// No observation is lost: the ones over the cap are aggregated.
	if got := reg.Observations(); got != iterations {
		t.Errorf("Observations() = %d, want %d", got, iterations)
	}

	var overflow *Series
	for _, s := range reg.Snapshot(nil) {
		if len(s.Labels()) > 1 && s.Labels()[1] == OverflowLabelValue {
			overflow = s
		}
	}
	if overflow == nil {
		t.Fatal("no overflow series recorded")
	}
	if overflow.Labels()[0] != "service" {
		t.Errorf("overflow series lost its label names: %v", overflow.Labels())
	}
}

func TestCardinalityCapCanBeDisabled(t *testing.T) {
	reg := NewRegistry(Options{MaxCardinality: -1})

	labels := []string{"service", ""}
	for i := 0; i < 500; i++ {
		labels[1] = fmt.Sprintf("service-%d", i)
		reg.CounterAdd("requests_total", labels, 1)
	}

	if got := reg.Cardinality(); got != 500 {
		t.Errorf("Cardinality() = %d, want 500 with the cap disabled", got)
	}
	if got := reg.DroppedSeries(); got != 0 {
		t.Errorf("DroppedSeries() = %d, want 0 with the cap disabled", got)
	}
}

// TestStableMemoryUnderRandomPaths is the regression test for the reported
// symptom: traffic with randomized paths must not grow the heap indefinitely.
func TestStableMemoryUnderRandomPaths(t *testing.T) {
	reg := NewRegistry(Options{MaxCardinality: 1000})
	rnd := rand.New(rand.NewSource(1))
	labels := []string{"service", "my-service", "path", ""}

	record := func(iterations int) {
		for i := 0; i < iterations; i++ {
			labels[3] = fmt.Sprintf("/users/%d/items/%d", rnd.Int63(), rnd.Int63())
			reg.CounterAdd("requests_total", labels, 1)
			reg.ObserveDuration("request_duration_seconds", labels, rnd.Float64())
		}
	}

	// Warm up until the cap is reached and the heap has settled.
	record(50000)

	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	runtime.GC()

	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	record(200000)

	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	runtime.GC()

	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	if delta := int64(after.Alloc) - int64(before.Alloc); delta > 200*1024 {
		t.Errorf("memory growth detected: Alloc increased by %d bytes over 200000 requests", delta)
	}
	if got := reg.Cardinality(); got > 1000 {
		t.Errorf("Cardinality() = %d, want <= 1000", got)
	}
}

func TestConcurrentObservations(t *testing.T) {
	reg := NewRegistry(Options{MaxCardinality: 50})

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()

			labels := []string{"service", "", "code", "200"}
			for i := 0; i < 2000; i++ {
				labels[1] = fmt.Sprintf("service-%d-%d", worker, i)
				reg.CounterAdd("requests_total", labels, 1)
			}
		}(w)
	}
	wg.Wait()

	if got := reg.Cardinality(); got > 50 {
		t.Errorf("Cardinality() = %d, want <= 50", got)
	}
	if got := reg.Observations(); got != 16000 {
		t.Errorf("Observations() = %d, want 16000", got)
	}
}

func BenchmarkMetricsPooledObserve(b *testing.B) {
	reg := NewRegistry(Options{})

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reg.CounterAdd("requests_total", benchLabels, 1)
		reg.ObserveDuration("request_duration_seconds", benchLabels, 0.42)
	}
}

func BenchmarkMetricsNaiveObserve(b *testing.B) {
	naive := newNaiveRegistry()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		naive.observe("requests_total", benchLabelMap(), 1)
		naive.observe("request_duration_seconds", benchLabelMap(), 0.42)
	}
}

func BenchmarkMetricsParallelObserve(b *testing.B) {
	reg := NewRegistry(Options{})

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			reg.CounterAdd("requests_total", benchLabels, 1)
		}
	})
}

func BenchmarkMetricsHighCardinality(b *testing.B) {
	reg := NewRegistry(Options{MaxCardinality: 1000})
	labels := []string{"service", "my-service", "path", ""}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		labels[3] = "/users/42"
		reg.CounterAdd("requests_total", labels, 1)
	}
}

func BenchmarkMetricsWriteText(b *testing.B) {
	reg := NewRegistry(Options{})

	labels := []string{"service", "", "code", "200"}
	for i := 0; i < 500; i++ {
		labels[1] = fmt.Sprintf("service-%d", i)
		reg.CounterAdd("requests_total", labels, 1)
		reg.ObserveDuration("request_duration_seconds", labels, 0.42)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := reg.WriteText(io.Discard); err != nil {
			b.Fatal(err)
		}
	}
}
