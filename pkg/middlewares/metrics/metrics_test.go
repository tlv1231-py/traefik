package metrics

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aozorayama45/Traefik/pkg/metrics"
)

func allLabels() metrics.Options {
	return metrics.Options{
		AddEntryPointsLabels: true,
		AddRoutersLabels:     true,
		AddServicesLabels:    true,
	}
}

func okHandler() http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusOK)
	})
}

// labelValue returns the value of key on the first series of the registry
// carrying it.
func labelValue(t *testing.T, reg *metrics.Registry, metric, key string) string {
	t.Helper()

	for _, s := range reg.Snapshot(nil) {
		if s.Name() != metric {
			continue
		}

		labels := s.Labels()
		for i := 0; i+1 < len(labels); i += 2 {
			if labels[i] == key {
				return labels[i+1]
			}
		}
	}

	t.Fatalf("no series %q with label %q in %v", metric, key, reg.Snapshot(nil))

	return ""
}

func TestMiddlewareRecordsRequests(t *testing.T) {
	reg := metrics.NewRegistry(allLabels())

	handler := New(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusTeapot)
	}), reg, Config{EntryPoint: "web", Router: "my-router", Service: "my-service"})

	for i := 0; i < 3; i++ {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api", nil))
	}

	// One counter and one histogram, for a single label set.
	if got := reg.Cardinality(); got != 2 {
		t.Fatalf("Cardinality() = %d, want 2", got)
	}

	for _, test := range []struct{ key, want string }{
		{entryPointLabel, "web"},
		{routerLabel, "my-router"},
		{serviceLabel, "my-service"},
		{methodLabel, http.MethodGet},
		{codeLabel, "418"},
		{protocolLabel, "http"},
	} {
		if got := labelValue(t, reg, RequestsTotalName, test.key); got != test.want {
			t.Errorf("label %q = %q, want %q", test.key, got, test.want)
		}
	}

	for _, s := range reg.Snapshot(nil) {
		if s.Name() == RequestsTotalName && s.Sum() != 3 {
			t.Errorf("%s = %v, want 3", RequestsTotalName, s.Sum())
		}
		if s.Name() == RequestDurationName && s.Count() != 3 {
			t.Errorf("%s count = %d, want 3", RequestDurationName, s.Count())
		}
	}
}

func TestMiddlewareDefaultsToStatusOK(t *testing.T) {
	reg := metrics.NewRegistry(allLabels())

	handler := New(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		_, _ = rw.Write([]byte("body"))
	}), reg, Config{Service: "my-service"})

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api", nil))

	if got := labelValue(t, reg, RequestsTotalName, codeLabel); got != "200" {
		t.Errorf("code label = %q, want 200", got)
	}
}

// TestMiddlewareIgnoresDynamicPaths is the regression test for the reported
// cardinality explosion: raw request paths must never become label values.
func TestMiddlewareIgnoresDynamicPaths(t *testing.T) {
	reg := metrics.NewRegistry(allLabels())
	handler := New(okHandler(), reg, Config{Service: "my-service"})

	for i := 0; i < 1000; i++ {
		target := fmt.Sprintf("/users/%d?token=%d", i, i)
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, target, nil))
	}

	if got := reg.Cardinality(); got != 2 {
		t.Errorf("Cardinality() = %d after 1000 distinct paths, want 2", got)
	}
}

func TestMiddlewarePathTemplates(t *testing.T) {
	reg := metrics.NewRegistry(allLabels())
	handler := New(okHandler(), reg, Config{
		Service:       "my-service",
		PathTemplates: []string{"/users/{id}", "/users/{id}/items/{itemID}"},
	})

	for i := 0; i < 100; i++ {
		for _, target := range []string{
			fmt.Sprintf("/users/%d", i),
			fmt.Sprintf("/users/%d/items/%d", i, i),
			fmt.Sprintf("/blobs/%d", i),
		} {
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, target, nil))
		}
	}

	// Two templates plus "other", for the counter and the histogram.
	if got := reg.Cardinality(); got != 6 {
		t.Errorf("Cardinality() = %d, want 6 (3 path values x 2 metrics)", got)
	}

	counts := map[string]float64{}
	for _, s := range reg.Snapshot(nil) {
		if s.Name() != RequestsTotalName {
			continue
		}

		labels := s.Labels()
		for i := 0; i+1 < len(labels); i += 2 {
			if labels[i] == pathLabel {
				counts[labels[i+1]] = s.Sum()
			}
		}
	}

	for path, want := range map[string]float64{
		"/users/{id}":                100,
		"/users/{id}/items/{itemID}": 100,
		otherPathValue:               100,
	} {
		if counts[path] != want {
			t.Errorf("requests with path label %q = %v, want %v", path, counts[path], want)
		}
	}
}

func TestMiddlewareBoundsTheMethodLabel(t *testing.T) {
	reg := metrics.NewRegistry(allLabels())
	handler := New(okHandler(), reg, Config{Service: "my-service"})

	req := httptest.NewRequest(http.MethodGet, "/api", nil)
	req.Method = "WHATEVER"
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if got := labelValue(t, reg, RequestsTotalName, methodLabel); got != extensionMethod {
		t.Errorf("method label = %q, want %q", got, extensionMethod)
	}
}

func TestMiddlewareSanitizesConfiguredLabels(t *testing.T) {
	reg := metrics.NewRegistry(allLabels())
	handler := New(okHandler(), reg, Config{
		Router:  "my router\n{injected}",
		Service: strings.Repeat("x", maxLabelValueLen+50),
	})

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api", nil))

	if got := labelValue(t, reg, RequestsTotalName, routerLabel); got != "my_router__injected_" {
		t.Errorf("router label = %q, want %q", got, "my_router__injected_")
	}
	if got := labelValue(t, reg, RequestsTotalName, serviceLabel); len(got) != maxLabelValueLen {
		t.Errorf("service label length = %d, want %d", len(got), maxLabelValueLen)
	}
}

func TestMiddlewareHonoursDisabledLabels(t *testing.T) {
	reg := metrics.NewRegistry(metrics.Options{AddServicesLabels: true})
	handler := New(okHandler(), reg, Config{EntryPoint: "web", Router: "my-router", Service: "my-service"})

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api", nil))

	for _, s := range reg.Snapshot(nil) {
		labels := s.Labels()
		for i := 0; i+1 < len(labels); i += 2 {
			if labels[i] == entryPointLabel || labels[i] == routerLabel {
				t.Errorf("label %q recorded while disabled", labels[i])
			}
		}
	}
}

func TestMiddlewareRespectsTheCardinalityCap(t *testing.T) {
	reg := metrics.NewRegistry(metrics.Options{MaxCardinality: 10, AddServicesLabels: true})

	// A template per path would be a configuration mistake; the cap is the
	// safety net that keeps it from being an OOM.
	templates := make([]string, 0, 200)
	for i := 0; i < 200; i++ {
		templates = append(templates, fmt.Sprintf("/p%d/{id}", i))
	}

	handler := New(okHandler(), reg, Config{Service: "my-service", PathTemplates: templates})

	for i := 0; i < 200; i++ {
		target := fmt.Sprintf("/p%d/%d", i, i)
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, target, nil))
	}

	if got := reg.Cardinality(); got > 10 {
		t.Errorf("Cardinality() = %d, want <= 10", got)
	}
	if got := reg.Observations(); got != 400 {
		t.Errorf("Observations() = %d, want 400 (2 metrics x 200 requests)", got)
	}
}

func TestMiddlewareHotPathDoesNotAllocate(t *testing.T) {
	reg := metrics.NewRegistry(allLabels())
	handler := New(okHandler(), reg, Config{
		EntryPoint:    "web",
		Router:        "my-router",
		Service:       "my-service",
		PathTemplates: []string{"/users/{id}"},
	})

	rw := &nopResponseWriter{header: http.Header{}}
	req := httptest.NewRequest(http.MethodGet, "/users/42", nil)

	allocs := testing.AllocsPerRun(1000, func() {
		handler.ServeHTTP(rw, req)
	})

	if allocs != 0 {
		t.Errorf("AllocsPerRun() = %v, want 0 allocation per request", allocs)
	}
}

func TestMatchTemplate(t *testing.T) {
	for _, test := range []struct {
		tmpl, path string
		want       bool
	}{
		{"/users/{id}", "/users/42", true},
		{"/users/{id}", "/users/f81d4fae-7dec-11d0-a765-00a0c91e6bf6", true},
		{"/users/{id}", "/users/42/items", false},
		{"/users/{id}", "/users/", false},
		{"/users/{id}", "/users", false},
		{"/users/{id}", "/accounts/42", false},
		{"/users/{id}/items/{itemID}", "/users/42/items/7", true},
		{"/users/{id}/items/{itemID}", "/users/42/items", false},
		{"/health", "/health", true},
		{"/health", "/health/", false},
		{"/", "/", true},
	} {
		if got := matchTemplate(test.tmpl, test.path); got != test.want {
			t.Errorf("matchTemplate(%q, %q) = %v, want %v", test.tmpl, test.path, got, test.want)
		}
	}
}

func TestSanitizeLabelValue(t *testing.T) {
	for _, test := range []struct{ in, want string }{
		{"", unknownValue},
		{"my-service@docker", "my-service@docker"},
		{"web", "web"},
		{"a b", "a_b"},
		{"a\"b\n", "a_b_"},
		{strings.Repeat("x", maxLabelValueLen+1), strings.Repeat("x", maxLabelValueLen)},
	} {
		if got := SanitizeLabelValue(test.in); got != test.want {
			t.Errorf("SanitizeLabelValue(%q) = %q, want %q", test.in, got, test.want)
		}
	}
}

func BenchmarkMetricsMiddleware(b *testing.B) {
	reg := metrics.NewRegistry(allLabels())
	handler := New(okHandler(), reg, Config{
		EntryPoint:    "web",
		Router:        "my-router",
		Service:       "my-service",
		PathTemplates: []string{"/users/{id}"},
	})

	rw := &nopResponseWriter{header: http.Header{}}
	req := httptest.NewRequest(http.MethodGet, "/users/42", nil)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		handler.ServeHTTP(rw, req)
	}
}

type nopResponseWriter struct {
	header http.Header
}

func (w *nopResponseWriter) Header() http.Header         { return w.header }
func (w *nopResponseWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *nopResponseWriter) WriteHeader(int)             {}
