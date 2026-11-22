// Package metrics provides the middleware recording the request metrics
// (count, status code and latency) of an entry point, a router or a service.
//
// Label values are the main driver of the registry memory usage, so they are
// kept under control here: router and service names are sanitized once, the
// request method is mapped onto the known methods, and the request path only
// becomes a label when it matches one of the configured path templates.
package metrics

import (
	"bufio"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aozorayama45/Traefik/pkg/metrics"
)

// Names of the metrics recorded by the middleware. They are unchanged from the
// previous implementation, as are the label names below.
const (
	RequestsTotalName   = "traefik_service_requests_total"
	RequestDurationName = "traefik_service_request_duration_seconds"
)

const (
	entryPointLabel = "entrypoint"
	routerLabel     = "router"
	serviceLabel    = "service"
	methodLabel     = "method"
	codeLabel       = "code"
	protocolLabel   = "protocol"
	pathLabel       = "path"
)

const (
	// unknownValue replaces a label value that is empty once sanitized.
	unknownValue = "unknown"

	// otherPathValue is the path label of the requests that match none of the
	// configured templates. It is what keeps dynamic paths (ids, UUIDs, query
	// specific segments) from creating one series per request.
	otherPathValue = "other"

	// extensionMethod mirrors the Prometheus provider: methods outside of the
	// known set share a single series.
	extensionMethod = "EXTENSION_METHOD"

	// maxLabelValueLen bounds the length of a sanitized label value.
	maxLabelValueLen = 128
)

// Config holds the static identity of the handler being measured.
type Config struct {
	EntryPoint string
	Router     string
	Service    string

	// PathTemplates is the safe mapping schema of the path label: only a
	// request path matching one of these templates (such as /users/{id}) is
	// turned into a label value, the template itself. When empty, no path
	// label is recorded at all.
	PathTemplates []string
}

// Middleware records the metrics of the requests it forwards to next.
type Middleware struct {
	next     http.Handler
	registry *metrics.Registry

	// Sanitized once, at build time: these never change per request.
	entryPoint string
	router     string
	service    string

	templates []string
}

// New returns a middleware recording the metrics of next into registry.
func New(next http.Handler, registry *metrics.Registry, cfg Config) *Middleware {
	m := &Middleware{
		next:     next,
		registry: registry,
	}

	opts := registry.Options()
	if opts.AddEntryPointsLabels && cfg.EntryPoint != "" {
		m.entryPoint = SanitizeLabelValue(cfg.EntryPoint)
	}
	if opts.AddRoutersLabels && cfg.Router != "" {
		m.router = SanitizeLabelValue(cfg.Router)
	}
	if opts.AddServicesLabels && cfg.Service != "" {
		m.service = SanitizeLabelValue(cfg.Service)
	}

	for _, tmpl := range cfg.PathTemplates {
		if tmpl != "" {
			m.templates = append(m.templates, tmpl)
		}
	}

	return m
}

// labelsPool holds the flattened label slices of the hot path. The registry
// only reads them, so they go back to the pool once the observation is done.
var labelsPool = sync.Pool{
	New: func() interface{} {
		labels := make([]string, 0, 16)
		return &labels
	},
}

// recorderPool holds the response writers capturing the status code.
var recorderPool = sync.Pool{
	New: func() interface{} {
		return &responseRecorder{}
	},
}

func (m *Middleware) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	start := time.Now()

	recorder := recorderPool.Get().(*responseRecorder)
	recorder.reset(rw)

	m.next.ServeHTTP(recorder, req)

	duration := time.Since(start).Seconds()
	code := recorder.status

	recorder.reset(nil)
	recorderPool.Put(recorder)

	labelsp := labelsPool.Get().(*[]string)
	labels := (*labelsp)[:0]

	if m.entryPoint != "" {
		labels = append(labels, entryPointLabel, m.entryPoint)
	}
	if m.router != "" {
		labels = append(labels, routerLabel, m.router)
	}
	if m.service != "" {
		labels = append(labels, serviceLabel, m.service)
	}

	labels = append(labels,
		methodLabel, sanitizeMethod(req.Method),
		codeLabel, statusString(code),
		protocolLabel, protocol(req),
	)

	if len(m.templates) > 0 {
		labels = append(labels, pathLabel, m.matchPath(req.URL.Path))
	}

	m.registry.CounterAdd(RequestsTotalName, labels, 1)
	m.registry.ObserveDuration(RequestDurationName, labels, duration)

	*labelsp = labels
	labelsPool.Put(labelsp)
}

// matchPath returns the template the path matches, or otherPathValue. The raw
// path is never used as a label value.
func (m *Middleware) matchPath(path string) string {
	for _, tmpl := range m.templates {
		if matchTemplate(tmpl, path) {
			return tmpl
		}
	}

	return otherPathValue
}

// matchTemplate reports whether path matches tmpl, where a {placeholder}
// segment of tmpl matches any single non-empty path segment. It walks both
// strings in place, so it does not allocate.
func matchTemplate(tmpl, path string) bool {
	ti, pi := 0, 0

	for ti < len(tmpl) || pi < len(path) {
		if ti >= len(tmpl) || pi >= len(path) {
			return false
		}
		if tmpl[ti] != '/' || path[pi] != '/' {
			return false
		}
		ti++
		pi++

		tEnd := segmentEnd(tmpl, ti)
		pEnd := segmentEnd(path, pi)

		tSeg := tmpl[ti:tEnd]
		pSeg := path[pi:pEnd]

		if len(tSeg) > 1 && tSeg[0] == '{' && tSeg[len(tSeg)-1] == '}' {
			if len(pSeg) == 0 {
				return false
			}
		} else if tSeg != pSeg {
			return false
		}

		ti = tEnd
		pi = pEnd
	}

	return true
}

func segmentEnd(s string, from int) int {
	if i := strings.IndexByte(s[from:], '/'); i >= 0 {
		return from + i
	}

	return len(s)
}

// SanitizeLabelValue strips the characters that have no business in a label
// value and bounds its length. The value is returned unchanged, without any
// allocation, when it is already clean.
func SanitizeLabelValue(value string) string {
	if value == "" {
		return unknownValue
	}

	clean := len(value) <= maxLabelValueLen
	if clean {
		for i := 0; i < len(value); i++ {
			if !allowedLabelByte(value[i]) {
				clean = false
				break
			}
		}
	}

	if clean {
		return value
	}

	if len(value) > maxLabelValueLen {
		value = value[:maxLabelValueLen]
	}

	var b strings.Builder
	b.Grow(len(value))

	for i := 0; i < len(value); i++ {
		if allowedLabelByte(value[i]) {
			b.WriteByte(value[i])
		} else {
			b.WriteByte('_')
		}
	}

	return b.String()
}

func allowedLabelByte(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z',
		c >= 'A' && c <= 'Z',
		c >= '0' && c <= '9':
		return true
	case c == '-', c == '_', c == '.', c == '@', c == ':':
		return true
	default:
		return false
	}
}

// sanitizeMethod maps a request method onto a bounded set of label values: a
// client is free to send an arbitrary method, which would otherwise create one
// series per distinct value.
func sanitizeMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut,
		http.MethodPatch, http.MethodDelete, http.MethodConnect,
		http.MethodOptions, http.MethodTrace:
		return method
	default:
		return extensionMethod
	}
}

func protocol(req *http.Request) string {
	if isWebsocketRequest(req) {
		return "websocket"
	}

	return "http"
}

func isWebsocketRequest(req *http.Request) bool {
	return strings.EqualFold(req.Header.Get("Connection"), "upgrade") &&
		strings.EqualFold(req.Header.Get("Upgrade"), "websocket")
}

// statusStrings caches the string form of the status codes, so that recording
// a request does not allocate one per observation.
var statusStrings [600]string

func init() {
	for code := 100; code < len(statusStrings); code++ {
		statusStrings[code] = strconv.Itoa(code)
	}
}

func statusString(code int) string {
	if code >= 100 && code < len(statusStrings) {
		return statusStrings[code]
	}

	return strconv.Itoa(code)
}

// responseRecorder captures the status code of the response. It is pooled, so
// measuring a request does not allocate a wrapper per request.
type responseRecorder struct {
	http.ResponseWriter

	status int
}

func (r *responseRecorder) reset(rw http.ResponseWriter) {
	r.ResponseWriter = rw
	r.status = http.StatusOK
}

func (r *responseRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *responseRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *responseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := r.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}

	return nil, nil, http.ErrNotSupported
}
