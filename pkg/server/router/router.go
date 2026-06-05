package router

import (
	"context"
	"net/http"
	"sync"
)

// Router manages the routing and transports.
type Router struct {
	Transports []*http.Transport
	cancel     context.CancelFunc
}

// Close cleans up the router's resources.
func (r *Router) Close() {
	if r.cancel != nil {
		r.cancel()
	}
	for _, t := range r.Transports {
		t.CloseIdleConnections()
	}
}

// Manager manages the active routers.
type Manager struct {
	mu             sync.Mutex
	currentRouters map[string]*Router
}

// NewManager creates a new Manager.
func NewManager() *Manager {
	return &Manager{
		currentRouters: make(map[string]*Router),
	}
}

// Update updates the routers based on the new configuration.
func (m *Manager) Update(data map[string]string, ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Close and release old routers
	for _, r := range m.currentRouters {
		r.Close()
	}
	m.currentRouters = make(map[string]*Router)

	// Create new router
	routerCtx, routerCancel := context.WithCancel(ctx)
	
	transport := &http.Transport{}
	r := &Router{
		Transports: []*http.Transport{transport},
		cancel:     routerCancel,
	}

	// Spawn a goroutine that simulates background work for this router
	go func(c context.Context) {
		<-c.Done()
	}(routerCtx)

	m.currentRouters["default"] = r
}

// Close closes all managed routers.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.currentRouters {
		r.Close()
	}
	m.currentRouters = nil
}
