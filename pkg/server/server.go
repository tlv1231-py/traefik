package server

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/aozorayama45/Traefik/pkg/server/router"
)

// Configuration holds the dynamic configuration data.
type Configuration struct {
	Data map[string]string
}

// Server is the main server struct.
type Server struct {
	currentConfiguration atomic.Value // holds *Configuration
	routerManager        *router.Manager
	mu                   sync.Mutex
	cancel               context.CancelFunc
	ctx                  context.Context
}

// NewServer creates and initializes a new Server.
func NewServer() *Server {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		routerManager: router.NewManager(),
		ctx:           ctx,
		cancel:        cancel,
	}
	s.currentConfiguration.Store(&Configuration{})
	return s
}

// SwitchConfigs updates the server configuration and cleans up old resources.
func (s *Server) SwitchConfigs(newConfig *Configuration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Store the new configuration
	s.currentConfiguration.Store(newConfig)

	// Update the router manager with the new configuration
	s.routerManager.Update(newConfig.Data, s.ctx)
}

// Close stops the server and cleans up all resources.
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cancel()
	s.routerManager.Close()
}
