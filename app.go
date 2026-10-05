package containerproxy

import (
	"errors"
	"fmt"
	"sync"

	"github.com/caddyserver/caddy/v2"
)

const lifecycleAppID = "container_proxy_lifecycle"

func init() {
	caddy.RegisterModule(new(lifecycleApp))
}

type lifecycleApp struct {
	mu       sync.Mutex
	managers map[*manager]struct{}
	started  bool
}

func (*lifecycleApp) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  lifecycleAppID,
		New: func() caddy.Module { return new(lifecycleApp) },
	}
}

func (*lifecycleApp) Provision(caddy.Context) error { return nil }

func (a *lifecycleApp) register(m *manager) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.started {
		return errors.New("container lifecycle app already started")
	}
	if a.managers == nil {
		a.managers = make(map[*manager]struct{})
	}
	a.managers[m] = struct{}{}
	return nil
}

func (a *lifecycleApp) Start() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.started {
		return nil
	}
	activated := make([]*manager, 0, len(a.managers))
	for manager := range a.managers {
		if err := manager.activate(); err != nil {
			for _, previous := range activated {
				previous.rollbackActivation()
			}
			return fmt.Errorf("activating container %s lifecycle: %w", manager.clientID, err)
		}
		activated = append(activated, manager)
	}
	a.started = true
	return nil
}

func (a *lifecycleApp) Stop() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.started {
		return nil
	}
	for manager := range a.managers {
		manager.deactivate()
	}
	a.started = false
	return nil
}

var _ caddy.Module = (*lifecycleApp)(nil)
var _ caddy.Provisioner = (*lifecycleApp)(nil)
var _ caddy.App = (*lifecycleApp)(nil)
