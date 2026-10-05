package containerproxy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"go.uber.org/zap"
)

func TestConcurrentColdRequestsShareStartAndWaiterCancellation(t *testing.T) {
	docker := newFakeDocker(container.StateCreated)
	manager := newManager(docker, "test-id", lifecyclePolicy{idleTimeout: time.Hour}, managerStopped, zap.NewNop())
	manager.activate()
	t.Cleanup(func() { _ = manager.Destruct() })

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, ctx := range []context.Context{firstCtx, context.Background()} {
		wg.Add(1)
		go func(ctx context.Context) {
			defer wg.Done()
			_, err := manager.Acquire(ctx)
			if err == nil {
				manager.Release()
			}
			results <- err
		}(ctx)
	}
	<-docker.startEntered
	waitForActiveLeases(t, manager, 2)
	cancelFirst()
	if err := <-results; err == nil {
		t.Fatal("cancelled waiter unexpectedly acquired a lease")
	}
	docker.allowStart <- struct{}{}
	if err := <-results; err != nil {
		t.Fatalf("remaining waiter failed: %v", err)
	}
	wg.Wait()

	docker.mu.Lock()
	starts := docker.starts
	docker.mu.Unlock()
	if starts != 1 {
		t.Fatalf("got %d container starts; want exactly one", starts)
	}
}

func TestIdleStopWaitsForFinalLease(t *testing.T) {
	docker := newFakeDocker(container.StateRunning)
	manager := newManager(docker, "test-id", lifecyclePolicy{idleTimeout: 35 * time.Millisecond}, managerReady, zap.NewNop())
	manager.activate()
	t.Cleanup(func() { _ = manager.Destruct() })

	if _, err := manager.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Release()
	select {
	case <-docker.stopped:
		t.Fatal("container stopped while an active lease remained")
	case <-time.After(60 * time.Millisecond):
	}
	manager.Release()
	select {
	case <-docker.stopped:
	case <-time.After(time.Second):
		t.Fatal("idle container was not stopped")
	}
}

func TestArrivalDuringStopStartsOnlyAfterStopCompletes(t *testing.T) {
	docker := newFakeDocker(container.StateRunning)
	docker.stopEntered = make(chan struct{}, 1)
	docker.allowStop = make(chan struct{}, 1)
	manager := newManager(docker, "test-id", lifecyclePolicy{idleTimeout: 25 * time.Millisecond}, managerRunning, zap.NewNop())
	manager.activate()
	t.Cleanup(func() { _ = manager.Destruct() })

	if _, err := manager.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Release()
	select {
	case <-docker.stopEntered:
	case <-time.After(time.Second):
		t.Fatal("idle stop did not begin")
	}

	acquired := make(chan error, 1)
	go func() {
		_, err := manager.Acquire(context.Background())
		if err == nil {
			manager.Release()
		}
		acquired <- err
	}()
	waitForActiveLeases(t, manager, 1)
	docker.mu.Lock()
	starts := docker.starts
	docker.mu.Unlock()
	if starts != 0 {
		t.Fatal("container start raced with an in-flight stop")
	}

	docker.allowStop <- struct{}{}
	<-docker.startEntered
	docker.allowStart <- struct{}{}
	if err := <-acquired; err != nil {
		t.Fatalf("request after idle stop failed to restart container: %v", err)
	}
	manager.mu.Lock()
	manager.stopIdleTimerLocked()
	manager.policy.idleTimeout = time.Hour
	manager.mu.Unlock()
	docker.mu.Lock()
	docker.stopEntered = nil
	docker.mu.Unlock()
}

func TestHealthProbeControlsReadiness(t *testing.T) {
	var probes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if probes.Add(1) == 1 {
			http.Error(w, "warming", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	docker := newFakeDocker(container.StateCreated)
	docker.allowStart <- struct{}{}
	manager := newManager(docker, "test-id", lifecyclePolicy{
		idleTimeout: time.Hour,
		healthURL:   server.URL + "/health/ready",
	}, managerStopped, zap.NewNop())
	manager.activate()
	t.Cleanup(func() { _ = manager.Destruct() })

	start := time.Now()
	if _, err := manager.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	manager.Release()
	if probes.Load() < 2 {
		t.Fatalf("readiness probes = %d; expected a retry after the initial 503", probes.Load())
	}
	// The fixed 250 ms poll interval prevents an immediate retry loop.
	if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
		t.Fatalf("readiness completed in %s; expected the poll interval", elapsed)
	}
}

func TestRequestDeadlineCancelsUnneededReadinessAndReleasesLease(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "warming", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	docker := newFakeDocker(container.StateCreated)
	docker.allowStart <- struct{}{}
	manager := newManager(docker, "test-id", lifecyclePolicy{
		idleTimeout: time.Hour,
		healthURL:   server.URL + "/health/ready",
	}, managerStopped, zap.NewNop())
	manager.activate()
	t.Cleanup(func() { _ = manager.Destruct() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := manager.Acquire(ctx); err != context.DeadlineExceeded {
		t.Fatalf("Acquire error = %v; want deadline exceeded", err)
	}
	waitForState(t, manager, managerRunning)
	manager.mu.Lock()
	active := manager.active
	manager.mu.Unlock()
	if active != 0 {
		t.Fatalf("active lease count = %d after request timeout; want 0", active)
	}
}

func TestCleanupDeadlineCoversDockerOperations(t *testing.T) {
	previousLimit := cleanupLimit
	cleanupLimit = 120 * time.Millisecond
	t.Cleanup(func() { cleanupLimit = previousLimit })

	docker := newFakeDocker(container.StateRunning)
	docker.inspectDelay = 80 * time.Millisecond
	docker.stopDelay = 80 * time.Millisecond
	manager := newManager(docker, "test-id", lifecyclePolicy{idleTimeout: time.Hour}, managerRunning, zap.NewNop())
	if err := manager.activate(); err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	err := manager.Destruct()
	elapsed := time.Since(started)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cleanup error = %v; want shared cleanup deadline", err)
	}
	if elapsed < 100*time.Millisecond || elapsed > 250*time.Millisecond {
		t.Fatalf("cleanup took %s; want approximately one 120 ms budget", elapsed)
	}
	docker.mu.Lock()
	stops, closes := docker.stops, docker.closes
	docker.mu.Unlock()
	if stops != 1 || closes != 1 {
		t.Fatalf("Docker stops/closes = %d/%d; want 1/1", stops, closes)
	}
}

func waitForActiveLeases(t *testing.T, manager *manager, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		manager.mu.Lock()
		active := manager.active
		manager.mu.Unlock()
		if active == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("active lease count did not reach %d", want)
}

func waitForState(t *testing.T, manager *manager, want managerState) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		manager.mu.Lock()
		state := manager.state
		manager.mu.Unlock()
		if state == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("manager state did not become %q", want)
}

type fakeDocker struct {
	mu           sync.Mutex
	state        container.State
	starts       int
	stops        int
	closes       int
	inspectDelay time.Duration
	stopDelay    time.Duration
	startEntered chan struct{}
	allowStart   chan struct{}
	stopEntered  chan struct{}
	allowStop    chan struct{}
	stopped      chan struct{}
}

func newFakeDocker(state container.ContainerState) *fakeDocker {
	return &fakeDocker{
		state:        container.State{Status: state, Running: state == container.StateRunning},
		startEntered: make(chan struct{}, 1),
		allowStart:   make(chan struct{}, 1),
		stopped:      make(chan struct{}, 1),
	}
}

func (d *fakeDocker) ContainerInspect(ctx context.Context, _ string) (container.InspectResponse, error) {
	d.mu.Lock()
	delay := d.inspectDelay
	d.mu.Unlock()
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return container.InspectResponse{}, ctx.Err()
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	state := d.state
	return container.InspectResponse{ContainerJSONBase: &container.ContainerJSONBase{
		ID: "test-id", State: &state,
	}}, nil
}

func (d *fakeDocker) ContainerStart(ctx context.Context, _ string, _ container.StartOptions) error {
	d.mu.Lock()
	d.starts++
	d.mu.Unlock()
	d.startEntered <- struct{}{}
	select {
	case <-d.allowStart:
		d.mu.Lock()
		d.state = container.State{Status: container.StateRunning, Running: true}
		d.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *fakeDocker) ContainerStop(ctx context.Context, _ string, _ container.StopOptions) error {
	d.mu.Lock()
	stopEntered, allowStop := d.stopEntered, d.allowStop
	d.stops++
	delay := d.stopDelay
	d.mu.Unlock()
	if stopEntered != nil {
		stopEntered <- struct{}{}
		select {
		case <-allowStop:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	d.mu.Lock()
	d.state = container.State{Status: container.StateExited}
	d.mu.Unlock()
	select {
	case d.stopped <- struct{}{}:
	default:
	}
	return nil
}

func (*fakeDocker) Ping(context.Context) (types.Ping, error) { return types.Ping{}, nil }
func (d *fakeDocker) Close() error {
	d.mu.Lock()
	d.closes++
	d.mu.Unlock()
	return nil
}
func (*fakeDocker) DaemonHost() string { return "test://docker" }

var _ dockerAPI = (*fakeDocker)(nil)
