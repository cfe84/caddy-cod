package containerproxy

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/errdefs"
	"go.uber.org/zap"
)

const (
	startupLimit   = 2 * time.Minute
	probeInterval  = 250 * time.Millisecond
	stopGrace      = 10 * time.Second
	stopAPITimeout = 15 * time.Second
	stopRetryDelay = time.Minute
)

var cleanupLimit = 30 * time.Second

type managerState uint8

const (
	managerStopped managerState = iota
	managerRunning
	managerStarting
	managerReady
	managerStopping
	managerUnavailable
)

type lifecyclePolicy struct {
	idleTimeout  time.Duration
	startupDelay time.Duration
	upstream     string
	healthURL    string
}

type managerOperation struct {
	done  chan struct{}
	err   error
	stop  bool
	state managerState
}

type manager struct {
	mu           sync.Mutex
	docker       dockerAPI
	clientID     string
	containerRef string
	resolvedID   string
	policy       lifecyclePolicy
	logger       *zap.Logger
	ctx          context.Context
	cancel       context.CancelFunc

	state        managerState
	active       int
	activeZero   chan struct{}
	idleSince    time.Time
	idleTimer    *time.Timer
	retryTimer   *time.Timer
	operation    *managerOperation
	attemptStop  context.CancelFunc
	generation   uint64
	retired      bool
	activeOwners int
	wasActivated bool

	probeClient *http.Client
}

func newManager(docker dockerAPI, id string, policy lifecyclePolicy, state managerState, logger *zap.Logger) *manager {
	ctx, cancel := context.WithCancel(context.Background())
	m := &manager{
		docker:       docker,
		clientID:     id,
		containerRef: id,
		resolvedID:   id,
		policy:       policy,
		logger:       logger,
		ctx:          ctx,
		cancel:       cancel,
		state:        state,
		activeZero:   closedSignal(),
		probeClient:  &http.Client{Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}
	return m
}

func closedSignal() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func policyFor(h *Handler) (lifecyclePolicy, error) {
	upstream, err := url.Parse(h.Upstream)
	if err != nil {
		return lifecyclePolicy{}, err
	}
	policy := lifecyclePolicy{
		idleTimeout:  time.Duration(h.IdleTimeout),
		startupDelay: time.Duration(h.StartupDelay),
		upstream:     upstream.Scheme + "://" + upstream.Host,
	}
	if h.HealthEndpoint != "" {
		policy.healthURL = policy.upstream + h.HealthEndpoint
	}
	return policy, nil
}

func (m *manager) checkPolicy(policy lifecyclePolicy) error {
	if m.policy == policy {
		return nil
	}
	return fmt.Errorf("container %s already has a lifecycle manager with a different idle timeout, startup delay, upstream, or health endpoint", m.clientID)
}

func (m *manager) checkRegistration(policy lifecyclePolicy, reference string) error {
	if m.containerRef != reference {
		return errors.New("routes for the same container must not mix name-based and ID-pinned references")
	}
	return m.checkPolicy(policy)
}

func (m *manager) currentID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.resolvedID
}

func (m *manager) activate() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.retired {
		return errors.New("container lifecycle manager is retired")
	}
	m.activeOwners++
	m.wasActivated = true
	if m.state == managerRunning || m.state == managerReady || m.state == managerUnavailable {
		m.scheduleIdleLocked(time.Now())
	}
	return nil
}

func (m *manager) deactivate() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.activeOwners == 0 {
		return
	}
	m.activeOwners--
	if m.activeOwners == 0 {
		m.stopIdleTimerLocked()
		m.stopRetryTimerLocked()
	}
}

func (m *manager) rollbackActivation() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.activeOwners == 0 {
		return
	}
	m.activeOwners--
	if m.activeOwners == 0 {
		m.wasActivated = false
		m.stopIdleTimerLocked()
		m.stopRetryTimerLocked()
	}
}

func (m *manager) Acquire(ctx context.Context) (bool, error) {
	m.mu.Lock()
	if m.retired {
		m.mu.Unlock()
		return false, errors.New("container lifecycle manager is retired")
	}
	if m.active == 0 {
		m.activeZero = make(chan struct{})
	}
	m.active++
	m.stopIdleTimerLocked()
	m.stopRetryTimerLocked()
	cold := m.state != managerReady
	generation := m.generation
	m.mu.Unlock()

	if err := m.waitUntilReady(ctx); err != nil {
		m.Release()
		return cold, err
	}
	m.mu.Lock()
	cold = cold || generation != m.generation
	m.mu.Unlock()
	return cold, nil
}

func (m *manager) Release() {
	m.mu.Lock()
	if m.active == 0 {
		m.mu.Unlock()
		return
	}
	m.active--
	if m.active == 0 {
		close(m.activeZero)
		if m.state == managerStarting && m.attemptStop != nil {
			m.attemptStop()
		}
		if m.state == managerReady || m.state == managerRunning || m.state == managerUnavailable {
			m.scheduleIdleLocked(time.Now())
		}
	}
	m.mu.Unlock()
}

func (m *manager) waitUntilReady(ctx context.Context) error {
	for {
		m.mu.Lock()
		if m.state == managerReady {
			id := m.resolvedID
			generation := m.generation
			m.mu.Unlock()
			info, err := inspectContainer(ctx, m.docker, m.containerRef)
			if err != nil && !errdefs.IsNotFound(err) {
				return err
			}
			if err == nil && info.ID == id && info.State.Running && !info.State.Paused && !info.State.Restarting {
				m.mu.Lock()
				ready := m.generation == generation && m.state == managerReady
				m.mu.Unlock()
				if ready {
					return nil
				}
				continue
			}
			m.mu.Lock()
			if m.generation == generation && m.state == managerReady {
				m.state = managerUnavailable
			}
			m.mu.Unlock()
			continue
		}
		if m.retired {
			m.mu.Unlock()
			return errors.New("container lifecycle manager is shutting down")
		}
		if m.state == managerStarting || m.state == managerStopping {
			operation := m.operation
			m.mu.Unlock()
			select {
			case <-operation.done:
				if operation.err != nil && !(operation.stop && (operation.state == managerRunning || operation.state == managerReady)) {
					return operation.err
				}
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		m.generation++
		generation := m.generation
		attemptCtx, attemptCancel := context.WithTimeout(m.ctx, startupLimit)
		m.attemptStop = attemptCancel
		operation := &managerOperation{done: make(chan struct{})}
		m.operation = operation
		m.state = managerStarting
		m.mu.Unlock()

		go m.start(generation, attemptCtx, attemptCancel)
		select {
		case <-operation.done:
			if operation.err != nil {
				return operation.err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (m *manager) start(generation uint64, ctx context.Context, cancel context.CancelFunc) {
	state, err := m.startContainer(ctx)
	for errdefs.IsNotFound(err) && m.containerRef != m.clientID && ctx.Err() == nil {
		if err = sleepContext(ctx, probeInterval); err != nil {
			break
		}
		state, err = m.startContainer(ctx)
	}
	if err != nil && errors.Is(ctx.Err(), context.Canceled) {
		state = m.reconcileState()
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) && err == nil {
		err = context.DeadlineExceeded
	}
	cancel()

	m.mu.Lock()
	if generation != m.generation {
		m.mu.Unlock()
		return
	}
	m.state = state
	m.operation.err = err
	m.operation.state = state
	m.attemptStop = nil
	close(m.operation.done)
	if m.active == 0 && (state == managerRunning || state == managerReady) {
		m.scheduleIdleLocked(time.Now())
	}
	m.mu.Unlock()
	if err != nil {
		m.logger.Warn("container startup or readiness failed", zap.String("container_id", m.clientID), zap.Error(err))
	}
}

func (m *manager) startContainer(ctx context.Context) (managerState, error) {
	info, err := inspectContainer(ctx, m.docker, m.containerRef)
	if err != nil {
		return managerUnavailable, err
	}
	id := info.ID
	m.mu.Lock()
	m.resolvedID = id
	m.mu.Unlock()
	state := info.State
	switch {
	case state.Paused || state.Status == container.StatePaused:
		return managerUnavailable, errors.New("container is paused")
	case state.Dead || state.Status == container.StateDead:
		return managerUnavailable, errors.New("container is dead")
	case state.Status == container.StateRemoving:
		return managerUnavailable, errors.New("container is being removed")
	case state.Restarting || state.Status == container.StateRestarting:
		if err := m.waitForRunning(ctx, id); err != nil {
			return managerUnavailable, err
		}
	case state.Running || state.Status == container.StateRunning:
		// Adopt an already-running container; only its readiness is evaluated.
	case state.Status == container.StateCreated || state.Status == container.StateExited:
		if err := m.docker.ContainerStart(ctx, id, container.StartOptions{}); err != nil {
			inspectCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			info, inspectErr := inspectContainer(inspectCtx, m.docker, id)
			cancel()
			if inspectErr != nil || info.State == nil || (!info.State.Running && info.State.Status != container.StateRunning) {
				return managerUnavailable, fmt.Errorf("starting container: %w", err)
			}
		} else if err := m.waitForRunning(ctx, id); err != nil {
			return managerUnavailable, err
		}
	default:
		return managerUnavailable, fmt.Errorf("unsupported Docker container state %q", state.Status)
	}

	if err := sleepContext(ctx, m.policy.startupDelay); err != nil {
		return managerRunning, err
	}
	if m.policy.healthURL != "" {
		if err := m.waitForHealth(ctx); err != nil {
			return managerRunning, err
		}
	}
	return managerReady, nil
}

func (m *manager) waitForRunning(ctx context.Context, id string) error {
	ticker := time.NewTicker(probeInterval)
	defer ticker.Stop()
	for {
		info, err := inspectContainer(ctx, m.docker, id)
		if err != nil {
			return err
		}
		if info.State.Paused || info.State.Dead || info.State.Status == container.StateRemoving {
			return fmt.Errorf("container entered unsupported state %q", info.State.Status)
		}
		if info.State.Running || info.State.Status == container.StateRunning {
			return nil
		}
		if info.State.Status != container.StateRestarting && info.State.Status != container.StateCreated {
			return fmt.Errorf("container failed to reach running state; current state is %q", info.State.Status)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (m *manager) waitForHealth(ctx context.Context) error {
	ticker := time.NewTicker(probeInterval)
	defer ticker.Stop()
	for {
		ready, err := m.probe(ctx)
		if err == nil && ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("readiness deadline exceeded: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (m *manager) probe(ctx context.Context) (bool, error) {
	probeCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(probeCtx, http.MethodGet, m.policy.healthURL, nil)
	if err != nil {
		return false, err
	}
	response, err := m.probeClient.Do(request)
	if err != nil {
		return false, err
	}
	defer response.Body.Close()
	return response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices, nil
}

func (m *manager) reconcileState() managerState {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	info, err := inspectContainer(ctx, m.docker, m.currentID())
	if err != nil {
		m.logger.Error("failed to reconcile container state after startup cancellation", zap.String("container_id", m.clientID), zap.Error(err))
		return managerUnavailable
	}
	state, err := initialManagerState(info.State)
	if err != nil {
		m.logger.Error("container is in unsupported state after startup cancellation", zap.String("container_id", m.clientID), zap.Error(err))
		return managerUnavailable
	}
	return state
}

func (m *manager) markUnready() {
	m.mu.Lock()
	if m.state == managerReady {
		m.state = managerUnavailable
	}
	m.mu.Unlock()
}

func (m *manager) scheduleIdleLocked(now time.Time) {
	if m.retired || m.activeOwners == 0 || m.active != 0 {
		return
	}
	m.stopIdleTimerLocked()
	m.idleSince = now
	m.idleTimer = time.AfterFunc(m.policy.idleTimeout, m.stopWhenIdle)
}

func (m *manager) stopIdleTimerLocked() {
	if m.idleTimer != nil {
		m.idleTimer.Stop()
		m.idleTimer = nil
	}
}

func (m *manager) stopRetryTimerLocked() {
	if m.retryTimer != nil {
		m.retryTimer.Stop()
		m.retryTimer = nil
	}
}

func (m *manager) stopWhenIdle() {
	m.mu.Lock()
	if m.retired || m.activeOwners == 0 || m.active != 0 || (m.state != managerReady && m.state != managerRunning && m.state != managerUnavailable) {
		m.mu.Unlock()
		return
	}
	remaining := m.policy.idleTimeout - time.Since(m.idleSince)
	if remaining > 0 {
		m.idleTimer = time.AfterFunc(remaining, m.stopWhenIdle)
		m.mu.Unlock()
		return
	}
	m.generation++
	generation := m.generation
	operation := &managerOperation{done: make(chan struct{}), stop: true}
	m.operation = operation
	m.state = managerStopping
	m.mu.Unlock()

	err, newState := m.stopContainer(m.ctx)
	m.mu.Lock()
	logFailure := false
	if generation == m.generation {
		m.state = newState
		operation.err = err
		operation.state = newState
		close(operation.done)
		if err != nil {
			logFailure = true
			if m.activeOwners > 0 && m.active == 0 && !m.retired {
				m.retryTimer = time.AfterFunc(stopRetryDelay, m.stopWhenIdle)
			}
		} else if m.active == 0 && !m.retired && (newState == managerRunning || newState == managerReady || newState == managerUnavailable) {
			m.scheduleIdleLocked(time.Now())
		}
	}
	m.mu.Unlock()
	if logFailure {
		m.logger.Error("failed to stop idle container", zap.String("container_id", m.clientID), zap.Error(err))
	}
}

func (m *manager) stopContainer(parent context.Context) (error, managerState) {
	id := m.currentID()
	inspectCtx, inspectCancel := context.WithTimeout(parent, 5*time.Second)
	info, err := inspectContainer(inspectCtx, m.docker, id)
	inspectCancel()
	if errdefs.IsNotFound(err) {
		return nil, managerStopped
	}
	if err != nil {
		return err, managerUnavailable
	}
	if info.State.Paused || info.State.Dead || info.State.Status == container.StateRemoving {
		return fmt.Errorf("cannot safely stop container in state %q", info.State.Status), managerUnavailable
	}
	if !info.State.Running && info.State.Status != container.StateRunning && info.State.Status != container.StateRestarting {
		return nil, managerStopped
	}
	grace := int(stopGrace.Seconds())
	stopCtx, stopCancel := context.WithTimeout(parent, stopAPITimeout)
	err = m.docker.ContainerStop(stopCtx, id, container.StopOptions{Timeout: &grace})
	stopCancel()
	if err != nil {
		stopErr := err
		inspectCtx, inspectCancel := context.WithTimeout(parent, 5*time.Second)
		latest, inspectErr := inspectContainer(inspectCtx, m.docker, id)
		inspectCancel()
		if errdefs.IsNotFound(inspectErr) {
			return nil, managerStopped
		}
		if inspectErr == nil && latest.State != nil {
			switch latest.State.Status {
			case container.StateCreated, container.StateExited:
				return nil, managerStopped
			case container.StateDead, container.StatePaused, container.StateRemoving:
				return stopErr, managerUnavailable
			}
		}
		if inspectErr != nil {
			stopErr = errors.Join(stopErr, fmt.Errorf("reconciling container state after stop failure: %w", inspectErr))
		}
		return stopErr, managerRunning
	}
	return nil, managerStopped
}

func (m *manager) Destruct() error {
	m.mu.Lock()
	if m.retired {
		m.mu.Unlock()
		return nil
	}
	m.retired = true
	m.stopIdleTimerLocked()
	m.stopRetryTimerLocked()
	if m.attemptStop != nil {
		m.attemptStop()
	}
	activeZero := m.activeZero
	shouldStop := m.wasActivated
	var opDone <-chan struct{}
	if m.operation != nil {
		opDone = m.operation.done
	}
	inOperation := m.state == managerStarting || m.state == managerStopping
	m.mu.Unlock()
	m.cancel()
	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), cleanupLimit)
	defer cancelCleanup()

	if inOperation && opDone != nil {
		select {
		case <-opDone:
		case <-cleanupCtx.Done():
			m.logger.Error("timed out waiting for lifecycle operation during cleanup", zap.String("container_id", m.clientID))
			return errors.Join(cleanupCtx.Err(), m.closeDocker())
		}
	}
	select {
	case <-activeZero:
	case <-cleanupCtx.Done():
		m.logger.Error("leaving managed container running because requests did not drain", zap.String("container_id", m.clientID))
		return errors.Join(cleanupCtx.Err(), m.closeDocker())
	}

	var cleanupErr error
	if shouldStop {
		stopErr, _ := m.stopContainer(cleanupCtx)
		if stopErr != nil {
			m.logger.Error("failed to stop managed container during cleanup", zap.String("container_id", m.clientID), zap.Error(stopErr))
		}
		cleanupErr = stopErr
	}
	closeErr := m.closeDocker()
	return errors.Join(cleanupErr, closeErr)
}

func (m *manager) closeDocker() error {
	err := m.docker.Close()
	if err != nil {
		m.logger.Error("failed to close Docker client during cleanup", zap.String("container_id", m.clientID), zap.Error(err))
	}
	return err
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

var _ caddy.Destructor = (*manager)(nil)
