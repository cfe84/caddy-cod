package containerproxy

import (
	"context"
	"fmt"
	"os"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

type dockerAPI interface {
	ContainerInspect(context.Context, string) (container.InspectResponse, error)
	ContainerStart(context.Context, string, container.StartOptions) error
	ContainerStop(context.Context, string, container.StopOptions) error
	Ping(context.Context) (types.Ping, error)
	Close() error
	DaemonHost() string
}

var newDockerClient = func() (dockerAPI, error) {
	return client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
}

type managerRegistryKey struct {
	endpoint    string
	tlsVerify   string
	certificate string
	containerID string
}

func dockerEndpointKey(docker dockerAPI, containerID string) managerRegistryKey {
	return managerRegistryKey{
		endpoint:    docker.DaemonHost(),
		tlsVerify:   os.Getenv("DOCKER_TLS_VERIFY"),
		certificate: os.Getenv("DOCKER_CERT_PATH"),
		containerID: containerID,
	}
}

func inspectContainer(ctx context.Context, docker dockerAPI, id string) (container.InspectResponse, error) {
	info, err := docker.ContainerInspect(ctx, id)
	if err != nil {
		return container.InspectResponse{}, fmt.Errorf("inspecting container: %w", err)
	}
	if info.ContainerJSONBase == nil || info.State == nil || info.ID == "" {
		return container.InspectResponse{}, fmt.Errorf("Docker returned incomplete container information")
	}
	return info, nil
}

func initialManagerState(state *container.State) (managerState, error) {
	switch {
	case state.Paused || state.Status == container.StatePaused:
		return managerUnavailable, fmt.Errorf("container is paused")
	case state.Dead || state.Status == container.StateDead:
		return managerUnavailable, fmt.Errorf("container is dead")
	case state.Status == container.StateRemoving:
		return managerUnavailable, fmt.Errorf("container is being removed")
	case state.Running || state.Status == container.StateRunning:
		return managerRunning, nil
	case state.Restarting || state.Status == container.StateRestarting:
		return managerStopped, nil
	case state.Status == container.StateCreated || state.Status == container.StateExited:
		return managerStopped, nil
	default:
		return managerUnavailable, fmt.Errorf("unsupported Docker container state %q", state.Status)
	}
}
