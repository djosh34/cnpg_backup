// Copyright 2026 cnpg_backup contributors. All rights reserved.
package cnpgi

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"time"

	"github.com/cloudnative-pg/cnpg-i/pkg/identity"
	"github.com/cloudnative-pg/cnpg-i/pkg/lifecycle"
	"github.com/cloudnative-pg/cnpg-i/pkg/operator"
	"github.com/djosh34/cnpg_backup/internal/configuration"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

const managerTLSPath = "/cnpg-backup/tls"

type ManagerConfig struct {
	Namespaces  []string            `json:"namespaces"`
	SecretNames map[string][]string `json:"secretNames"`
	Image       string              `json:"image"`
	ClientName  string              `json:"clientName"`
}
type managerIdentity struct {
	Identity
	clientName string
}

func (s managerIdentity) GetPluginCapabilities(context.Context, *identity.GetPluginCapabilitiesRequest) (*identity.GetPluginCapabilitiesResponse, error) {
	result := &identity.GetPluginCapabilitiesResponse{}
	for _, kind := range []identity.PluginCapability_Service_Type{identity.PluginCapability_Service_TYPE_OPERATOR_SERVICE, identity.PluginCapability_Service_TYPE_LIFECYCLE_SERVICE, identity.PluginCapability_Service_TYPE_INSTANCE_SIDECAR_INJECTION, identity.PluginCapability_Service_TYPE_INSTANCE_JOB_SIDECAR_INJECTION} {
		result.Capabilities = append(result.Capabilities, &identity.PluginCapability{Type: &identity.PluginCapability_Service_{Service: &identity.PluginCapability_Service{Type: kind}}})
	}
	return result, nil
}
func (s managerIdentity) Probe(context.Context, *identity.ProbeRequest) (*identity.ProbeResponse, error) {
	if _, err := configuration.LoadManagerTLS(managerTLSPath, s.clientName); err != nil {
		return nil, status.Error(codes.Unavailable, "manager TLS projection invalid")
	}
	return &identity.ProbeResponse{Ready: true}, nil
}

type Operator struct {
	operator.UnimplementedOperatorServer
	API *API
}

func (s Operator) GetCapabilities(context.Context, *operator.OperatorCapabilitiesRequest) (*operator.OperatorCapabilitiesResult, error) {
	result := &operator.OperatorCapabilitiesResult{}
	for _, kind := range []operator.OperatorCapability_RPC_Type{operator.OperatorCapability_RPC_TYPE_VALIDATE_CLUSTER_CREATE, operator.OperatorCapability_RPC_TYPE_VALIDATE_CLUSTER_CHANGE} {
		result.Capabilities = append(result.Capabilities, &operator.OperatorCapability{Type: &operator.OperatorCapability_Rpc{Rpc: &operator.OperatorCapability_RPC{Type: kind}}})
	}
	return result, nil
}
func validationError(err error) []*operator.ValidationError {
	if err == nil {
		return nil
	}
	return []*operator.ValidationError{{PathComponents: []string{"spec", "plugins"}, Message: err.Error()}}
}
func (s Operator) ValidateClusterCreate(ctx context.Context, req *operator.OperatorValidateClusterCreateRequest) (*operator.OperatorValidateClusterCreateResult, error) {
	c, err := ParseCluster(req.Definition)
	if err == nil {
		_, _, err = s.API.ValidateRepositories(ctx, c)
	}
	return &operator.OperatorValidateClusterCreateResult{ValidationErrors: validationError(err)}, nil
}
func (s Operator) ValidateClusterChange(ctx context.Context, req *operator.OperatorValidateClusterChangeRequest) (*operator.OperatorValidateClusterChangeResult, error) {
	old, err := ParseCluster(req.OldCluster)
	var next Cluster
	if err == nil {
		next, err = ParseCluster(req.NewCluster)
	}
	if err == nil && (!reflect.DeepEqual(old.Spec.Bootstrap, next.Spec.Bootstrap) || old.Metadata.UID != next.Metadata.UID) {
		err = errors.New("bootstrap and Cluster identity are immutable")
	}
	if err == nil {
		_, _, err = s.API.ValidateRepositories(ctx, next)
	}
	return &operator.OperatorValidateClusterChangeResult{ValidationErrors: validationError(err)}, nil
}

type Lifecycle struct {
	lifecycle.UnimplementedOperatorLifecycleServer
	API   *API
	Image string
}

func (s Lifecycle) GetCapabilities(context.Context, *lifecycle.OperatorLifecycleCapabilitiesRequest) (*lifecycle.OperatorLifecycleCapabilitiesResponse, error) {
	ops := func(kinds ...lifecycle.OperatorOperationType_Type) []*lifecycle.OperatorOperationType {
		result := []*lifecycle.OperatorOperationType{}
		for _, kind := range kinds {
			result = append(result, &lifecycle.OperatorOperationType{Type: kind})
		}
		return result
	}
	return &lifecycle.OperatorLifecycleCapabilitiesResponse{LifecycleCapabilities: []*lifecycle.OperatorLifecycleCapabilities{
		{Group: "", Kind: "Pod", OperationTypes: ops(lifecycle.OperatorOperationType_TYPE_CREATE, lifecycle.OperatorOperationType_TYPE_EVALUATE, lifecycle.OperatorOperationType_TYPE_UPDATE, lifecycle.OperatorOperationType_TYPE_PATCH)},
		{Group: "batch", Kind: "Job", OperationTypes: ops(lifecycle.OperatorOperationType_TYPE_CREATE)},
	}}, nil
}
func (s Lifecycle) LifecycleHook(ctx context.Context, req *lifecycle.OperatorLifecycleRequest) (*lifecycle.OperatorLifecycleResponse, error) {
	if req.OperationType == nil {
		return nil, status.Error(codes.InvalidArgument, "operation type required")
	}
	switch req.OperationType.Type {
	case lifecycle.OperatorOperationType_TYPE_CREATE, lifecycle.OperatorOperationType_TYPE_EVALUATE, lifecycle.OperatorOperationType_TYPE_UPDATE, lifecycle.OperatorOperationType_TYPE_PATCH:
	default:
		return nil, status.Error(codes.Unimplemented, "lifecycle operation unsupported")
	}
	c, err := ParseCluster(req.ClusterDefinition)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Cluster")
	}
	p, err := Place(ctx, s.API, c, req.ObjectDefinition, s.Image)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &lifecycle.OperatorLifecycleResponse{JsonPatch: p}, nil
}
func RunManager(ctx context.Context, revision string) error {
	b, err := os.ReadFile("/cnpg-backup/manager/config.json")
	if err != nil {
		return err
	}
	var config ManagerConfig
	if err := configuration.StrictJSON(b, &config); err != nil {
		return err
	}
	if len(config.Namespaces) == 0 || len(config.Namespaces) > 32 || config.ClientName == "" || !regexp.MustCompile(`^[a-zA-Z0-9./:_-]+@sha256:[0-9a-f]{64}$`).MatchString(config.Image) {
		return errors.New("invalid manager configuration")
	}
	if _, err := configuration.LoadManagerTLS(managerTLSPath, config.ClientName); err != nil {
		return err
	}
	kube, err := rest.InClusterConfig()
	if err != nil {
		return err
	}
	kube.QPS = 5
	kube.Burst = 10
	kube.Timeout = 10 * time.Second
	httpClient, err := rest.HTTPClientFor(kube)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, kube.Host+"/version", nil)
	if err != nil {
		return err
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return err
	}
	var version struct {
		GitVersion string `json:"gitVersion"`
	}
	err = json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&version)
	response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK || version.GitVersion != "v1.35.8" {
		return errors.New("unsupported Kubernetes version; require v1.35.8")
	}
	client, err := dynamic.NewForConfig(kube)
	if err != nil {
		return err
	}
	api := &API{Client: client, Namespaces: config.Namespaces, SecretNames: config.SecretNames}
	listener, err := net.Listen("tcp", ":9090")
	if err != nil {
		return err
	}
	defer listener.Close()
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
		return configuration.LoadManagerTLS(managerTLSPath, config.ClientName)
	}}
	// Check each NEW RPC against current trust too: a still-open transport is not
	// permission to retain stale trust indefinitely after an invalid rotation.
	authorize := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		current, err := configuration.LoadManagerTLS(managerTLSPath, config.ClientName)
		p, ok := peer.FromContext(ctx)
		if err != nil || !ok {
			return nil, status.Error(codes.Unauthenticated, "manager trust unavailable")
		}
		tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
		if !ok || configuration.VerifyClient(tlsInfo.State.PeerCertificates, current.ClientCAs, config.ClientName) != nil {
			return nil, status.Error(codes.Unauthenticated, "manager client rejected")
		}
		requestCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		return handler(requestCtx, req)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)), grpc.MaxRecvMsgSize(3<<20), grpc.MaxSendMsgSize(3<<20), grpc.MaxConcurrentStreams(16), grpc.UnaryInterceptor(authorize))
	identity.RegisterIdentityServer(server, managerIdentity{Identity: Identity{Revision: revision}, clientName: config.ClientName})
	operator.RegisterOperatorServer(server, Operator{API: api})
	lifecycle.RegisterOperatorLifecycleServer(server, Lifecycle{API: api, Image: config.Image})
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			server.Stop()
		case <-done:
		}
	}()
	err = server.Serve(listener)
	close(done)
	if ctx.Err() != nil || errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}
