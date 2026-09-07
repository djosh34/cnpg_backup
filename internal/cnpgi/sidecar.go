// Copyright 2026 cnpg_backup contributors. All rights reserved.
// Package cnpgi implements the currently available CNPG-I wire surface. Until
// lifecycle/configuration and data features land, only Identity is advertised.
package cnpgi

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/cloudnative-pg/cnpg-i/pkg/identity"
	"github.com/djosh34/cnpg_backup/internal/recoveryguard"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Identity struct {
	identity.UnimplementedIdentityServer
	Revision string
}

func (s Identity) GetPluginMetadata(context.Context, *identity.GetPluginMetadataRequest) (*identity.GetPluginMetadataResponse, error) {
	return &identity.GetPluginMetadataResponse{
		Name: recoveryguard.PluginName, Version: "development-" + s.Revision,
		DisplayName: "CNPG Backup", Description: "CNPG backup lifecycle foundation (data services unavailable)",
		ProjectUrl: "https://github.com/djosh34/cnpg_backup", RepositoryUrl: "https://github.com/djosh34/cnpg_backup",
		License: "All rights reserved", LicenseUrl: "https://github.com/djosh34/cnpg_backup/blob/main/LICENSE", Maturity: "alpha",
	}, nil
}
func (s Identity) GetPluginCapabilities(context.Context, *identity.GetPluginCapabilitiesRequest) (*identity.GetPluginCapabilitiesResponse, error) {
	return &identity.GetPluginCapabilitiesResponse{}, nil
}
func (s Identity) Probe(context.Context, *identity.ProbeRequest) (*identity.ProbeResponse, error) {
	info, err := os.Stat(recoveryguard.HelperPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0555 {
		// CNPG v1.30 ignores ready=false; failure MUST be a gRPC error.
		return nil, status.Error(codes.Unavailable, "helper not installed")
	}
	return &identity.ProbeResponse{Ready: true}, nil
}

// Serve uses a real listener also shared by the private guard control stream.
// Stopping is uncertainty, not a clean Drain acknowledgment.
func Serve(ctx context.Context, listener net.Listener, admission *recoveryguard.Admission, revision string) error {
	server := grpc.NewServer(grpc.MaxRecvMsgSize(32<<10), grpc.MaxSendMsgSize(32<<10), grpc.MaxConcurrentStreams(16))
	identity.RegisterIdentityServer(server, Identity{Revision: revision})
	if admission != nil {
		recoveryguard.RegisterControl(server, admission)
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			if admission != nil {
				admission.Close()
			}
			server.Stop()
		case <-done:
		}
	}()
	err := server.Serve(listener)
	close(done)
	if admission != nil {
		admission.Close()
	}
	if ctx.Err() != nil || errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}

// PrepareSocket creates a private UID-owned directory below the kubelet-owned
// emptyDir root. A non-root container cannot chmod that root. Subsequent main
// and sidecar mounts select this directory with subPath after this init exits.
func PrepareSocket() error {
	path := "/cnpg-backup/socket-root/plugins"
	if err := os.Mkdir(path, 0700); err != nil && !os.IsExist(err) {
		return err
	}
	fd, err := unix.Open(path, unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return err
	}
	if int(st.Uid) != os.Geteuid() {
		return errors.New("private socket directory owner mismatch")
	}
	return unix.Fchmod(fd, 0700)
}

func RunSidecar(ctx context.Context, recovery bool, revision string) error {
	var admission *recoveryguard.Admission
	if recovery {
		config, err := recoveryguard.LoadConfig(recoveryguard.ConfigPath)
		if err != nil {
			return err
		}
		admission, err = recoveryguard.NewAdmission(config, os.Getenv("POD_UID"))
		if err != nil {
			return err
		}
		defer admission.Close()
	}
	// A stale socket may be replaced only while exclusively owning its permanent
	// local lock. Do not unlink a live incarnation's socket to steal its address.
	fd, err := unix.Open("/cnpg-backup/bin/socket.lock", unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0600)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	var st unix.Stat_t
	if err = unix.Fstat(fd, &st); err != nil {
		return err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Nlink != 1 || int(st.Uid) != os.Geteuid() || st.Mode&0077 != 0 {
		return errors.New("unsafe socket lock")
	}
	if err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return errors.New("sidecar socket already owned")
	}
	source, err := os.Executable()
	if err != nil {
		return err
	}
	if err = recoveryguard.InstallHelper(source, recoveryguard.HelperPath); err != nil {
		return err
	}
	dir := filepath.Dir(recoveryguard.SocketPath)
	if err = os.Chmod(dir, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(recoveryguard.SocketPath)
	if err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return errors.New("unexpected non-socket plugin entry")
		}
		if err = os.Remove(recoveryguard.SocketPath); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	listener, err := net.Listen("unix", recoveryguard.SocketPath)
	if err != nil {
		return err
	}
	defer listener.Close()
	if err = os.Chmod(recoveryguard.SocketPath, 0600); err != nil {
		return err
	}
	return Serve(ctx, listener, admission, revision)
}

func Probe(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	conn, err := grpc.NewClient("unix://"+recoveryguard.SocketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = identity.NewIdentityClient(conn).Probe(ctx, &identity.ProbeRequest{})
	return err
}
