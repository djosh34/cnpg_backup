package configuration

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func TestManagerReauthorizesAlreadyOpenConnection(t *testing.T) {
	old, new := newCA(t), newCA(t)
	cert, key := old.leaf(t, "manager.test", x509.ExtKeyUsageServerAuth)
	cc, ck := old.leaf(t, "client", x509.ExtKeyUsageClientAuth)
	pair, err := tls.X509KeyPair(cc, ck)
	if err != nil {
		t.Fatal(err)
	}
	roots, err := CAPool(old.pem, false)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	files := map[string][]byte{"tls.crt": cert, "tls.key": key, "client-ca.crt": old.pem}
	publish(t, dir, "first", files)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var handshakes atomic.Int32
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
		handshakes.Add(1)
		return LoadManagerTLS(dir, "client")
	}})), grpc.UnaryInterceptor(ManagerAuthorization(dir, "client")))
	healthpb.RegisterHealthServer(server, health.NewServer())
	go server.Serve(listener)
	defer server.Stop()
	client, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, ServerName: "manager.test", RootCAs: roots, Certificates: []tls.Certificate{pair}})))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	check := func(want codes.Code) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err := healthpb.NewHealthClient(client).Check(ctx, &healthpb.HealthCheckRequest{})
		if status.Code(err) != want {
			t.Fatalf("got %v want %v", err, want)
		}
	}
	check(codes.OK)
	files["client-ca.crt"] = append(append([]byte(nil), old.pem...), new.pem...)
	publish(t, dir, "overlap", files)
	check(codes.OK)
	files["client-ca.crt"] = new.pem
	publish(t, dir, "retired", files)
	check(codes.Unauthenticated)
	files["client-ca.crt"] = old.pem
	files["tls.key"] = []byte("invalid key rotation")
	publish(t, dir, "invalid", files)
	check(codes.Unauthenticated)
	if handshakes.Load() != 1 {
		t.Fatalf("test reconnected: %d handshakes", handshakes.Load())
	}
}
