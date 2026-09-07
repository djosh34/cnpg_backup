package main

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	wire "github.com/cloudnative-pg/cnpg-i/pkg/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestRelocateSocketUsesFullVolumeAliasAndPreservesListener(t *testing.T) {
	volume, err := os.MkdirTemp("", "sock-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(volume)
	if err := os.Mkdir(filepath.Join(volume, "plugins"), 0700); err != nil {
		t.Fatal(err)
	}
	original := filepath.Join(volume, "plugins", "service")
	listener, err := net.Listen("unix", original)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	// Real listener under the volume root; discovery is a separate alias. The
	// actual hosted campaign supplies the distinct subPath bind mount.
	aliasDir, err := os.MkdirTemp("", "alias-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(aliasDir)
	alias := filepath.Join(aliasDir, "service")
	if err := os.Symlink(original, alias); err != nil {
		t.Fatal(err)
	}
	if err := relocateSocket(alias, volume); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(original); !os.IsNotExist(err) {
		t.Fatal("discovery entry still exists", err)
	}
	conn, err := net.DialTimeout("unix", filepath.Join(volume, "upstream.sock"), time.Second)
	if err != nil {
		t.Fatal("renamed actual listener unavailable", err)
	}
	conn.Close()
	if err := relocateSocket(filepath.Join(volume, "upstream.sock"), volume); err == nil {
		t.Fatal("wrong alias accepted")
	}
}

// Real local socket/protobuf interoperability for the observer transport. This
// is not actual CNPG coverage and never increments the campaign family ledger.
func TestRawObserverPreservesUnaryWireAndErrorStatus(t *testing.T) {
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	server := grpc.NewServer(grpc.ForceServerCodec(rawCodec{}), grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		method, _ := grpc.MethodFromServerStream(stream)
		if method != wire.WAL_Restore_FullMethodName {
			t.Error(method)
		}
		var b []byte
		if e := stream.RecvMsg(&b); e != nil {
			return e
		}
		var req wire.WALRestoreRequest
		if e := proto.Unmarshal(b, &req); e != nil {
			t.Error(e)
		}
		if !bytes.Equal(req.ClusterDefinition, []byte(`{"kind":"Cluster"}`)) || req.Parameters["recoveryID"] != "opaque" {
			t.Error("request changed")
		}
		if req.SourceWalName == "missing" {
			return status.Error(codes.NotFound, "ordinary miss")
		}
		if req.SourceWalName == "broken" {
			return status.Error(codes.DataLoss, "required corruption")
		}
		response := []byte{}
		return stream.SendMsg(&response)
	}))
	go server.Serve(listener)
	defer server.Stop()
	conn, e := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	for _, test := range []struct {
		name string
		code codes.Code
	}{{"present", codes.OK}, {"missing", codes.NotFound}, {"broken", codes.DataLoss}} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, e := wire.NewWALClient(conn).Restore(ctx, &wire.WALRestoreRequest{ClusterDefinition: []byte(`{"kind":"Cluster"}`), Parameters: map[string]string{"recoveryID": "opaque"}, SourceWalName: test.name})
		cancel()
		if status.Code(e) != test.code {
			t.Fatalf("%s: %v", test.name, e)
		}
	}
}
