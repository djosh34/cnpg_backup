// Copyright 2026 cnpg_backup contributors. All rights reserved.
package configuration

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"time"
)

// ManagerAuthorization checks each new RPC, including on an already-open TLS
// transport, against a complete current leaf/key/trust generation. A broken
// rotation or retired client CA never keeps authorizing via stale transport state.
func ManagerAuthorization(directory, clientName string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		current, err := LoadManagerTLS(directory, clientName)
		p, ok := peer.FromContext(ctx)
		if err != nil || !ok {
			return nil, status.Error(codes.Unauthenticated, "manager trust unavailable")
		}
		info, ok := p.AuthInfo.(credentials.TLSInfo)
		if !ok || VerifyClient(info.State.PeerCertificates, current.ClientCAs, clientName) != nil {
			return nil, status.Error(codes.Unauthenticated, "manager client rejected")
		}
		bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		return handler(bounded, req)
	}
}
