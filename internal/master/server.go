package master

import (
	"errors"
	"fmt"
	"net"

	pb "github.com/felipetojal/bibous/proto/masterpb"
	"google.golang.org/grpc"
)

// RPCServer administra apenas o transporte gRPC, não os dados do domínio.
type RPCServer struct {
	listenAddr string
	listenPort string
	grpc       *grpc.Server
}

// NewRPCServer registra o contrato do mestre na instância gRPC.
func NewRPCServer(listenAddr, listenPort string, service pb.MasterServiceServer) *RPCServer {
	server := grpc.NewServer()
	pb.RegisterMasterServiceServer(server, service)
	return &RPCServer{listenAddr: listenAddr, listenPort: listenPort, grpc: server}
}

// Start abre a porta TCP e bloqueia enquanto atende os RPCs.
func (r *RPCServer) Start() error {
	addr := net.JoinHostPort(r.listenAddr, r.listenPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("master listen %s: %w", addr, err)
	}
	if err = r.grpc.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("master serve: %w", err)
	}
	return nil
}

// Stop inicia encerramento gracioso, aguardando chamadas em curso.
func (r *RPCServer) Stop() { r.grpc.GracefulStop() }

// ForceStop interrompe imediatamente as conexões gRPC.
func (r *RPCServer) ForceStop() { r.grpc.Stop() }
