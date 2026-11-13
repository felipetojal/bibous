package master

import (
	"errors"
	"fmt"
	"net"
	"time"

	grpc "google.golang.org/grpc"
)

// RPCServer encapsula os dados para um servidor
// RPC.
type RPCServer struct {
	listenAddr string
	listenPort string
	grpc       *grpc.Server
}

var (
	ErrorCreatingListener = errors.New("unable to create listener for RPCServer")
	ErrorServingRequests  = errors.New("unable to server requests with listener.")
)

// NewRPCServer é o construtor de RPCServer.
func NewRPCServer(listenAddr, listenPort string) *RPCServer {
	grpc := grpc.NewServer()
	return &RPCServer{
		listenAddr: listenAddr,
		listenPort: listenPort,
		grpc:       grpc,
	}
}

// Start inicializa o servidor RPC.
func (rpc *RPCServer) Start() error {
	address := net.JoinHostPort(rpc.listenAddr, rpc.listenPort)

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("%v | %w: %w", time.Now(), ErrorCreatingListener, err)
	}

	fmt.Printf("%v | Master listening on address %s:%s", time.Now(), rpc.listenAddr, rpc.listenPort)

	if err := rpc.grpc.Serve(listener); err != nil {
		return fmt.Errorf("%v | %w: %w", time.Now(), ErrorServingRequests, err)
	}

	return nil
}

// Stop é responsável por desligar o servidor.
func (rpc *RPCServer) Stop() {
	rpc.grpc.GracefulStop()
}