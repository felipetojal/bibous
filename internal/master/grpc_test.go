package master

import (
	"context"
	"net"
	"testing"
	"time"

	pb "github.com/felipetojal/bibous/proto/masterpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// TestMasterRPC verifica o roteamento real do gRPC e a serialização dos dados.
// O teste usa um listener em memória, sem depender de portas do sistema.
func TestMasterRPC(t *testing.T) {
	mestre, _, _ := testSetup(t)
	servico := NewMasterService(mestre)
	const tamanhoBuffer = 1024 * 1024
	listener := bufconn.Listen(tamanhoBuffer)
	servidor := grpc.NewServer()
	pb.RegisterMasterServiceServer(servidor, servico)
	go func() { _ = servidor.Serve(listener) }()
	defer servidor.Stop()

	conexao, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conexao.Close()
	cliente := pb.NewMasterServiceClient(conexao)
	ctx, cancelar := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelar()

	// Registra um sexto nó por meio da pilha real de transporte gRPC.
	registro, err := cliente.RegisterNode(ctx, &pb.RegisterRequest{
		NodeId: "node-6", NodeAddress: "node-6:9000", MachineId: "host-6",
		DiskId: "disk-1", AvailableSize: 1000, TotalSize: 1000,
	})
	if err != nil || !registro.GetSuccess() {
		t.Fatalf("registro: resposta=%v erro=%v", registro, err)
	}

	// A presença opcional permite informar que o espaço disponível é zero.
	livre := uint64(0)
	resposta, err := cliente.Heartbeat(ctx, &pb.HeartbeatRequest{
		NodeId: "node-6", Nonce: 42, AvailableSize: &livre,
	})
	if err != nil || resposta.GetNonce() != 42 {
		t.Fatalf("heartbeat: resposta=%v erro=%v", resposta, err)
	}
	no, existe := mestre.nr.Get("node-6")
	if !existe || no.AvailableSize != 0 {
		t.Fatal("o heartbeat não atualizou a capacidade do nó")
	}

	// Consulta o resumo do cluster usando a interface remota pública.
	resumo, err := cliente.GetClusterStatus(ctx, &pb.ClusterStatusRequest{})
	if err != nil || len(resumo.GetNodes()) != 6 {
		t.Fatalf("consulta do cluster: resumo=%v erro=%v", resumo, err)
	}
}
