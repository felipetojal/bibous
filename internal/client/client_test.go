package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/felipetojal/bibous/internal/master"
	"github.com/felipetojal/bibous/internal/storage"
	storagepb "github.com/felipetojal/bibous/proto"
	mpb "github.com/felipetojal/bibous/proto/masterpb"

	"google.golang.org/grpc"
)

// Os testes sobem o mestre real (internal/master) e nós de storage reais
// (internal/storage) em portas locais, e usam o cliente contra eles.

type testNode struct {
	id, addr, dir string
	server        *grpc.Server
}

type cluster struct {
	nodes  []*testNode
	master *master.Master
	addr   string
}

func startNode(t *testing.T, id string) *testNode {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(storage.MaxRPCMessageBytes),
		grpc.MaxSendMsgSize(storage.MaxRPCMessageBytes),
	)
	storagepb.RegisterStorageServiceServer(srv, storage.NewRPCServer(store, id))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return &testNode{id: id, addr: lis.Addr().String(), dir: dir, server: srv}
}

func newCluster(t *testing.T, nodeCount, replicas int) *cluster {
	t.Helper()
	cfg := master.DefaultConfig()
	cfg.DefaultReplicas = replicas
	m, err := master.NewMaster(t.TempDir(), cfg, master.NewGRPCBlockTransport())
	if err != nil {
		t.Fatal(err)
	}

	c := &cluster{master: m}
	for i := 1; i <= nodeCount; i++ {
		n := startNode(t, fmt.Sprintf("storage-%d", i))
		c.nodes = append(c.nodes, n)
		err := m.RegisterNode(master.NodeInfo{
			ID: n.id, Address: n.addr, MachineID: fmt.Sprintf("maquina-%d", i), DiskID: "disco-1",
			TotalSize: 10 << 30, AvailableSize: 10 << 30,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	mpb.RegisterMasterServiceServer(srv, master.NewMasterService(m))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	c.addr = lis.Addr().String()
	return c
}

func (c *cluster) client(t *testing.T, mod func(*Config)) *Client {
	t.Helper()
	cfg := Config{MasterAddr: c.addr, BlockSize: 1000, Parallelism: 3, RPCTimeout: 5 * time.Second}
	if mod != nil {
		mod(&cfg)
	}
	cl, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cl.Close() })
	return cl
}

func writeRandom(t *testing.T, size int) (string, []byte) {
	t.Helper()
	data := make([]byte, size)
	rand.Read(data)
	p := filepath.Join(t.TempDir(), "in.bin")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return p, data
}

func roundTrip(t *testing.T, cl *Client, remote string, data []byte) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "out.bin")
	if _, err := cl.Download(context.Background(), remote, out); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(out)
	if !bytes.Equal(got, data) {
		t.Fatalf("%s: conteúdo baixado difere do original", remote)
	}
}

func TestRoundTripSizes(t *testing.T) {
	c := newCluster(t, 3, 3)
	cl := c.client(t, nil)
	ctx := context.Background()

	for _, size := range []int{0, 1, 1000, 1001, 5555} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			local, data := writeRandom(t, size)
			remote := fmt.Sprintf("/f%d.bin", size)
			info, err := cl.Upload(ctx, local, remote, UploadOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if info.GetStatus() != "HEALTHY" || info.GetSize() != uint64(size) {
				t.Fatalf("info = %v", info)
			}
			for _, b := range info.GetBlocks() {
				if len(b.GetReplicas()) != 3 {
					t.Fatalf("bloco com %d réplicas", len(b.GetReplicas()))
				}
			}
			roundTrip(t, cl, remote, data)
		})
	}
}

func TestFullSizeBlocks(t *testing.T) {
	c := newCluster(t, 3, 2)
	cl := c.client(t, func(cfg *Config) { cfg.BlockSize = 0 })
	local, data := writeRandom(t, 2*MaxBlockSize+123)
	if _, err := cl.Upload(context.Background(), local, "/grande", UploadOptions{}); err != nil {
		t.Fatal(err)
	}
	roundTrip(t, cl, "/grande", data)
}

func TestEncryption(t *testing.T) {
	c := newCluster(t, 3, 2)
	key := []byte("0123456789abcdef0123456789abcdef")
	cl := c.client(t, func(cfg *Config) { cfg.EncryptionKey = key; cfg.BlockSize = 0 })
	local, data := writeRandom(t, MaxBlockSize+777)

	if _, err := cl.Upload(context.Background(), local, "/s", UploadOptions{}); err != nil {
		t.Fatal(err)
	}
	roundTrip(t, cl, "/s", data)

	// Os nós guardam só texto cifrado.
	for _, n := range c.nodes {
		files, _ := filepath.Glob(filepath.Join(n.dir, "*.blk"))
		for _, f := range files {
			raw, _ := os.ReadFile(f)
			if len(raw) > 64 && bytes.Contains(data, raw[gcmOverhead:gcmOverhead+32]) {
				t.Fatalf("%s contém texto claro", f)
			}
		}
	}

	wrong := c.client(t, func(cfg *Config) { cfg.EncryptionKey = []byte("XXXXXXXXXXabcdef0123456789abcdef") })
	if _, err := wrong.Download(context.Background(), "/s", filepath.Join(t.TempDir(), "x")); err == nil {
		t.Fatal("download com chave errada deveria falhar")
	}
}

func TestDownloadFallbackAndCorruption(t *testing.T) {
	c := newCluster(t, 3, 3)
	cl := c.client(t, nil)
	ctx := context.Background()

	local, data := writeRandom(t, 2500)
	info, err := cl.Upload(ctx, local, "/x", UploadOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Corrompe a cópia do bloco 0 no primeiro nó que o cliente vai tentar.
	b0 := info.GetBlocks()[0]
	first := b0.GetReplicas()[0].GetNodeId()
	for _, n := range c.nodes {
		if n.id == first {
			p := filepath.Join(n.dir, fmt.Sprintf("%s-v%d.blk", b0.GetBlockId(), b0.GetVersion()))
			if err := os.WriteFile(p, bytes.Repeat([]byte("x"), int(b0.GetSize())), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	roundTrip(t, cl, "/x", data)

	// A corrupção foi reportada: o mestre tirou a réplica ruim.
	st, _ := cl.Stat(ctx, "/x")
	for _, r := range st.GetBlocks()[0].GetReplicas() {
		if r.GetNodeId() == first {
			t.Fatal("réplica corrompida continua listada no mestre")
		}
	}

	// Com dois nós fora, cada bloco ainda tem uma réplica viva.
	c.nodes[0].server.Stop()
	c.nodes[1].server.Stop()
	roundTrip(t, cl, "/x", data)
}

func TestUploadFailsAndCleansUp(t *testing.T) {
	c := newCluster(t, 3, 3)
	c.nodes[2].server.Stop() // o mestre ainda acha que está vivo
	cl := c.client(t, func(cfg *Config) { cfg.Retries = 0 })

	local, _ := writeRandom(t, 1500)
	if _, err := cl.Upload(context.Background(), local, "/falha", UploadOptions{}); err == nil {
		t.Fatal("upload deveria falhar com um nó fora")
	}
	if _, err := cl.Stat(context.Background(), "/falha"); !IsNotFound(err) {
		t.Fatalf("upload falho não pode aparecer no mestre: %v", err)
	}
	// O upload foi abortado: dá para tentar de novo o mesmo caminho.
	_, _, _, _, pending := c.master.ClusterStatus()
	if pending != 0 {
		t.Fatalf("%d uploads pendentes, esperado 0", pending)
	}
}

func TestNamespace(t *testing.T) {
	c := newCluster(t, 3, 2)
	cl := c.client(t, nil)
	ctx := context.Background()
	local, data := writeRandom(t, 2048)
	other, otherData := writeRandom(t, 10)

	if _, err := cl.Upload(ctx, local, "/a/b/x", UploadOptions{}); err == nil {
		t.Fatal("put num diretório inexistente deveria falhar")
	}
	if err := cl.MakeDir(ctx, "/a/b", false); err == nil {
		t.Fatal("mkdir sem -p deveria falhar sem o pai")
	}
	if err := cl.MakeDir(ctx, "/a/b", true); err != nil {
		t.Fatal(err)
	}
	if err := cl.MakeDir(ctx, "/a/b", true); err != nil {
		t.Fatalf("mkdir -p num diretório existente: %v", err)
	}
	if _, err := cl.Upload(ctx, local, "/a/b/x", UploadOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Upload(ctx, other, "/a/b/x", UploadOptions{}); err == nil {
		t.Fatal("put sem -f num arquivo existente deveria falhar")
	}
	if _, err := cl.Upload(ctx, other, "/a/b/x", UploadOptions{Overwrite: true}); err != nil {
		t.Fatal(err)
	}
	roundTrip(t, cl, "/a/b/x", otherData)

	// mv para dentro de um diretório e renomeação.
	if err := cl.Move(ctx, "/a/b/x", "/a", false); err != nil {
		t.Fatal(err)
	}
	if err := cl.Move(ctx, "/a/x", "/a/y", false); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Upload(ctx, local, "/a/z", UploadOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := cl.Move(ctx, "/a/z", "/a/y", false); err == nil {
		t.Fatal("mv sem -f sobre arquivo existente deveria falhar")
	}
	if err := cl.Move(ctx, "/a/z", "/a/y", true); err != nil {
		t.Fatal(err)
	}
	roundTrip(t, cl, "/a/y", data)

	entries, err := cl.List(ctx, "/a")
	if err != nil || len(entries) != 2 || !entries[0].IsDir || entries[1].Path != "/a/y" ||
		entries[1].File.GetStatus() != "HEALTHY" {
		t.Fatalf("ls /a = %+v, %v", entries, err)
	}

	if err := cl.RemoveDir(ctx, "/a", false); err == nil {
		t.Fatal("rmdir sem -r deveria falhar com diretório não vazio")
	}
	if err := cl.RemoveDir(ctx, "/a", true); err != nil {
		t.Fatal(err)
	}
	if entries, _ := cl.List(ctx, "/"); len(entries) != 0 {
		t.Fatalf("raiz deveria estar vazia: %+v", entries)
	}

	// rm apagou os blocos dos nós, não só os metadados.
	for _, n := range c.nodes {
		if files, _ := filepath.Glob(filepath.Join(n.dir, "*.blk")); len(files) != 0 {
			t.Fatalf("%s ainda tem blocos: %v", n.id, files)
		}
	}
}
