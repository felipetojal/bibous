package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/felipetojal/bibous/internal/storage"
	pb "github.com/felipetojal/bibous/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ---------------------------------------------------------------------------
// Nós de storage reais (internal/storage) rodando em portas locais.

type testNode struct {
	id     string
	addr   string
	server *grpc.Server
}

func startNode(t *testing.T, id string) *testNode {
	t.Helper()

	store, err := storage.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(storage.MaxRPCMessageBytes),
		grpc.MaxSendMsgSize(storage.MaxRPCMessageBytes),
	)
	pb.RegisterStorageServiceServer(srv, storage.NewRPCServer(store, id))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	return &testNode{id: id, addr: lis.Addr().String(), server: srv}
}

// ---------------------------------------------------------------------------
// Mestre falso, em memória, que implementa o contrato de client.proto.

type fakeFile struct {
	info   *pb.FileInfo
	blocks []*pb.BlockLocation
}

type fakeMaster struct {
	pb.UnimplementedClientServiceServer

	mu         sync.Mutex
	nodes      []*pb.NodeLocation
	defaultRF  int
	nextID     int
	pending    map[string]*fakeFile
	files      map[string]*fakeFile
	dirs       map[string]bool
	lastCommit *pb.CommitUploadRequest
}

func startMaster(t *testing.T, nodes []*testNode, defaultRF int) (*fakeMaster, string) {
	t.Helper()

	m := &fakeMaster{
		defaultRF: defaultRF,
		pending:   map[string]*fakeFile{},
		files:     map[string]*fakeFile{},
		dirs:      map[string]bool{"/": true},
	}
	for _, n := range nodes {
		m.nodes = append(m.nodes, &pb.NodeLocation{NodeId: n.id, Address: n.addr})
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterClientServiceServer(srv, m)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	return m, lis.Addr().String()
}

func (m *fakeMaster) PrepareUpload(_ context.Context, req *pb.PrepareUploadRequest) (*pb.PrepareUploadResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.files[req.GetPath()]; exists && !req.GetOverwrite() {
		return nil, status.Error(codes.AlreadyExists, "arquivo já existe")
	}

	rf := int(req.GetReplicationFactor())
	if rf == 0 {
		rf = m.defaultRF
	}
	if rf > len(m.nodes) {
		rf = len(m.nodes)
	}

	m.nextID++
	uploadID := fmt.Sprintf("u%d", m.nextID)

	f := &fakeFile{info: &pb.FileInfo{
		Path:              req.GetPath(),
		Size:              req.GetSize(),
		Hash:              req.GetHash(),
		ReplicationFactor: uint32(rf),
		BlockCount:        uint32(len(req.GetBlocks())),
	}}
	resp := &pb.PrepareUploadResponse{UploadId: uploadID}

	for _, spec := range req.GetBlocks() {
		var replicas []*pb.NodeLocation
		for k := 0; k < rf; k++ {
			replicas = append(replicas, m.nodes[(int(spec.GetIndex())+k)%len(m.nodes)])
		}
		loc := &pb.BlockLocation{
			Index:    spec.GetIndex(),
			BlockId:  fmt.Sprintf("%s-b%d", uploadID, spec.GetIndex()),
			Version:  1,
			Size:     spec.GetSize(),
			Hash:     spec.GetHash(),
			Replicas: replicas,
		}
		f.blocks = append(f.blocks, loc)
		resp.Blocks = append(resp.Blocks, loc)
	}

	m.pending[uploadID] = f
	return resp, nil
}

func (m *fakeMaster) CommitUpload(_ context.Context, req *pb.CommitUploadRequest) (*pb.CommitUploadResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	f, ok := m.pending[req.GetUploadId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "upload desconhecido")
	}
	if len(req.GetBlocks()) != len(f.blocks) {
		return nil, status.Error(codes.InvalidArgument, "quantidade de blocos")
	}

	byAddr := map[string]*pb.NodeLocation{}
	for _, n := range m.nodes {
		byAddr[n.GetNodeId()] = n
	}
	for i, stored := range req.GetBlocks() {
		if stored.GetBlockId() != f.blocks[i].GetBlockId() || len(stored.GetNodeIds()) == 0 {
			return nil, status.Error(codes.InvalidArgument, "bloco inválido no commit")
		}
		// Só as réplicas que confirmaram passam a valer.
		var replicas []*pb.NodeLocation
		for _, id := range stored.GetNodeIds() {
			replicas = append(replicas, byAddr[id])
		}
		f.blocks[i].Replicas = replicas
	}

	f.info.State = pb.FileState_FILE_STATE_HEALTHY
	for _, b := range f.blocks {
		if len(b.GetReplicas()) < int(f.info.GetReplicationFactor()) {
			f.info.State = pb.FileState_FILE_STATE_UNDER_REPLICATED
		}
	}

	delete(m.pending, req.GetUploadId())
	m.files[f.info.GetPath()] = f
	m.lastCommit = req
	return &pb.CommitUploadResponse{File: f.info}, nil
}

func (m *fakeMaster) GetFile(_ context.Context, req *pb.GetFileRequest) (*pb.GetFileResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	f, ok := m.files[req.GetPath()]
	if !ok {
		return nil, status.Error(codes.NotFound, "arquivo não encontrado")
	}
	return &pb.GetFileResponse{File: f.info, Blocks: f.blocks}, nil
}

func parentDir(p string) string {
	d := path.Dir(p)
	return d
}

func (m *fakeMaster) ListFiles(_ context.Context, req *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	dir := req.GetPrefix()
	if dir == "" {
		dir = "/"
	}
	resp := &pb.ListFilesResponse{}
	for p, f := range m.files {
		if parentDir(p) == dir {
			resp.Files = append(resp.Files, f.info)
		}
	}
	for d := range m.dirs {
		if d != "/" && parentDir(d) == dir {
			resp.Files = append(resp.Files, &pb.FileInfo{Path: d, IsDir: true})
		}
	}
	sort.Slice(resp.Files, func(i, j int) bool { return resp.Files[i].Path < resp.Files[j].Path })
	return resp, nil
}

func (m *fakeMaster) Move(_ context.Context, req *pb.MoveRequest) (*pb.MoveResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	f, ok := m.files[req.GetFrom()]
	if !ok {
		return nil, status.Error(codes.NotFound, "origem não encontrada")
	}
	if _, exists := m.files[req.GetTo()]; exists && !req.GetOverwrite() {
		return nil, status.Error(codes.AlreadyExists, "destino já existe")
	}
	delete(m.files, req.GetFrom())
	f.info.Path = req.GetTo()
	m.files[req.GetTo()] = f
	return &pb.MoveResponse{File: f.info}, nil
}

func (m *fakeMaster) MakeDir(_ context.Context, req *pb.MakeDirRequest) (*pb.MakeDirResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.dirs[parentDir(req.GetPath())] {
		if !req.GetParents() {
			return nil, status.Error(codes.NotFound, "diretório pai não existe")
		}
		for d := parentDir(req.GetPath()); d != "/"; d = parentDir(d) {
			m.dirs[d] = true
		}
	}
	m.dirs[req.GetPath()] = true
	return &pb.MakeDirResponse{Dir: &pb.FileInfo{Path: req.GetPath(), IsDir: true}}, nil
}

func (m *fakeMaster) RemoveDir(_ context.Context, req *pb.RemoveDirRequest) (*pb.RemoveDirResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	d := req.GetPath()
	if !m.dirs[d] {
		return nil, status.Error(codes.NotFound, "diretório não existe")
	}
	var inside []string
	for p := range m.files {
		if strings.HasPrefix(p, d+"/") {
			inside = append(inside, p)
		}
	}
	for p := range m.dirs {
		if strings.HasPrefix(p, d+"/") {
			inside = append(inside, p)
		}
	}
	if len(inside) > 0 && !req.GetRecursive() {
		return nil, status.Error(codes.FailedPrecondition, "diretório não está vazio")
	}
	for _, p := range inside {
		delete(m.files, p)
		delete(m.dirs, p)
	}
	delete(m.dirs, d)
	return &pb.RemoveDirResponse{Deleted: true}, nil
}

func (m *fakeMaster) DeleteFile(_ context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.files[req.GetPath()]; !ok {
		return nil, status.Error(codes.NotFound, "arquivo não encontrado")
	}
	delete(m.files, req.GetPath())
	return &pb.DeleteFileResponse{Deleted: true}, nil
}

// ---------------------------------------------------------------------------

type cluster struct {
	nodes  []*testNode
	master *fakeMaster
	addr   string
}

func newCluster(t *testing.T, nodeCount, rf int) *cluster {
	t.Helper()
	c := &cluster{}
	for i := 0; i < nodeCount; i++ {
		c.nodes = append(c.nodes, startNode(t, fmt.Sprintf("storage-%d", i+1)))
	}
	c.master, c.addr = startMaster(t, c.nodes, rf)
	return c
}

func (c *cluster) client(t *testing.T, mod func(*Config)) *Client {
	t.Helper()
	cfg := Config{
		MasterAddr:  c.addr,
		BlockSize:   1000,
		Parallelism: 3,
		RPCTimeout:  5 * time.Second,
	}
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
	path := filepath.Join(t.TempDir(), "in.bin")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path, data
}

func TestRoundTripSizes(t *testing.T) {
	c := newCluster(t, 3, 2)
	cl := c.client(t, nil)
	ctx := context.Background()

	// 0 byte, menor que um bloco, exatamente um bloco, bloco + 1, vários.
	for _, size := range []int{0, 1, 1000, 1001, 5555} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			local, data := writeRandom(t, size)
			remote := fmt.Sprintf("/dados/%d.bin", size)

			info, err := cl.Upload(ctx, local, remote, UploadOptions{})
			if err != nil {
				t.Fatal(err)
			}
			wantBlocks := (size + 999) / 1000
			if int(info.GetBlockCount()) != wantBlocks || info.GetSize() != int64(size) {
				t.Fatalf("info = %v, esperado %d blocos", info, wantBlocks)
			}

			out := filepath.Join(t.TempDir(), "out.bin")
			if _, err := cl.Download(ctx, remote, out); err != nil {
				t.Fatal(err)
			}
			got, _ := os.ReadFile(out)
			if !bytes.Equal(got, data) {
				t.Fatal("conteúdo baixado difere do original")
			}
		})
	}
}

func TestFullSizeBlocks(t *testing.T) {
	c := newCluster(t, 2, 2)
	cl := c.client(t, func(cfg *Config) { cfg.BlockSize = MaxBlockSize })
	ctx := context.Background()

	local, data := writeRandom(t, 2*MaxBlockSize+123)
	if _, err := cl.Upload(ctx, local, "/grande.bin", UploadOptions{}); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "out.bin")
	if _, err := cl.Download(ctx, "/grande.bin", out); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(out)
	if !bytes.Equal(got, data) {
		t.Fatal("conteúdo baixado difere do original")
	}
}

func TestDownloadFallsBackToOtherReplica(t *testing.T) {
	c := newCluster(t, 3, 3)
	cl := c.client(t, nil)
	ctx := context.Background()

	local, data := writeRandom(t, 4321)
	if _, err := cl.Upload(ctx, local, "/x", UploadOptions{}); err != nil {
		t.Fatal(err)
	}

	// Derruba dois dos três nós: cada bloco ainda tem uma réplica viva.
	c.nodes[0].server.Stop()
	c.nodes[2].server.Stop()

	out := filepath.Join(t.TempDir(), "out.bin")
	if _, err := cl.Download(ctx, "/x", out); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(out)
	if !bytes.Equal(got, data) {
		t.Fatal("conteúdo baixado difere do original")
	}
}

func TestUploadWithDeadReplica(t *testing.T) {
	c := newCluster(t, 3, 3)
	c.nodes[1].server.Stop()
	ctx := context.Background()
	local, _ := writeRandom(t, 2500)

	// Com mínimo 1, o upload passa e o commit lista só os nós vivos.
	cl := c.client(t, nil)
	if _, err := cl.Upload(ctx, local, "/a", UploadOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, b := range c.master.lastCommit.GetBlocks() {
		if len(b.GetNodeIds()) != 2 {
			t.Fatalf("bloco %s: confirmado por %v, esperado 2 nós", b.GetBlockId(), b.GetNodeIds())
		}
		for _, id := range b.GetNodeIds() {
			if id == c.nodes[1].id {
				t.Fatal("nó desligado aparece como confirmado")
			}
		}
	}

	// Exigindo as 3 réplicas, o upload precisa falhar.
	strict := c.client(t, func(cfg *Config) { cfg.MinReplicas = 3 })
	if _, err := strict.Upload(ctx, local, "/b", UploadOptions{}); err == nil {
		t.Fatal("upload deveria falhar com um nó fora e min-replicas=3")
	}
	if _, ok := c.master.files["/b"]; ok {
		t.Fatal("upload falho não pode ser confirmado no mestre")
	}
}

func TestDownloadRejectsWrongHash(t *testing.T) {
	c := newCluster(t, 2, 2)
	cl := c.client(t, nil)
	ctx := context.Background()

	local, _ := writeRandom(t, 1500)
	if _, err := cl.Upload(ctx, local, "/x", UploadOptions{}); err != nil {
		t.Fatal(err)
	}

	// Simula metadados que não batem com o conteúdo dos nós.
	c.master.files["/x"].blocks[1].Hash = strings.Repeat("0", 64)

	dir := t.TempDir()
	out := filepath.Join(dir, "out.bin")
	if _, err := cl.Download(ctx, "/x", out); err == nil {
		t.Fatal("download deveria falhar com hash divergente")
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatalf("download falho deixou arquivos: %v", entries)
	}
}

func TestOverwriteListDelete(t *testing.T) {
	c := newCluster(t, 2, 1)
	cl := c.client(t, nil)
	ctx := context.Background()
	local, _ := writeRandom(t, 10)

	if _, err := cl.Upload(ctx, local, "/docs/a", UploadOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Upload(ctx, local, "/docs/a", UploadOptions{}); status.Code(unwrapStatus(err)) != codes.AlreadyExists {
		t.Fatalf("esperado AlreadyExists, veio %v", err)
	}
	if _, err := cl.Upload(ctx, local, "/docs/a", UploadOptions{Overwrite: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Upload(ctx, local, "/outros/b", UploadOptions{}); err != nil {
		t.Fatal(err)
	}

	files, err := cl.List(ctx, "/docs")
	if err != nil || len(files) != 1 || files[0].GetPath() != "/docs/a" {
		t.Fatalf("List = %v, %v", files, err)
	}
	if files[0].GetState() != pb.FileState_FILE_STATE_HEALTHY {
		t.Fatalf("estado = %v, esperado íntegro", files[0].GetState())
	}
	if err := cl.Delete(ctx, "/docs/a"); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Stat(ctx, "/docs/a"); err == nil {
		t.Fatal("arquivo removido ainda aparece")
	}
}

// unwrapStatus acha o status gRPC dentro de um erro embrulhado.
func unwrapStatus(err error) error {
	for e := err; e != nil; {
		if _, ok := status.FromError(e); ok {
			return e
		}
		u, ok := e.(interface{ Unwrap() error })
		if !ok {
			return err
		}
		e = u.Unwrap()
	}
	return err
}

func TestDirectoriesAndMove(t *testing.T) {
	c := newCluster(t, 2, 2)
	cl := c.client(t, nil)
	ctx := context.Background()

	if err := cl.MakeDir(ctx, "/a/b", false); err == nil {
		t.Fatal("mkdir sem -p deveria falhar sem o pai")
	}
	if err := cl.MakeDir(ctx, "/a/b", true); err != nil {
		t.Fatal(err)
	}

	local, data := writeRandom(t, 2048)
	if _, err := cl.Upload(ctx, local, "/a/b/x", UploadOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Move(ctx, "/a/b/x", "/a/y", false); err != nil {
		t.Fatal(err)
	}

	// O arquivo movido continua legível (só os metadados mudaram).
	out := filepath.Join(t.TempDir(), "out")
	if _, err := cl.Download(ctx, "/a/y", out); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(out); !bytes.Equal(got, data) {
		t.Fatal("conteúdo difere após mv")
	}

	entries, err := cl.List(ctx, "/a")
	if err != nil || len(entries) != 2 || !entries[0].GetIsDir() || entries[1].GetPath() != "/a/y" {
		t.Fatalf("ls /a = %v, %v", entries, err)
	}

	if err := cl.RemoveDir(ctx, "/a", false); err == nil {
		t.Fatal("rmdir sem -r deveria falhar com diretório não vazio")
	}
	if err := cl.RemoveDir(ctx, "/a", true); err != nil {
		t.Fatal(err)
	}
	if entries, _ := cl.List(ctx, "/"); len(entries) != 0 {
		t.Fatalf("raiz deveria estar vazia: %v", entries)
	}
}

func TestUnderReplicatedState(t *testing.T) {
	c := newCluster(t, 3, 3)
	c.nodes[2].server.Stop()
	cl := c.client(t, nil)
	ctx := context.Background()

	local, _ := writeRandom(t, 100)
	if _, err := cl.Upload(ctx, local, "/s", UploadOptions{}); err != nil {
		t.Fatal(err)
	}
	st, err := cl.Stat(ctx, "/s")
	if err != nil {
		t.Fatal(err)
	}
	if st.GetFile().GetState() != pb.FileState_FILE_STATE_UNDER_REPLICATED {
		t.Fatalf("estado = %v, esperado sub-replicado", st.GetFile().GetState())
	}
}
