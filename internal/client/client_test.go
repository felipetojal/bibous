package client

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/felipetojal/bibous/internal/storage"
	pb "github.com/felipetojal/bibous/proto"
	"google.golang.org/grpc"
)

type cluster struct {
	nodes   []Replica
	stores  []*storage.Store
	servers []*grpc.Server
	peers   map[string]string
}

func newCluster(t *testing.T) *cluster {
	t.Helper()
	c := &cluster{peers: make(map[string]string)}
	var listeners []net.Listener
	for _, id := range []string{"storage-a", "storage-b", "storage-c"} {
		l, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		s, e := storage.NewStore(t.TempDir())
		if e != nil {
			l.Close()
			t.Fatal(e)
		}
		c.nodes = append(c.nodes, Replica{NodeID: id, Address: l.Addr().String()})
		c.stores = append(c.stores, s)
		c.peers[id] = l.Addr().String()
		listeners = append(listeners, l)
	}
	for i, l := range listeners {
		c.serve(i, l, nil)
	}
	t.Cleanup(func() {
		for _, s := range c.servers {
			s.Stop()
		}
		for _, s := range c.stores {
			_ = s.Close()
		}
	})
	return c
}
func (c *cluster) serve(i int, l net.Listener, override pb.StorageServiceServer) {
	server := grpc.NewServer(grpc.MaxRecvMsgSize(storage.MaxRPCMessageBytes), grpc.MaxSendMsgSize(storage.MaxRPCMessageBytes))
	if override == nil {
		override = storage.NewRPCServer(c.stores[i], c.nodes[i].NodeID, c.peers)
	}
	pb.RegisterStorageServiceServer(server, override)
	if i < len(c.servers) {
		c.servers[i] = server
	} else {
		c.servers = append(c.servers, server)
	}
	go func() { _ = server.Serve(l) }()
}
func (c *cluster) restart(t *testing.T, i int, override pb.StorageServiceServer) {
	t.Helper()
	l, e := net.Listen("tcp", c.nodes[i].Address)
	if e != nil {
		t.Fatal(e)
	}
	c.serve(i, l, override)
}
func TestReplicationFallbackRepairAndPendingDelete(t *testing.T) {
	c := newCluster(t)
	cli := Client{Timeout: time.Second}
	ctx := context.Background()
	dir := t.TempDir()
	input, manifest, output := filepath.Join(dir, "input"), filepath.Join(dir, "manifest.json"), filepath.Join(dir, "out")
	data := bytes.Repeat([]byte("a"), storage.MaxBlockBytes+3)
	if e := os.WriteFile(input, data, 0600); e != nil {
		t.Fatal(e)
	}
	if e := cli.Upload(ctx, input, manifest, c.nodes); e != nil {
		t.Fatal(e)
	}
	m, e := LoadManifest(manifest)
	if e != nil {
		t.Fatal(e)
	}
	if len(m.Blocks) != 2 || m.Blocks[1].Size != 3 {
		t.Fatal("chunk boundaries")
	}
	for _, b := range m.Blocks {
		for _, s := range c.stores {
			got, e := s.ReadBlock(b.BlockID, 1)
			if e != nil || hashBytes(got) != b.Hash {
				t.Fatal("replica", e)
			}
		}
	}
	c.servers[0].Stop()
	path := filepath.Join(c.stores[1].DataDir, m.Blocks[0].BlockID+"-v1.blk")
	if e = os.WriteFile(path, []byte("bad"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = cli.Download(ctx, manifest, output); e != nil {
		t.Fatal("fallback", e)
	}
	got, e := os.ReadFile(output)
	if e != nil || !bytes.Equal(got, data) {
		t.Fatal("download", e)
	}
	if e = cli.Repair(ctx, manifest, "storage-b"); e != nil {
		t.Fatal("direct copy", e)
	}
	if c.stores[1].Status().Corrupt != 0 || c.stores[1].Status().Valid != 2 {
		t.Fatal("repair status")
	}
	if e = cli.Delete(ctx, manifest); e == nil {
		t.Fatal("delete succeeded while A down")
	}
	m, e = LoadManifest(manifest)
	if e != nil || m.State != "deleting" {
		t.Fatal("pending delete not saved", e)
	}
	c.restart(t, 0, nil)
	if e = cli.Delete(ctx, manifest); e != nil {
		t.Fatal(e)
	}
	for _, s := range c.stores {
		if s.Status().Deleted != 2 {
			t.Fatal("tombstones missing")
		}
	}
	if e = cli.Download(ctx, manifest, filepath.Join(dir, "deleted")); e == nil {
		t.Fatal("downloaded deleted file")
	}
}

type badServer struct {
	pb.UnimplementedStorageServiceServer
	calls atomic.Int32
}

func (s *badServer) Heartbeat(ctx context.Context, r *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return &pb.HeartbeatResponse{NodeId: "storage-a", Nonce: r.Nonce}, nil
}
func (s *badServer) GetBlock(ctx context.Context, r *pb.GetRequest) (*pb.GetResponse, error) {
	s.calls.Add(1)
	return &pb.GetResponse{BlockId: r.BlockId, Version: r.Version, Size: 3, Hash: hashBytes([]byte("abc")), Data: []byte("bad")}, nil
}
func TestInvalidResponseAndAllReplicasLost(t *testing.T) {
	c := newCluster(t)
	cli := Client{Timeout: time.Second}
	dir := t.TempDir()
	input, manifest := filepath.Join(dir, "in"), filepath.Join(dir, "manifest")
	if e := os.WriteFile(input, []byte("abc"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := cli.Upload(context.Background(), input, manifest, c.nodes); e != nil {
		t.Fatal(e)
	}
	c.servers[0].Stop()
	bad := &badServer{}
	c.restart(t, 0, bad)
	if e := cli.Download(context.Background(), manifest, filepath.Join(dir, "out")); e != nil {
		t.Fatal(e)
	}
	if bad.calls.Load() != 1 {
		t.Fatal("bad replica not exercised")
	}
	c.servers[1].Stop()
	c.servers[2].Stop()
	output := filepath.Join(dir, "failed")
	if e := cli.Download(context.Background(), manifest, output); e == nil {
		t.Fatal("invalid bytes accepted")
	}
	if _, e := os.Stat(output); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("partial output published")
	}
}
func TestEmptyFileAndNoOverwrite(t *testing.T) {
	c := newCluster(t)
	cli := Client{Timeout: time.Second}
	dir := t.TempDir()
	input, manifest, output := filepath.Join(dir, "in"), filepath.Join(dir, "manifest"), filepath.Join(dir, "out")
	if e := os.WriteFile(input, nil, 0600); e != nil {
		t.Fatal(e)
	}
	if e := cli.Upload(context.Background(), input, manifest, c.nodes); e != nil {
		t.Fatal(e)
	}
	if e := cli.Download(context.Background(), manifest, output); e != nil {
		t.Fatal(e)
	}
	if e := cli.Download(context.Background(), manifest, output); e == nil {
		t.Fatal("overwrote output")
	}
	if e := cli.Upload(context.Background(), input, manifest, c.nodes); e == nil {
		t.Fatal("overwrote manifest")
	}
}
