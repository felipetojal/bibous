package master

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type fakeTransport struct {
	mu     sync.Mutex
	blocks map[string][]byte
}

func (f *fakeTransport) key(n NodeInfo, b BlockMetadata) string {
	return fmt.Sprintf("%s/%s/%d", n.ID, b.ID, b.Version)
}

func (f *fakeTransport) ReadBlock(_ context.Context, n NodeInfo, b BlockMetadata) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.blocks[f.key(n, b)]
	if !ok {
		return nil, errors.New("missing block")
	}
	return bytes.Clone(data), nil
}

func (f *fakeTransport) WriteBlock(_ context.Context, n NodeInfo, b BlockMetadata, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blocks[f.key(n, b)] = bytes.Clone(data)
	return nil
}

func digest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

func testSetup(t *testing.T) (*Master, *fakeTransport, string) {
	t.Helper()
	dir := t.TempDir()
	store := &fakeTransport{blocks: map[string][]byte{}}
	cfg := DefaultConfig()
	cfg.MaxDiskUsage = 1
	m, err := NewMaster(dir, cfg, store)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		err = m.RegisterNode(NodeInfo{ID: fmt.Sprintf("node-%d", i), Address: fmt.Sprintf("node-%d:9000", i), MachineID: fmt.Sprintf("host-%d", i), DiskID: "disk-1", TotalSize: 100000000, AvailableSize: 100000000})
		if err != nil {
			t.Fatal(err)
		}
	}
	return m, store, dir
}

func TestUploadCommitPersistenceAndRecovery(t *testing.T) {
	m, store, dir := testSetup(t)
	if err := m.CreateDirectory("/docs"); err != nil {
		t.Fatal(err)
	}
	a, b := []byte("hello "), []byte("distributed world")
	whole := append(bytes.Clone(a), b...)
	upload, err := m.PrepareUpload("/docs/report.txt", uint64(len(whole)), digest(whole), []BlockSpec{{uint64(len(a)), digest(a)}, {uint64(len(b)), digest(b)}}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.GetFile("/docs/report.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("uncommitted file visible: %v", err)
	}
	if len(upload.File.Blocks) != 2 {
		t.Fatal("wrong block count")
	}
	for i, block := range upload.File.Blocks {
		data := a
		if i == 1 {
			data = b
		}
		machines := map[string]bool{}
		for _, id := range block.Replicas {
			n, ok := m.nr.Get(id)
			if !ok {
				t.Fatal("missing node")
			}
			if machines[n.MachineID] {
				t.Fatal("replicas on same machine")
			}
			machines[n.MachineID] = true
			if err := store.WriteBlock(context.Background(), n, block, data); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err = m.CommitUpload(context.Background(), upload.ID); err != nil {
		t.Fatal(err)
	}
	if status := m.FileStatus(upload.File); status != "HEALTHY" {
		t.Fatal(status)
	}
	restart, err := NewMaster(dir, DefaultConfig(), store)
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := restart.GetFile("/docs/report.txt")
	if err != nil || len(persisted.Blocks) != 2 {
		t.Fatalf("state not persisted: %v", err)
	}
	if status := restart.FileStatus(persisted); status != "UNAVAILABLE" {
		t.Fatalf("old node status trusted after restart: %s", status)
	}
	for i := 1; i <= 5; i++ {
		n, _ := m.nr.Get(fmt.Sprintf("node-%d", i))
		if err := restart.RegisterNode(n); err != nil {
			t.Fatal(err)
		}
	}
	// Simula a perda de um nó que contém o primeiro bloco.
	failedID := persisted.Blocks[0].Replicas[0]
	restart.nr.mu.Lock()
	n := restart.nr.nodes[failedID]
	n.LastHeartbeat = time.Now().Add(-time.Hour)
	restart.nr.nodes[failedID] = n
	restart.nr.mu.Unlock()
	restart.nr.Reap(time.Now(), 9*time.Second, 15*time.Second)
	if status := restart.FileStatus(persisted); status != "UNDER_REPLICATED" {
		t.Fatalf("expected under-replicated, got %s", status)
	}
	tasks := restart.PlannedRepairs()
	if len(tasks) == 0 {
		t.Fatal("no recovery planned")
	}
	if err := restart.RepairOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	fixed, err := restart.GetFile("/docs/report.txt")
	if err != nil {
		t.Fatal(err)
	}
	if status := restart.FileStatus(fixed); status != "HEALTHY" {
		t.Fatal(status)
	}
	if err := restart.MoveFile("/docs/report.txt", "/docs/renamed.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := restart.GetFile("/docs/renamed.txt"); err != nil {
		t.Fatal(err)
	}
	if err := restart.DeleteFile("/docs/renamed.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := restart.GetFile("/docs/renamed.txt"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("file not deleted: %v", err)
	}
}
func TestRejectsMissingOrCorruptReplicas(t *testing.T) {
	m, store, _ := testSetup(t)
	content := []byte("test")
	upload, err := m.PrepareUpload("/a.txt", 4, digest(content), []BlockSpec{{4, digest(content)}}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.CommitUpload(context.Background(), upload.ID); err == nil {
		t.Fatal("missing replicas accepted")
	}
	for _, id := range upload.File.Blocks[0].Replicas {
		n, _ := m.nr.Get(id)
		_ = store.WriteBlock(context.Background(), n, upload.File.Blocks[0], content)
	}
	n, _ := m.nr.Get(upload.File.Blocks[0].Replicas[1])
	store.mu.Lock()
	store.blocks[store.key(n, upload.File.Blocks[0])] = []byte("bad!")
	store.mu.Unlock()
	if _, err = m.CommitUpload(context.Background(), upload.ID); err == nil {
		t.Fatal("corrupted replica accepted")
	}
}
func TestCapacityAndFailureDomain(t *testing.T) {
	m, _, _ := testSetup(t)
	// Identidades distintas não permitem duas réplicas na mesma máquina.
	if err := m.RegisterNode(NodeInfo{ID: "node-6", Address: "n6:9000", MachineID: "host-1", DiskID: "disk-2", TotalSize: 100000000, AvailableSize: 100000000}); err != nil {
		t.Fatal(err)
	}
	u, err := m.PrepareUpload("/x", 1, digest([]byte("x")), []BlockSpec{{1, digest([]byte("x"))}}, 5)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, id := range u.File.Blocks[0].Replicas {
		n, _ := m.nr.Get(id)
		if seen[n.MachineID] {
			t.Fatal("duplicate failure domain")
		}
		seen[n.MachineID] = true
	}
	if _, err := m.PrepareUpload("/y", 1, digest([]byte("y")), []BlockSpec{{1, digest([]byte("y"))}}, 6); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected insufficient machines, got %v", err)
	}
}
func TestDirectoryAndPendingConflict(t *testing.T) {
	m, _, _ := testSetup(t)
	if err := m.CreateDirectory("/dir"); err != nil {
		t.Fatal(err)
	}
	_, err := m.PrepareUpload("/dir/a", 1, digest([]byte("a")), []BlockSpec{{1, digest([]byte("a"))}}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteDirectory("/dir"); !errors.Is(err, ErrConflict) {
		t.Fatalf("directory removed despite pending upload: %v", err)
	}
	_, err = m.PrepareUpload("/dir/a", 1, digest([]byte("a")), []BlockSpec{{1, digest([]byte("a"))}}, 2)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting upload accepted: %v", err)
	}
}
func TestHeartbeatTimeout(t *testing.T) {
	m, _, _ := testSetup(t)
	n, _ := m.nr.Get("node-1")
	if err := m.nr.Heartbeat("unknown", nil); err == nil {
		t.Fatal("unknown heartbeat accepted")
	}
	if err := m.nr.Register(NodeInfo{ID: "node-1", Address: "n", MachineID: "other", DiskID: "d", TotalSize: 100, AvailableSize: 100}); err == nil {
		t.Fatal("identity mismatch accepted")
	}
	m.nr.mu.Lock()
	n.LastHeartbeat = time.Now().Add(-20 * time.Second)
	m.nr.nodes["node-1"] = n
	m.nr.mu.Unlock()
	ids := m.nr.Reap(time.Now(), 9*time.Second, 15*time.Second)
	if len(ids) != 1 || ids[0] != "node-1" {
		t.Fatalf("unexpected failed IDs: %v", ids)
	}
	n, _ = m.nr.Get("node-1")
	if n.Status != NodeDead {
		t.Fatal(n.Status)
	}
	if err := m.nr.Heartbeat("node-1", nil); err != nil {
		t.Fatal(err)
	}
	n, _ = m.nr.Get("node-1")
	if n.Status != NodeAlive {
		t.Fatal(n.Status)
	}
}

func TestDrainAndCorruptionDoNotReuseBadReplica(t *testing.T) {
	m, store, _ := testSetup(t)
	bytes1 := []byte("abc")
	u, err := m.PrepareUpload("/x", 3, digest(bytes1), []BlockSpec{{3, digest(bytes1)}}, 3)
	if err != nil {
		t.Fatal(err)
	}
	b := u.File.Blocks[0]
	for _, id := range b.Replicas {
		n, _ := m.nr.Get(id)
		_ = store.WriteBlock(context.Background(), n, b, bytes1)
	}
	if _, err := m.CommitUpload(context.Background(), u.ID); err != nil {
		t.Fatal(err)
	}
	bad := b.Replicas[0]
	if err := m.ReportCorruption(bad, b.ID, b.Version); err != nil {
		t.Fatal(err)
	}
	tasks := m.PlannedRepairs()
	if len(tasks) != 1 {
		t.Fatalf("expected one task, got %d", len(tasks))
	}
	if tasks[0].Destination.ID == bad {
		t.Fatal("corrupt replica destination reused")
	}
	if err := m.RepairOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	f, _ := m.GetFile("/x")
	if m.FileStatus(f) != "HEALTHY" {
		t.Fatal("did not recover corruption")
	}
	if err := m.DrainNode(f.Blocks[0].Replicas[0]); err != nil {
		t.Fatal(err)
	}
	f, _ = m.GetFile("/x")
	if m.FileStatus(f) != "UNDER_REPLICATED" {
		t.Fatal("drain did not lower active replica count")
	}
}

func TestConcurrentUploadReservation(t *testing.T) {
	m, _, _ := testSetup(t)
	const workers = 20
	var wg sync.WaitGroup
	success := make(chan bool, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := m.PrepareUpload("/same", 1, digest([]byte("a")), []BlockSpec{{1, digest([]byte("a"))}}, 3)
			success <- err == nil
		}()
	}
	wg.Wait()
	close(success)
	count := 0
	for ok := range success {
		if ok {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly one upload reservation, got %d", count)
	}
}

// TestNodeIdentityAfterDeath impede que uma máquina diferente se aproprie das
// referências de réplicas de um nó que ficou indisponível.
func TestNodeIdentityAfterDeath(t *testing.T) {
	registry := NewNodeRegistry()
	original := NodeInfo{
		ID: "node-perdido", Address: "127.0.0.1:9001",
		MachineID: "machine-1", DiskID: "disk-1",
		TotalSize: 100, AvailableSize: 100,
	}
	if err := registry.Register(original); err != nil {
		t.Fatal(err)
	}
	registry.Reap(time.Now().Add(time.Minute), 9*time.Second, 15*time.Second)

	outraMaquina := original
	outraMaquina.MachineID = "machine-2"
	if err := registry.Register(outraMaquina); err == nil {
		t.Fatal("um ID antigo foi reutilizado por outra máquina")
	}
	if err := registry.Register(original); err != nil {
		t.Fatalf("o mesmo disco deveria poder se registrar novamente: %v", err)
	}
}
