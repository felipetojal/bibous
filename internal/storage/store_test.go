package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
)

func openTestStore(t *testing.T, dir string) *Store {
	t.Helper()
	s, e := NewStore(dir)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
func testBlock(t *testing.T, id string, data []byte) *Block {
	t.Helper()
	b, e := CreateBlock(id, 1, data)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func TestPersistenceCorruptionRepairAndDeletion(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir)
	data := []byte("first block")
	b := testBlock(t, "B1", data)
	if e := s.SaveBlock(b, data); e != nil {
		t.Fatal(e)
	}
	if e := s.SaveBlock(b, data); e != nil {
		t.Fatal("retry", e)
	}
	other := testBlock(t, "B1", []byte("other"))
	if !errors.Is(s.SaveBlock(other, []byte("other")), ErrConflict) {
		t.Fatal("expected conflict")
	}
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	s = openTestStore(t, dir)
	got, e := s.ReadBlock("B1", 1)
	if e != nil || !bytes.Equal(data, got) {
		t.Fatal("restart", e)
	}
	if e = os.WriteFile(filepath.Join(dir, "B1-v1.blk"), []byte("bad"), 0600); e != nil {
		t.Fatal(e)
	}
	result, e := s.Scrub()
	if e != nil || result.Corrupt != 1 {
		t.Fatal(result, e)
	}
	if _, e = s.ReadBlock("B1", 1); !errors.Is(e, ErrCorrupt) {
		t.Fatal("served corruption", e)
	}
	if e = s.SaveBlock(b, data); e != nil {
		t.Fatal("repair", e)
	}
	if e = s.DeleteBlock("B1", 1); e != nil {
		t.Fatal(e)
	}
	if e = s.DeleteBlock("B1", 1); e != nil {
		t.Fatal("repeat delete", e)
	}
	if e = s.SaveBlock(b, data); !errors.Is(e, ErrDeleted) {
		t.Fatal("resurrected", e)
	}
	_ = s.Close()
	s = openTestStore(t, dir)
	if e = s.SaveBlock(b, data); !errors.Is(e, ErrDeleted) {
		t.Fatal("resurrected after restart", e)
	}
	if e = s.DeleteBlock("absent", 1); e != nil {
		t.Fatal(e)
	}
	if e = s.SaveBlock(testBlock(t, "absent", data), data); !errors.Is(e, ErrDeleted) {
		t.Fatal("late put accepted", e)
	}
}
func TestLockAndRecovery(t *testing.T) {
	dir := t.TempDir()
	s := openTestStore(t, dir)
	if another, e := NewStore(dir); e == nil {
		another.Close()
		t.Fatal("two owners")
	}
	b := testBlock(t, "B1", []byte("content"))
	if e := s.SaveBlock(b, []byte("content")); e != nil {
		t.Fatal(e)
	}
	_ = s.Close()
	if e := os.Remove(filepath.Join(dir, "B1-v1.blk")); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(dir, "orphan-v1.blk"), []byte("orphan"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(dir, "tmp", "upload-abandoned"), []byte("partial"), 0600); e != nil {
		t.Fatal(e)
	}
	s = openTestStore(t, dir)
	if s.Status().Corrupt != 1 {
		t.Fatal("missing block not marked corrupt")
	}
	if _, e := os.Stat(filepath.Join(dir, "orphan-v1.blk")); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("orphan still published")
	}
	matches, e := filepath.Glob(filepath.Join(dir, "quarantine", "orphan-*", "orphan-v1.blk"))
	if e != nil || len(matches) != 1 {
		t.Fatal("quarantine", matches, e)
	}
	entries, e := os.ReadDir(filepath.Join(dir, "tmp"))
	if e != nil || len(entries) != 0 {
		t.Fatal("temp cleanup", e)
	}
}
func TestConcurrentRequests(t *testing.T) {
	s := openTestStore(t, t.TempDir())
	data := []byte("concurrent")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, e := CreateBlock("same", 1, data)
			if e != nil {
				t.Error(e)
				return
			}
			if e = s.SaveBlock(b, data); e != nil {
				t.Error(e)
			}
			if _, e = s.ReadBlock("same", 1); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
}
func TestInvalidSnapshotFailsClosed(t *testing.T) {
	dir := t.TempDir()
	if e := os.WriteFile(filepath.Join(dir, "snapshot.json"), []byte("{"), 0600); e != nil {
		t.Fatal(e)
	}
	if s, e := NewStore(dir); e == nil {
		s.Close()
		t.Fatal("accepted malformed JSON")
	}
}
func TestLegacySnapshot(t *testing.T) {
	dir := t.TempDir()
	data := []byte("old")
	b := testBlock(t, "old", data)
	b.State = VALID
	raw, e := json.Marshal(Snapshot{FormatVersion: 1, Blocks: []Block{*b}})
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(dir, "snapshot.json"), raw, 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(dir, "old-v1.blk"), data, 0600); e != nil {
		t.Fatal(e)
	}
	s := openTestStore(t, dir)
	if _, e = s.ReadBlock("old", 1); e != nil {
		t.Fatal(e)
	}
}

// Processo filho realmente recebe SIGKILL nos limites de persistência.
func TestCrashHelper(t *testing.T) {
	stage := os.Getenv("BIBOUS_TEST_CRASH")
	if stage == "" {
		return
	}
	s, e := NewStore(os.Getenv("BIBOUS_TEST_DIR"))
	if e != nil {
		t.Fatal(e)
	}
	s.failpoint = func(at string) error {
		if at == stage {
			_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		}
		return nil
	}
	if stage == "delete-intent" || stage == "delete-unlinked" {
		e = s.DeleteBlock("B1", 1)
	} else {
		b, _ := CreateBlock("B1", 1, []byte("durable"))
		e = s.SaveBlock(b, []byte("durable"))
	}
	t.Fatalf("failpoint not reached: %v", e)
}
func TestCrashRecovery(t *testing.T) {
	for _, stage := range []string{"put-intent", "put-published", "delete-intent", "delete-unlinked"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			if stage == "delete-intent" || stage == "delete-unlinked" {
				s := openTestStore(t, dir)
				if e := s.SaveBlock(testBlock(t, "B1", []byte("durable")), []byte("durable")); e != nil {
					t.Fatal(e)
				}
				_ = s.Close()
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestCrashHelper$")
			cmd.Env = append(os.Environ(), "BIBOUS_TEST_CRASH="+stage, "BIBOUS_TEST_DIR="+dir)
			output, e := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(e, &exit) {
				t.Fatalf("child was not killed: %v %s", e, output)
			}
			state, ok := exit.Sys().(syscall.WaitStatus)
			if !ok || !state.Signaled() || state.Signal() != syscall.SIGKILL {
				t.Fatalf("unexpected child failure: %v %s", e, output)
			}
			s := openTestStore(t, dir)
			data, e := s.ReadBlock("B1", 1)
			switch stage {
			case "put-published":
				if e != nil || string(data) != "durable" {
					t.Fatal("lost published content", e)
				}
			case "put-intent":
				if !errors.Is(e, ErrCorrupt) {
					t.Fatal("invalid intent served", e)
				}
			default:
				if !errors.Is(e, ErrDeleted) {
					t.Fatal("delete not recovered", e)
				}
			}
		})
	}
}
