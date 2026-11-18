package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type Snapshot struct {
	FormatVersion int     `json:"format_version"`
	Blocks        []Block `json:"blocks"`
}

func (s *Store) cloneBlocks() map[BlockKey]Block {
	next := make(map[BlockKey]Block, len(s.blocks)+1)
	for key, b := range s.blocks {
		next[key] = b
	}
	return next
}
func (s *Store) commitLocked(next map[BlockKey]Block) error {
	if err := s.saveSnapshotLocked(next); err != nil {
		s.persistenceErr = err
		return fmt.Errorf("persistir inventário: %w", err)
	}
	s.blocks = next
	return nil
}
func (s *Store) persistBlockLocked(b Block) error {
	next := s.cloneBlocks()
	next[BlockKey{b.BlockID, b.Version}] = b
	return s.commitLocked(next)
}
func (s *Store) saveSnapshotLocked(blocks map[BlockKey]Block) (err error) {
	snapshot := Snapshot{FormatVersion: 2, Blocks: make([]Block, 0, len(blocks))}
	for _, b := range blocks {
		snapshot.Blocks = append(snapshot.Blocks, b)
	}
	sort.Slice(snapshot.Blocks, func(i, j int) bool {
		a, b := snapshot.Blocks[i], snapshot.Blocks[j]
		if a.BlockID == b.BlockID {
			return a.Version < b.Version
		}
		return a.BlockID < b.BlockID
	})
	dir := filepath.Join(s.DataDir, "tmp")
	f, err := os.CreateTemp(dir, "snapshot-*")
	if err != nil {
		return err
	}
	name := f.Name()
	closed := false
	defer func() {
		if !closed {
			_ = f.Close()
		}
		_ = os.Remove(name)
	}()
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	if err = encoder.Encode(snapshot); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	err = f.Close()
	closed = true
	if err != nil {
		return err
	}
	if err = os.Rename(name, filepath.Join(s.DataDir, "snapshot.json")); err != nil {
		return err
	}
	return errors.Join(syncDir(dir), syncDir(s.DataDir))
}

// Carrega v1 antigo e v2 com intenções/tombstones. JSON inválido falha fechado.
func (s *Store) loadSnapshot() error {
	data, err := os.ReadFile(filepath.Join(s.DataDir, "snapshot.json"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err == nil {
		var snapshot Snapshot
		if err = json.Unmarshal(data, &snapshot); err != nil {
			return fmt.Errorf("snapshot inválido; restaure uma cópia confiável: %w", err)
		}
		if snapshot.FormatVersion != 1 && snapshot.FormatVersion != 2 {
			return fmt.Errorf("versão de snapshot não suportada")
		}
		for _, b := range snapshot.Blocks {
			if _, err = s.blockPath(&b); err != nil {
				return err
			}
			if b.State < WRITING || b.State > DELETED {
				return fmt.Errorf("estado inválido")
			}
			if b.State != DELETING && b.State != DELETED {
				if err = validateMetadata(b); err != nil {
					return err
				}
			}
			key := BlockKey{b.BlockID, b.Version}
			if _, exists := s.blocks[key]; exists {
				return fmt.Errorf("identidade duplicada no snapshot")
			}
			s.blocks[key] = b
		}
	}
	// Recuperação é idempotente, inclusive se o processo morrer novamente aqui.
	for key, b := range s.blocks {
		path, _ := s.blockPath(&b)
		switch b.State {
		case DELETING, DELETED:
			if err = os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			if err = syncDir(s.DataDir); err != nil {
				return err
			}
			b.State = DELETED
		case VALID, WRITING:
			content, e := readContent(path)
			if e == nil {
				e = validateContent(&b, content)
			}
			if e != nil {
				b.State = CORRUPT
			} else {
				b.State = VALID
			}
		}
		s.blocks[key] = b
	}
	if err = s.reconcileFiles(); err != nil {
		return err
	}
	// Confirma que o diretório é utilizável e persiste alterações da recuperação.
	return s.saveSnapshotLocked(s.blocks)
}
func (s *Store) reconcileFiles() error {
	expected := make(map[string]bool)
	for _, b := range s.blocks {
		path, _ := s.blockPath(&b)
		expected[filepath.Base(path)] = true
	}
	entries, err := os.ReadDir(s.DataDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		path := filepath.Join(s.DataDir, name)
		if strings.HasPrefix(name, ".upload-") || strings.HasPrefix(name, ".snapshot-") {
			if !entry.IsDir() {
				if err = os.Remove(path); err != nil {
					return err
				}
			}
		}
		if filepath.Ext(name) == ".blk" && !expected[name] {
			// Não inventa hash esperado para um órfão e não o serve.
			parent := filepath.Join(s.DataDir, "quarantine")
			dir, e := os.MkdirTemp(parent, "orphan-*")
			if e != nil {
				return e
			}
			if err = os.Rename(path, filepath.Join(dir, name)); err != nil {
				return err
			}
			if err = errors.Join(syncDir(dir), syncDir(parent), syncDir(s.DataDir)); err != nil {
				return err
			}
		}
	}
	tmp := filepath.Join(s.DataDir, "tmp")
	entries, err = os.ReadDir(tmp)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() && (strings.HasPrefix(entry.Name(), "upload-") || strings.HasPrefix(entry.Name(), "snapshot-")) {
			if err = os.Remove(filepath.Join(tmp, entry.Name())); err != nil {
				return err
			}
		}
	}
	return errors.Join(syncDir(tmp), syncDir(s.DataDir))
}
