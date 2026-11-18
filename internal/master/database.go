package master

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// MetadataDB persiste cada transação por meio de um snapshot atômico.
type MetadataDB struct {
	dir      string
	filename string
}

// NewMetadataDB carrega o último snapshot ou inicia a árvore vazia.
func NewMetadataDB(dir string) (*MetadataDB, MetadataState, error) {
	if dir == "" {
		return nil, MetadataState{}, errors.New("metadata directory is required")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, MetadataState{}, err
	}
	d := &MetadataDB{dir: dir, filename: filepath.Join(dir, "master-state.json")}
	b, err := os.ReadFile(d.filename)
	if errors.Is(err, os.ErrNotExist) {
		return d, emptyState(), nil
	}
	if err != nil {
		return nil, MetadataState{}, err
	}
	var state MetadataState
	if err := json.Unmarshal(b, &state); err != nil {
		return nil, MetadataState{}, fmt.Errorf("decode metadata: %w", err)
	}
	if state.Files == nil || state.Pending == nil || state.Directories == nil || !state.Directories["/"] {
		return nil, MetadataState{}, errors.New("invalid metadata snapshot")
	}
	return d, state, nil
}

// Save grava um snapshot temporário, sincroniza e publica por rename.
func (d *MetadataDB) Save(s MetadataState) (err error) {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(d.dir, ".master-state-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer func() { _ = os.Remove(name) }()
	if _, err = f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, d.filename); err != nil {
		return err
	}
	dir, err := os.Open(d.dir)
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	return errors.Join(syncErr, closeErr)
}
