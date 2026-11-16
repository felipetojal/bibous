package storage

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

type Snapshot struct {
	FormatVersion int     `json:"format_version"`
	Blocks        []Block `json:"blocks"`
}

// O chamador deve manter s.mu bloqueado.
func (s *Store) saveSnapshotLocked(blocks map[BlockKey]Block) (err error) {
	snapshot := Snapshot{
		FormatVersion: 1,
		Blocks:        make([]Block, 0, len(blocks)),
	}

	for _, block := range blocks {
		snapshot.Blocks = append(snapshot.Blocks, block)
	}

	// Mantém uma ordem previsível no JSON.
	sort.Slice(snapshot.Blocks, func(i, j int) bool {
		a, b := snapshot.Blocks[i], snapshot.Blocks[j]
		if a.BlockID == b.BlockID {
			return a.Version < b.Version
		}
		return a.BlockID < b.BlockID
	})

	f, err := os.CreateTemp(s.DataDir, ".snapshot-*")
	if err != nil {
		return fmt.Errorf("criar snapshot temporário: %w", err)
	}

	tempPath := f.Name()
	closed := false
	published := false

	defer func() {
		if !closed {
			if closeErr := f.Close(); closeErr != nil {
				err = errors.Join(err, closeErr)
			}
		}

		if !published {
			if removeErr := os.Remove(tempPath); removeErr != nil &&
				!errors.Is(removeErr, os.ErrNotExist) {
				err = errors.Join(err, removeErr)
			}
		}
	}()

	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")

	if err := encoder.Encode(snapshot); err != nil {
		return fmt.Errorf("serializar snapshot: %w", err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("sincronizar snapshot: %w", err)
	}

	closeErr := f.Close()
	closed = true
	if closeErr != nil {
		return fmt.Errorf("fechar snapshot: %w", closeErr)
	}

	path := filepath.Join(s.DataDir, "snapshot.json")

	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("publicar snapshot: %w", err)
	}
	published = true

	dir, err := os.Open(s.DataDir)
	if err != nil {
		return fmt.Errorf("abrir diretório do snapshot: %w", err)
	}

	syncErr := dir.Sync()
	dirCloseErr := dir.Close()

	if err := errors.Join(syncErr, dirCloseErr); err != nil {
		return fmt.Errorf("sincronizar diretório do snapshot: %w", err)
	}

	return nil
}

// Persiste uma cópia atualizada antes de substituir o mapa em memória.
// O chamador deve manter s.mu bloqueado.
func (s *Store) persistBlockLocked(b Block) error {
	next := make(map[BlockKey]Block, len(s.blocks)+1)

	for key, block := range s.blocks {
		next[key] = block
	}

	key := BlockKey{BlockID: b.BlockID, Version: b.Version}
	next[key] = b

	if err := s.saveSnapshotLocked(next); err != nil {
		s.persistenceErr = err
		return fmt.Errorf("persistir inventário: %w", err)
	}

	s.blocks = next
	return nil
}

// Executada na inicialização, antes de compartilhar o Store.
func (s *Store) loadSnapshot() error {
	path := filepath.Join(s.DataDir, "snapshot.json")

	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		// Um nó novo pode começar vazio. Blocos preexistentes
		// sem snapshot precisam de reconciliação.
		return s.checkUntrackedBlocks()
	}
	if err != nil {
		return fmt.Errorf("ler snapshot: %w", err)
	}

	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("snapshot inválido: %w", err)
	}

	if snapshot.FormatVersion != 1 {
		return fmt.Errorf(
			"versão de snapshot não suportada: %d",
			snapshot.FormatVersion,
		)
	}

	loaded := make(map[BlockKey]Block, len(snapshot.Blocks))

	for _, b := range snapshot.Blocks {
		blockPath, err := s.blockPath(&b)
		if err != nil {
			return fmt.Errorf("metadados inválidos: %w", err)
		}

		hashBytes, err := hex.DecodeString(b.Hash)
		if err != nil || len(hashBytes) != 32 {
			return fmt.Errorf("hash inválido no bloco %s", b.BlockID)
		}
		b.Hash = hex.EncodeToString(hashBytes)

		if b.Size < 0 {
			return fmt.Errorf("tamanho negativo no bloco %s", b.BlockID)
		}

		// Esta versão só persiste estados estáveis.
		if b.State != VALID && b.State != CORRUPT {
			return fmt.Errorf(
				"estado não recuperável no bloco %s: %d",
				b.BlockID, b.State,
			)
		}

		key := BlockKey{BlockID: b.BlockID, Version: b.Version}
		if _, exists := loaded[key]; exists {
			return fmt.Errorf(
				"entrada duplicada: %s versão %d",
				b.BlockID, b.Version,
			)
		}

		if b.State == VALID {
			content, err := os.ReadFile(blockPath)
			if err != nil {
				return fmt.Errorf(
					"recuperar bloco %s: %w", b.BlockID, err,
				)
			}

			if err := validateContent(&b, content); err != nil {
				return fmt.Errorf(
					"bloco %s inválido na recuperação: %w",
					b.BlockID, err,
				)
			}
		}

		loaded[key] = b
	}

	s.blocks = loaded
	return s.checkUntrackedBlocks()
}

// Não adota automaticamente arquivos cujo hash esperado foi perdido.
func (s *Store) checkUntrackedBlocks() error {
	expected := make(map[string]bool, len(s.blocks))

	for _, b := range s.blocks {
		path, err := s.blockPath(&b)
		if err != nil {
			return err
		}
		expected[filepath.Base(path)] = true
	}

	entries, err := os.ReadDir(s.DataDir)
	if err != nil {
		return fmt.Errorf("listar diretório de dados: %w", err)
	}

	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".blk" &&
			!expected[entry.Name()] {
			return fmt.Errorf(
				"bloco sem registro no snapshot: %s; "+
					"reconciliação necessária",
				entry.Name(),
			)
		}
	}

	return nil
}
