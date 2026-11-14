package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)


// Grava e lê blocos no disco
type Store struct {
	dataDir string
	mu      sync.Mutex
}

func NewStore(dataDir string) (*Store, error) {
	if dataDir == "" {
		return nil, errors.New("diretorio de dados vazio")
	}

	absDir, err := filepath.Abs(dataDir)
	
	if err != nil {
		return nil, fmt.Errorf("resolver diretorio: %w",err)
	}

	if err:= os.MkdirAll(absDir,0750); err != nil {
		return nil, fmt.Errorf("criar diretorio %q: %w", absDir, err)
	}

	return &Store{
		dataDir: absDir,
	}, nil 

}

func (s *Store) blockPath(b *Block) (string, error) {
	if b == nil {
		return "", errors.New("bloco ausente")
	}

	if !blockIDPattern.MatchString(b.BlockID) {
		return "", errors.New("BlockID invalido")
	}

	if b.Versao < 1 {
		return "", errors.New("Versao invalida")
	}

	name := b.BlockID + "-v" + strconv.Itoa(b.Versao) + ".blk"
	return filepath.Join(s.dataDir,name), nil
}

func validateContent(b *Block, data []byte) error {
	if int64(len(data)) != b.Size {
		return fmt.Errorf("tamanho diferente: esperado %d, recebido %d", b.Size, len(data))
	}

	sum := sha256.Sum256(data)
	atualHash := hex.EncodeToString(sum[:])

	if atualHash != b.Hash {
		return errors.New("SHA-256 diferente")
	}

	return nil
}


func (s *Store) SaveBlock(b *Block, data []byte) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// pega o path do bloco
	path, err := s.blockPath(b)
	if err != nil {
		return err
	}

	// valida o bloco
	if err := validateContent(b, data); err != nil {
		return err
	}

	// verifica se ja existe um path
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("bloco já existe: %s", b.BlockID)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("consultar destino: %w", err)
	}

	b.State = WRITING

	// Mesmo diretório: temporário e destino ficam no mesmo filesystem
	f, err := os.CreateTemp(s.dataDir, ".upload-*")
	if err != nil {
		return fmt.Errorf("criar temporário: %w", err)
	}

	tempPath := f.Name()
	closed := false
	published := false

	defer func() {
		if !closed {
			if closeErr := f.Close(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf(
					"fechar temporário: %w", closeErr,
				))
			}
		}

		if !published {
			if removeErr := os.Remove(tempPath); removeErr != nil {
				if !errors.Is(removeErr, os.ErrNotExist) {
					err = errors.Join(err, fmt.Errorf(
						"remover temporário: %w", removeErr,
					))
				}
			}
		}
	}()

	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("gravar bloco: %w", err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("sincronizar bloco: %w", err)
	}

	closeErr := f.Close()
	closed = true
	if closeErr != nil {
		return fmt.Errorf("fechar bloco: %w", closeErr)
	}

	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("publicar bloco: %w", err)
	}
	published = true

	dir, err := os.Open(s.dataDir)
	if err != nil {
		return fmt.Errorf("abrir diretório para sincronização: %w", err)
	}

	syncErr := dir.Sync()
	dirCloseErr := dir.Close()
	if err := errors.Join(syncErr, dirCloseErr); err != nil {
		return fmt.Errorf("persistir publicação do bloco: %w", err)
	}

	b.State = VALID
	return nil
}

func (s *Store) ReadBlock(b *Block) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	path, err := s.blockPath(b)
	if err != nil {
		return nil, err
	}

	if b.State != VALID {
		return nil, fmt.Errorf("bloco não está VALID: %s", b.BlockID)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("ler bloco: %w", err)
	}

	if err := validateContent(b, data); err != nil {
		b.State = CORRUPT
		return nil, fmt.Errorf("bloco %s corrompido: %w", b.BlockID, err)
	}

	return data, nil
}
