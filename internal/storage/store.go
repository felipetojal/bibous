package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
)

var (
	ErrNotFound    = errors.New("bloco não encontrado")
	ErrConflict    = errors.New("identidade já possui outro conteúdo")
	ErrCorrupt     = errors.New("réplica inválida")
	ErrDeleted     = errors.New("bloco removido")
	ErrUnavailable = errors.New("storage indisponível")
)

const MaxBlockBytes = 4 * 1024 * 1024
const MaxRPCMessageBytes = MaxBlockBytes + 64*1024

type BlockKey struct {
	BlockID string
	Version int
}
type Store struct {
	DataDir        string
	blocks         map[BlockKey]Block
	persistenceErr error
	mu             sync.Mutex
	lockFile       *os.File
	closed         bool
	// Apenas testes internos usam este ponto de falha.
	failpoint func(string) error
}

// Um único processo por diretório. Implementação destinada ao Linux.
func NewStore(dataDir string) (*Store, error) {
	if dataDir == "" {
		return nil, fmt.Errorf("diretório de dados vazio")
	}
	abs, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(abs, 0750); err != nil {
		return nil, err
	}
	for _, dir := range []string{abs, filepath.Join(abs, "tmp"), filepath.Join(abs, "quarantine")} {
		if err = os.MkdirAll(dir, 0750); err != nil {
			return nil, err
		}
		info, e := os.Lstat(dir)
		if e != nil {
			return nil, e
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("diretório inválido: %s", dir)
		}
	}
	var rootStat syscall.Stat_t
	if err = syscall.Stat(abs, &rootStat); err != nil {
		return nil, err
	}
	for _, name := range []string{"tmp", "quarantine"} {
		var subStat syscall.Stat_t
		if err = syscall.Stat(filepath.Join(abs, name), &subStat); err != nil {
			return nil, err
		}
		if rootStat.Dev != subStat.Dev {
			return nil, fmt.Errorf("%s deve estar no mesmo sistema de arquivos de DATA_DIR", name)
		}
	}
	fd, err := syscall.Open(filepath.Join(abs, ".lock"), syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	lock := os.NewFile(uintptr(fd), "storage-lock")
	if err = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("DATA_DIR já está em uso: %w", err)
	}
	s := &Store{DataDir: abs, blocks: make(map[BlockKey]Block), lockFile: lock}
	if err = s.loadSnapshot(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.lockFile.Close() // Libera flock, inclusive na morte do processo.
}
func (s *Store) ready() error {
	if s.closed {
		return ErrUnavailable
	}
	if s.persistenceErr != nil {
		return fmt.Errorf("%w: reinicie após corrigir a causa: %v", ErrUnavailable, s.persistenceErr)
	}
	return nil
}
func (s *Store) trip(stage string) error {
	if s.failpoint != nil {
		return s.failpoint(stage)
	}
	return nil
}
func (s *Store) blockPath(b *Block) (string, error) {
	if b == nil || !blockIDPattern.MatchString(b.BlockID) || b.Version < 1 {
		return "", fmt.Errorf("identidade de bloco inválida")
	}
	return filepath.Join(s.DataDir, b.BlockID+"-v"+strconv.Itoa(b.Version)+".blk"), nil
}
func validateContent(b *Block, data []byte) error {
	if int64(len(data)) != b.Size {
		return fmt.Errorf("tamanho diferente")
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != b.Hash {
		return fmt.Errorf("SHA-256 diferente")
	}
	return nil
}
func validateMetadata(b Block) error {
	if !blockIDPattern.MatchString(b.BlockID) || b.Version < 1 || b.Size < 0 || b.Size > MaxBlockBytes {
		return fmt.Errorf("metadados inválidos")
	}
	hash, err := hex.DecodeString(b.Hash)
	if err != nil || len(hash) != sha256.Size || hex.EncodeToString(hash) != b.Hash {
		return fmt.Errorf("SHA-256 inválido")
	}
	return nil
}
func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

// Limita memória e não segue links simbólicos nos arquivos de blocos.
func readContent(path string) ([]byte, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > MaxBlockBytes {
		return nil, fmt.Errorf("arquivo inválido ou grande demais")
	}
	data, err := io.ReadAll(io.LimitReader(f, MaxBlockBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > MaxBlockBytes {
		return nil, fmt.Errorf("bloco grande demais")
	}
	return data, nil
}

func (s *Store) SaveBlock(b *Block, data []byte) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err = s.ready(); err != nil {
		return err
	}
	path, err := s.blockPath(b)
	if err != nil {
		return err
	}
	if err = validateMetadata(*b); err != nil {
		return err
	}
	if err = validateContent(b, data); err != nil {
		return err
	}
	key := BlockKey{b.BlockID, b.Version}
	if old, ok := s.blocks[key]; ok {
		if old.State == DELETING || old.State == DELETED {
			return ErrDeleted
		}
		if old.Size != b.Size || old.Hash != b.Hash {
			return ErrConflict
		}
		if old.State == VALID {
			stored, e := readContent(path)
			if e == nil && validateContent(&old, stored) == nil {
				b.State = VALID
				return nil
			}
		}
		// Mesma identidade e mesmo hash podem reparar CORRUPT/WRITING.
	}
	// Snapshot com intenção durável ANTES de publicar os bytes.
	intent := *b
	intent.State = WRITING
	if err = s.persistBlockLocked(intent); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			s.persistenceErr = err
		}
	}()
	if err = s.trip("put-intent"); err != nil {
		return err
	}
	tmpDir := filepath.Join(s.DataDir, "tmp")
	f, err := os.CreateTemp(tmpDir, "upload-*")
	if err != nil {
		return err
	}
	temp := f.Name()
	closed := false
	defer func() {
		if !closed {
			_ = f.Close()
		}
		_ = os.Remove(temp)
	}()
	n, err := f.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	if err = f.Sync(); err != nil {
		return err
	}
	err = f.Close()
	closed = true
	if err != nil {
		return err
	}
	if err = os.Rename(temp, path); err != nil {
		return err
	}
	if err = syncDir(tmpDir); err != nil {
		return err
	}
	if err = syncDir(s.DataDir); err != nil {
		return err
	}
	if err = s.trip("put-published"); err != nil {
		return err
	}
	intent.State = VALID
	if err = s.persistBlockLocked(intent); err != nil {
		return err
	}
	b.State = VALID
	return nil
}

func (s *Store) ReadBlock(id string, version int) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(); err != nil {
		return nil, err
	}
	b, ok := s.blocks[BlockKey{id, version}]
	if !ok {
		return nil, ErrNotFound
	}
	if b.State == DELETED || b.State == DELETING {
		return nil, ErrDeleted
	}
	if b.State != VALID {
		return nil, ErrCorrupt
	}
	path, err := s.blockPath(&b)
	if err != nil {
		return nil, err
	}
	data, err := readContent(path)
	if err == nil {
		err = validateContent(&b, data)
	}
	if err != nil {
		b.State = CORRUPT
		return nil, errors.Join(fmt.Errorf("%w: %v", ErrCorrupt, err), s.persistBlockLocked(b))
	}
	return data, nil
}

func (s *Store) DeleteBlock(id string, version int) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err = s.ready(); err != nil {
		return err
	}
	b := Block{BlockID: id, Version: version}
	path, err := s.blockPath(&b)
	if err != nil {
		return err
	}
	if old, ok := s.blocks[BlockKey{id, version}]; ok {
		b = old
		if b.State == DELETED {
			return nil
		}
	}
	// Mesmo ausente ganha tombstone, bloqueando um Put atrasado.
	b.State = DELETING
	if err = s.persistBlockLocked(b); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			s.persistenceErr = err
		}
	}()
	if err = s.trip("delete-intent"); err != nil {
		return err
	}
	if err = os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err = syncDir(s.DataDir); err != nil {
		return err
	}
	if err = s.trip("delete-unlinked"); err != nil {
		return err
	}
	b.State = DELETED
	return s.persistBlockLocked(b)
}

type ScrubResult struct{ Checked, Corrupt uint64 }

func (s *Store) Scrub() (ScrubResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result ScrubResult
	if err := s.ready(); err != nil {
		return result, err
	}
	next := s.cloneBlocks()
	changed := false
	for key, b := range next {
		if b.State != VALID && b.State != CORRUPT {
			continue
		}
		result.Checked++
		path, _ := s.blockPath(&b)
		data, err := readContent(path)
		if err == nil {
			err = validateContent(&b, data)
		}
		// CORRUPT só volta a VALID por Put verificado ou recuperação de WRITING.
		if err != nil && b.State == VALID {
			b.State = CORRUPT
			next[key] = b
			changed = true
		}
		if b.State == CORRUPT {
			result.Corrupt++
		}
	}
	if changed {
		return result, s.commitLocked(next)
	}
	return result, nil
}

type StoreStatus struct {
	Valid, Corrupt, Writing, Deleting, Deleted, ValidBytes, FreeBytes uint64
	Writable                                                          bool
	Error                                                             string
}

func (s *Store) Status() StoreStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := StoreStatus{Writable: s.ready() == nil}
	if err := s.ready(); err != nil {
		r.Error = err.Error()
	}
	for _, b := range s.blocks {
		switch b.State {
		case VALID:
			r.Valid++
			r.ValidBytes += uint64(b.Size)
		case CORRUPT:
			r.Corrupt++
		case WRITING:
			r.Writing++
		case DELETING:
			r.Deleting++
		case DELETED:
			r.Deleted++
		}
	}
	var stat syscall.Statfs_t
	if err := syscall.Statfs(s.DataDir, &stat); err != nil {
		r.Writable = false
		r.Error = err.Error()
	} else {
		r.FreeBytes = stat.Bavail * uint64(stat.Bsize)
	}
	return r
}
