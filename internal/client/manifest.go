package client

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"syscall"

	"github.com/felipetojal/bibous/internal/storage"
)

var idPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,128}$`)

type Replica struct {
	NodeID    string `json:"node_id"`
	Address   string `json:"address"`
	Confirmed bool   `json:"confirmed,omitempty"`
}
type ManifestBlock struct {
	Index    int       `json:"index"`
	BlockID  string    `json:"block_id"`
	Version  int64     `json:"version"`
	Size     int64     `json:"size"`
	Hash     string    `json:"sha256"`
	Replicas []Replica `json:"replicas"`
}
type Manifest struct {
	FormatVersion int             `json:"format_version"`
	State         string          `json:"state"`
	FileID        string          `json:"file_id"`
	FileName      string          `json:"file_name"`
	Size          int64           `json:"size"`
	Hash          string          `json:"sha256"`
	ChunkSize     int             `json:"chunk_size"`
	Blocks        []ManifestBlock `json:"blocks"`
}

func NewID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
func hashBytes(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func validHash(hash string) bool {
	b, e := hex.DecodeString(hash)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == hash
}
func validateNodes(nodes []Replica) error {
	if len(nodes) != 3 {
		return fmt.Errorf("são necessários exatamente três nós")
	}
	ids, addrs := map[string]bool{}, map[string]bool{}
	for _, n := range nodes {
		if !idPattern.MatchString(n.NodeID) || !storage.ValidAddress(n.Address) || ids[n.NodeID] || addrs[n.Address] {
			return fmt.Errorf("nós inválidos ou duplicados")
		}
		ids[n.NodeID] = true
		addrs[n.Address] = true
	}
	return nil
}
func LoadNodes(path string) ([]Replica, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var config struct {
		Nodes []Replica `json:"nodes"`
	}
	if err = json.Unmarshal(data, &config); err != nil {
		return nil, err
	}
	return config.Nodes, validateNodes(config.Nodes)
}
func LoadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err = json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	if m.FormatVersion != 2 || !idPattern.MatchString(m.FileID) || m.Size < 0 || m.ChunkSize != storage.MaxBlockBytes {
		return nil, fmt.Errorf("manifesto inválido ou antigo; esperado formato 2")
	}
	if m.State != "complete" && m.State != "uploading" && m.State != "deleting" && m.State != "deleted" {
		return nil, fmt.Errorf("estado do manifesto inválido")
	}
	if m.State == "complete" && !validHash(m.Hash) {
		return nil, fmt.Errorf("hash do arquivo inválido")
	}
	seen := make(map[string]bool)
	var total int64
	for i, b := range m.Blocks {
		key := fmt.Sprintf("%s/%d", b.BlockID, b.Version)
		if b.Index != i || !idPattern.MatchString(b.BlockID) || b.Version < 1 || b.Size <= 0 || b.Size > storage.MaxBlockBytes || !validHash(b.Hash) || seen[key] {
			return nil, fmt.Errorf("bloco %d inválido", i)
		}
		if i < len(m.Blocks)-1 && b.Size != storage.MaxBlockBytes {
			return nil, fmt.Errorf("bloco intermediário incompleto")
		}
		if err = validateNodes(b.Replicas); err != nil {
			return nil, err
		}
		if m.State == "complete" {
			for _, r := range b.Replicas {
				if !r.Confirmed {
					return nil, fmt.Errorf("réplica não confirmada")
				}
			}
		}
		if b.Size > m.Size-total {
			return nil, fmt.Errorf("tamanho total inválido")
		}
		total += b.Size
		seen[key] = true
	}
	if total != m.Size {
		return nil, fmt.Errorf("soma dos blocos diferente do tamanho")
	}
	return &m, nil
}
func syncDir(path string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	return errors.Join(f.Sync(), f.Close())
}

// noReplace usa link atômico no Linux: nunca sobrescreve um destino concorrente.
func atomicWrite(path string, noReplace bool, write func(io.Writer) error) (err error) {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".client-*")
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
	if err = write(f); err != nil {
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
	if noReplace {
		err = os.Link(temp, path)
	} else {
		err = os.Rename(temp, path)
	}
	if err != nil {
		return err
	}
	if noReplace {
		if err = os.Remove(temp); err != nil {
			return err
		}
	}
	if err = syncDir(dir); err != nil {
		return fmt.Errorf("arquivo publicado, durabilidade incerta: %w", err)
	}
	return nil
}
func saveManifest(path string, m *Manifest, initial bool) error {
	return atomicWrite(path, initial, func(w io.Writer) error { e := json.NewEncoder(w); e.SetIndent("", "  "); return e.Encode(m) })
}

// Serializa put/delete do mesmo manifesto entre processos. O arquivo fica no disco;
// quem define o bloqueio é flock, liberado pelo kernel após uma queda.
func lockManifest(path string) (*os.File, error) {
	fd, err := syscall.Open(path+".lock", syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), "manifest-lock")
	if err = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("manifesto em uso: %w", err)
	}
	return f, nil
}
