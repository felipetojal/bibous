package master

import (
	"errors"
	"path"
	"strings"
	"time"
)

const MaxBlockSize = 4 * 1024 * 1024

type BlockSpec struct {
	Size uint64 `json:"size"`
	Hash string `json:"hash"`
}

type BlockMetadata struct {
	ID          string   `json:"id"`
	Version     int64    `json:"version"`
	Length      uint64   `json:"length"`
	Hash        string   `json:"hash"`
	Replicas    []string `json:"replicas"`
	BadReplicas []string `json:"bad_replicas,omitempty"`
}

type FileMetadata struct {
	Path              string          `json:"path"`
	Size              uint64          `json:"size"`
	Hash              string          `json:"hash"`
	ReplicationFactor int             `json:"replication_factor"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
	Blocks            []BlockMetadata `json:"blocks"`
}

type PendingUpload struct {
	ID        string       `json:"id"`
	File      FileMetadata `json:"file"`
	ExpiresAt time.Time    `json:"expires_at"`
}

type MetadataState struct {
	Files       map[string]FileMetadata  `json:"files"`
	Directories map[string]bool          `json:"directories"`
	Pending     map[string]PendingUpload `json:"pending"`
}

// emptyState inicia o diretório raiz e os mapas de metadados.
func emptyState() MetadataState {
	return MetadataState{
		Files:       make(map[string]FileMetadata),
		Directories: map[string]bool{"/": true},
		Pending:     make(map[string]PendingUpload),
	}
}

// cloneState cria cópias das estruturas para transações seguras.
func cloneState(s MetadataState) MetadataState {
	out := emptyState()
	for k, v := range s.Files {
		v.Blocks = append([]BlockMetadata(nil), v.Blocks...)
		for i := range v.Blocks {
			v.Blocks[i].Replicas = append([]string(nil), v.Blocks[i].Replicas...)
			v.Blocks[i].BadReplicas = append([]string(nil), v.Blocks[i].BadReplicas...)
		}
		out.Files[k] = v
	}
	for k, v := range s.Directories {
		out.Directories[k] = v
	}
	for k, v := range s.Pending {
		v.File.Blocks = append([]BlockMetadata(nil), v.File.Blocks...)
		for i := range v.File.Blocks {
			v.File.Blocks[i].Replicas = append([]string(nil), v.File.Blocks[i].Replicas...)
		}
		out.Pending[k] = v
	}
	return out
}

// validatePath exige caminhos absolutos e normalizados.
func validatePath(p string, directory bool) error {
	if p == "" || !strings.HasPrefix(p, "/") || path.Clean(p) != p || strings.ContainsRune(p, '\x00') {
		return errors.New("path must be absolute and canonical")
	}
	if !directory && p == "/" {
		return errors.New("root is not a file")
	}
	return nil
}
