package master

import "time"

// FileMetadata representa os metadados sobre
// o arquivo armazenado.
type FileMetadata struct {
	id                uint64
	path              string
	size              uint64
	key               [32]byte
	replicationFactor uint8
	createdAt         time.Time
	updatedAt         time.Time
	blocks            []BlockMetadata
}

// BlockMetadata representa os metadados sobre
// bloco que foi armazenado.
type BlockMetadata struct {
	id       uint64
	length   uint64
	key      [32]byte
	replicas []string
}
