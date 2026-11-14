package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
)

type Estado int

const (
	WRITING Estado = iota
	VALID
	CORRUPT
	DELETING
)

type Block struct {
	BlockID string
	Size    int64
	Versao  int
	State   Estado
	Hash    string
}

var blockIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,128}$`)

func CreateBlock(id string, versao int, data []byte) (*Block, error) {
	if !blockIDPattern.MatchString(id) {
		return nil, fmt.Errorf(
			"BlockID inválido: use de 1 a 128 letras, números, '_' ou '-'",
		)
	}

	if versao < 1 {
		return nil, fmt.Errorf("versão inválida: deve ser maior que zero")
	}

	hash := sha256.Sum256(data)

	return &Block{
		BlockID: id,
		Size:    int64(len(data)),
		Versao:  versao,
		State:   WRITING,
		Hash:    hex.EncodeToString(hash[:]),
	}, nil
}
