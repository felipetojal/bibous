package client

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
)

const gcmOverhead = 16 // Tamanho da tag de autenticação do GCM

// encryptBlock cifra os dados. Usa o índice do bloco para garantir um Nonce determinístico.
func encryptBlock(key []byte, blockIndex uint32, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// Cria um nonce de 12 bytes baseado no index do bloco
	nonce := make([]byte, aesgcm.NonceSize())
	binary.BigEndian.PutUint32(nonce[8:], blockIndex)

	return aesgcm.Seal(nil, nonce, plaintext, nil), nil
}

// decryptBlock decifra os dados usando a mesma lógica de Nonce.
func decryptBlock(key []byte, blockIndex uint32, ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, aesgcm.NonceSize())
	binary.BigEndian.PutUint32(nonce[8:], blockIndex)

	return aesgcm.Open(nil, nonce, ciphertext, nil)
}
