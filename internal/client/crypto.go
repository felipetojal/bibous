package client

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

const (
	nonceSize = 12
	tagSize   = 16
	// gcmOverhead é quanto cada bloco cifrado cresce: nonce + tag do GCM.
	gcmOverhead = nonceSize + tagSize
)

// deriveNonce calcula o nonce como HMAC-SHA256(chave, índice || texto claro).
//
// É determinístico (o mesmo bloco gera sempre o mesmo texto cifrado, o que
// permite calcular o hash na divisão e reler o bloco no envio), mas dois
// blocos diferentes nunca compartilham nonce, ao contrário de usar apenas o
// índice do bloco, que repetiria o nonce em todo arquivo cifrado com a
// mesma chave e quebraria a segurança do GCM.
func deriveNonce(key []byte, blockIndex uint32, plaintext []byte) []byte {
	mac := hmac.New(sha256.New, key)
	var idx [4]byte
	binary.BigEndian.PutUint32(idx[:], blockIndex)
	mac.Write(idx[:])
	mac.Write(plaintext)
	return mac.Sum(nil)[:nonceSize]
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// encryptBlock devolve nonce || AES-256-GCM(texto claro). O índice do bloco
// entra como dado autenticado: um bloco trocado de posição não decifra.
func encryptBlock(key []byte, blockIndex uint32, plaintext []byte) ([]byte, error) {
	aead, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	nonce := deriveNonce(key, blockIndex, plaintext)
	var ad [4]byte
	binary.BigEndian.PutUint32(ad[:], blockIndex)

	out := make([]byte, 0, len(plaintext)+gcmOverhead)
	out = append(out, nonce...)
	return aead.Seal(out, nonce, plaintext, ad[:]), nil
}

// decryptBlock desfaz encryptBlock e confere a autenticação.
func decryptBlock(key []byte, blockIndex uint32, data []byte) ([]byte, error) {
	if len(data) < gcmOverhead {
		return nil, errors.New("bloco cifrado curto demais")
	}
	aead, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	var ad [4]byte
	binary.BigEndian.PutUint32(ad[:], blockIndex)

	plain, err := aead.Open(nil, data[:nonceSize], data[nonceSize:], ad[:])
	if err != nil {
		return nil, errors.New("falha ao decifrar (chave errada ou bloco adulterado)")
	}
	return plain, nil
}
