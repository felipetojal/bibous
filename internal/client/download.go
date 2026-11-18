package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	pb "github.com/felipetojal/bibous/proto"
)

// Download baixa remotePath para o arquivo local localPath.
//
// Cada bloco é lido de uma das réplicas indicadas pelo mestre; se uma
// réplica falhar ou devolver dados com hash errado, tenta a próxima.
// O arquivo é montado num temporário e só substitui localPath depois que
// o hash do arquivo inteiro confere.
func (c *Client) Download(ctx context.Context, remotePath, localPath string) (*pb.FileInfo, error) {
	meta, err := c.Stat(ctx, remotePath)
	if err != nil {
		return nil, err
	}

	blocks, offsets, err := validateFileLayout(meta)
	if err != nil {
		return nil, fmt.Errorf("metadados de %s: %w", remotePath, err)
	}

	tmp, err := os.CreateTemp(filepath.Dir(localPath), ".bibous-download-*")
	if err != nil {
		return nil, err
	}
	tmpPath := tmp.Name()
	published := false
	defer func() {
		if !published {
			tmp.Close()
			os.Remove(tmpPath)
		}
	}()

	if len(c.cfg.EncryptionKey) == 0 {
		if err := tmp.Truncate(meta.GetFile().GetSize()); err != nil {
			return nil, err
		}
	}

	err = forEach(ctx, len(blocks), c.cfg.Parallelism, c.cfg.Progress,
		func(ctx context.Context, i int) error {
			data, err := c.getFromAnyReplica(ctx, blocks[i])
			if err != nil {
				return err
			}
			writeOffset := offsets[i]
			if len(c.cfg.EncryptionKey) == 32 {
				data, err = decryptBlock(c.cfg.EncryptionKey, blocks[i].GetIndex(), data)
				if err != nil {
					return fmt.Errorf("decifrar bloco %d: %w", i, err)
				}
				writeOffset -= int64(i) * gcmOverhead
			}

			if _, err := tmp.WriteAt(data, writeOffset); err != nil {
				return fmt.Errorf("gravar bloco %d: %w", i, err)
			}
			return nil
		})
	if err != nil {
		return nil, fmt.Errorf("download de %s: %w", remotePath, err)
	}

	if want := meta.GetFile().GetHash(); want != "" && len(c.cfg.EncryptionKey) == 0 {
		if _, err := tmp.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		h := sha256.New()
		if _, err := io.Copy(h, tmp); err != nil {
			return nil, err
		}
		if got := hex.EncodeToString(h.Sum(nil)); got != want {
			return nil, fmt.Errorf("hash do arquivo montado (%s) difere do registrado no mestre (%s)", got, want)
		}
	}

	// CreateTemp cria com 0600; usa a permissão comum de arquivo.
	if err := tmp.Chmod(0o644); err != nil {
		return nil, err
	}
	if err := tmp.Sync(); err != nil {
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(tmpPath, localPath); err != nil {
		return nil, err
	}
	published = true

	return meta.GetFile(), nil
}

// validateFileLayout confere se os blocos formam o arquivo inteiro, sem
// buracos nem sobreposição, e devolve o deslocamento de cada um.
func validateFileLayout(meta *pb.GetFileResponse) ([]*pb.BlockLocation, []int64, error) {
	if meta.GetFile() == nil {
		return nil, nil, errors.New("mestre não devolveu FileInfo")
	}

	blocks := make([]*pb.BlockLocation, len(meta.GetBlocks()))
	for _, b := range meta.GetBlocks() {
		if err := validateLocation(b); err != nil {
			return nil, nil, err
		}
		i := int(b.GetIndex())
		if i >= len(blocks) || blocks[i] != nil {
			return nil, nil, fmt.Errorf("index de bloco inválido ou repetido: %d", i)
		}
		blocks[i] = b
	}

	offsets := make([]int64, len(blocks))
	var total int64
	for i, b := range blocks {
		offsets[i] = total
		total += b.GetSize()
	}

	if total != meta.GetFile().GetSize() {
		return nil, nil, fmt.Errorf(
			"soma dos blocos (%d bytes) difere do tamanho do arquivo (%d bytes)",
			total, meta.GetFile().GetSize(),
		)
	}
	return blocks, offsets, nil
}

// getFromAnyReplica tenta as réplicas em sequência. A réplica inicial
// varia com o index do bloco para espalhar a leitura entre os nós.
func (c *Client) getFromAnyReplica(ctx context.Context, block *pb.BlockLocation) ([]byte, error) {
	replicas := uniqueReplicas(block.GetReplicas())
	start := int(block.GetIndex()) % len(replicas)

	var errs []error
	for k := range replicas {
		replica := replicas[(start+k)%len(replicas)]

		data, err := c.getBlock(ctx, replica, block)
		if err == nil {
			return data, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		errs = append(errs, fmt.Errorf("nó %s (%s): %w", replica.GetNodeId(), replica.GetAddress(), err))
	}

	return nil, fmt.Errorf(
		"bloco %d (%s): nenhuma réplica disponível: %w",
		block.GetIndex(), block.GetBlockId(), errors.Join(errs...),
	)
}

func (c *Client) getBlock(ctx context.Context, replica *pb.NodeLocation, block *pb.BlockLocation) ([]byte, error) {
	node, err := c.storage(replica.GetAddress())
	if err != nil {
		return nil, err
	}

	var resp *pb.GetResponse
	err = c.withRetry(ctx, func(ctx context.Context) error {
		var err error
		resp, err = node.GetBlock(ctx, &pb.GetRequest{
			BlockId: block.GetBlockId(),
			Version: block.GetVersion(),
		})
		return err
	})
	if err != nil {
		return nil, err
	}

	data := resp.GetData()

	// O hash de referência é o do mestre, não o que o nó informa.
	if resp.GetBlockId() != block.GetBlockId() || resp.GetVersion() != block.GetVersion() {
		return nil, errors.New("nó devolveu outro bloco")
	}
	if int64(len(data)) != block.GetSize() {
		return nil, fmt.Errorf("tamanho %d, esperado %d", len(data), block.GetSize())
	}
	if sha256Hex(data) != block.GetHash() {
		_ = c.ReportCorruption(ctx, block.GetBlockId(), replica.GetNodeId())
		return nil, errors.New("hash do bloco não confere com o registrado no mestre")
	}
	return data, nil
}
