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

	storagepb "github.com/felipetojal/bibous/proto"
	mpb "github.com/felipetojal/bibous/proto/masterpb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Download baixa remotePath para o arquivo local localPath.
//
// Cada bloco é lido de uma das réplicas indicadas pelo mestre; se uma
// réplica falhar ou devolver dados com hash errado, a corrupção é
// reportada ao mestre e o cliente tenta a próxima réplica. O arquivo é
// montado num temporário e só substitui localPath no final.
func (c *Client) Download(ctx context.Context, remotePath, localPath string) (*mpb.FileInfo, error) {
	file, err := c.Stat(ctx, remotePath)
	if err != nil {
		return nil, err
	}
	blocks := file.GetBlocks()
	if err := validateFileLayout(file); err != nil {
		return nil, fmt.Errorf("metadados de %s: %w", remotePath, err)
	}

	// Posição de cada bloco no arquivo final (texto claro).
	offsets := make([]int64, len(blocks))
	var plainTotal int64
	for i, b := range blocks {
		offsets[i] = plainTotal
		size := int64(b.GetSize())
		if c.cfg.encrypted() {
			if size < gcmOverhead {
				return nil, fmt.Errorf("bloco %d menor que a sobrecarga da cifra: o arquivo foi enviado sem -key?", i)
			}
			size -= gcmOverhead
		}
		plainTotal += size
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
	if err := tmp.Truncate(plainTotal); err != nil {
		return nil, err
	}

	// Hash do que veio dos nós, bloco a bloco, para conferir o hash do
	// arquivo registrado no mestre (calculado sobre o conteúdo gravado).
	stored := make([]string, len(blocks))

	err = forEach(ctx, len(blocks), c.cfg.Parallelism, c.cfg.Progress,
		func(ctx context.Context, i int) error {
			data, err := c.getFromAnyReplica(ctx, i, blocks[i])
			if err != nil {
				return err
			}
			stored[i] = sha256Hex(data)
			if c.cfg.encrypted() {
				if data, err = decryptBlock(c.cfg.EncryptionKey, uint32(i), data); err != nil {
					return fmt.Errorf("bloco %d: %w", i, err)
				}
			}
			if _, err := tmp.WriteAt(data, offsets[i]); err != nil {
				return fmt.Errorf("gravar bloco %d: %w", i, err)
			}
			return nil
		})
	if err != nil {
		return nil, fmt.Errorf("download de %s: %w", remotePath, err)
	}

	if !c.cfg.encrypted() {
		// Sem cifra, o conteúdo gravado é o próprio arquivo.
		if _, err := tmp.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		h := sha256.New()
		if _, err := io.Copy(h, tmp); err != nil {
			return nil, err
		}
		if got := hex.EncodeToString(h.Sum(nil)); got != file.GetSha256() {
			return nil, fmt.Errorf("hash do arquivo montado (%s) difere do registrado no mestre (%s)", got, file.GetSha256())
		}
	}

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
	return file, nil
}

// validateFileLayout confere se os blocos formam o arquivo inteiro.
func validateFileLayout(f *mpb.FileInfo) error {
	var total uint64
	for i, b := range f.GetBlocks() {
		if err := validatePlacement(i, b); err != nil {
			return err
		}
		total += b.GetSize()
	}
	if total != f.GetSize() {
		return fmt.Errorf("soma dos blocos (%d bytes) difere do tamanho do arquivo (%d bytes)", total, f.GetSize())
	}
	return nil
}

// getFromAnyReplica tenta as réplicas em sequência. A réplica inicial
// varia com o índice do bloco para espalhar a leitura entre os nós.
func (c *Client) getFromAnyReplica(ctx context.Context, i int, block *mpb.BlockPlacement) ([]byte, error) {
	replicas := uniqueReplicas(block.GetReplicas())
	start := i % len(replicas)

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
	return nil, fmt.Errorf("bloco %d (%s): nenhuma réplica disponível: %w", i, block.GetBlockId(), errors.Join(errs...))
}

// errCorrupt marca um bloco que chegou, mas com conteúdo errado.
var errCorrupt = errors.New("hash do bloco não confere com o registrado no mestre")

func (c *Client) getBlock(ctx context.Context, replica *mpb.NodeEndpoint, block *mpb.BlockPlacement) ([]byte, error) {
	node, err := c.storage(replica.GetAddress())
	if err != nil {
		return nil, err
	}

	var resp *storagepb.GetResponse
	err = c.withRetry(ctx, func(ctx context.Context) error {
		var err error
		resp, err = node.GetBlock(ctx, &storagepb.GetRequest{BlockId: block.GetBlockId(), Version: block.GetVersion()})
		return err
	})
	if err != nil {
		// O próprio nó detectou que a cópia local está corrompida.
		if status.Code(err) == codes.DataLoss {
			c.reportCorruption(ctx, replica.GetNodeId(), block)
		}
		return nil, err
	}

	// O hash de referência é o do mestre, não o que o nó informa.
	data := resp.GetData()
	if resp.GetBlockId() != block.GetBlockId() || resp.GetVersion() != block.GetVersion() ||
		uint64(len(data)) != block.GetSize() || sha256Hex(data) != block.GetSha256() {
		c.reportCorruption(ctx, replica.GetNodeId(), block)
		return nil, errCorrupt
	}
	return data, nil
}

// reportCorruption avisa o mestre para descartar a réplica e recriá-la.
func (c *Client) reportCorruption(ctx context.Context, nodeID string, block *mpb.BlockPlacement) {
	callCtx, cancel := c.rpcContext(ctx)
	defer cancel()
	_, _ = c.master.ReportCorruption(callCtx, &mpb.CorruptionReport{
		NodeId: nodeID, BlockId: block.GetBlockId(), Version: block.GetVersion(),
	})
}
