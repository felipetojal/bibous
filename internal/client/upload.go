package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	pb "github.com/felipetojal/bibous/proto"
)

// UploadOptions ajusta um upload específico.
type UploadOptions struct {
	// Overwrite substitui o arquivo se o caminho remoto já existir.
	Overwrite bool
}

// Upload envia o arquivo local localPath para remotePath.
//
//  1. Divide o arquivo em blocos e calcula os hashes.
//  2. Pede ao mestre (PrepareUpload) o block_id e as réplicas de cada bloco.
//  3. Envia cada bloco diretamente às réplicas (StorageService.PutBlock).
//  4. Confirma ao mestre (CommitUpload) quais réplicas guardaram cada bloco.
func (c *Client) Upload(
	ctx context.Context,
	localPath, remotePath string,
	opts UploadOptions,
) (*pb.FileInfo, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s não é um arquivo regular", localPath)
	}

	specs, size, fileHash, err := splitFile(f, c.cfg.BlockSize)
	if err != nil {
		return nil, err
	}

	uploadID, plan, err := c.prepareUpload(ctx, remotePath, size, fileHash, specs, opts)
	if err != nil {
		return nil, err
	}

	stored := make([]*pb.StoredBlock, len(plan))

	err = forEach(ctx, len(plan), c.cfg.Parallelism, c.cfg.Progress,
		func(ctx context.Context, i int) error {
			block := plan[i]

			data := make([]byte, block.GetSize())
			offset := int64(block.GetIndex()) * c.cfg.BlockSize
			if _, err := f.ReadAt(data, offset); err != nil && !errors.Is(err, io.EOF) {
				return fmt.Errorf("ler bloco %d: %w", block.GetIndex(), err)
			}
			if sha256Hex(data) != block.GetHash() {
				return fmt.Errorf(
					"bloco %d mudou desde a divisão: o arquivo foi alterado durante o upload",
					block.GetIndex(),
				)
			}

			nodes, err := c.putReplicas(ctx, block, data)
			if err != nil {
				return err
			}

			stored[i] = &pb.StoredBlock{
				BlockId: block.GetBlockId(),
				Version: block.GetVersion(),
				NodeIds: nodes,
			}
			return nil
		})
	if err != nil {
		// Sem commit o mestre não publica o arquivo; os blocos já
		// gravados ficam órfãos até o mestre descartar o upload.
		return nil, fmt.Errorf("upload de %s: %w", localPath, err)
	}

	commitCtx, cancel := c.rpcContext(ctx)
	defer cancel()

	resp, err := c.master.CommitUpload(commitCtx, &pb.CommitUploadRequest{
		UploadId: uploadID,
		Blocks:   stored,
	})
	if err != nil {
		return nil, fmt.Errorf("confirmar upload no mestre: %w", err)
	}
	return resp.GetFile(), nil
}

// prepareUpload pede o plano ao mestre e devolve os blocos validados,
// na ordem do arquivo (plan[i].Index == i).
func (c *Client) prepareUpload(
	ctx context.Context,
	remotePath string,
	size int64,
	fileHash string,
	specs []*pb.BlockSpec,
	opts UploadOptions,
) (string, []*pb.BlockLocation, error) {
	reqCtx, cancel := c.rpcContext(ctx)
	defer cancel()

	resp, err := c.master.PrepareUpload(reqCtx, &pb.PrepareUploadRequest{
		Path:              remotePath,
		Size:              size,
		Hash:              fileHash,
		BlockSize:         c.cfg.BlockSize,
		ReplicationFactor: c.cfg.ReplicationFactor,
		Blocks:            specs,
		Overwrite:         opts.Overwrite,
	})
	if err != nil {
		return "", nil, fmt.Errorf("preparar upload no mestre: %w", err)
	}

	if resp.GetUploadId() == "" {
		return "", nil, errors.New("mestre não devolveu upload_id")
	}
	if len(resp.GetBlocks()) != len(specs) {
		return "", nil, fmt.Errorf(
			"mestre devolveu %d blocos, esperado %d",
			len(resp.GetBlocks()), len(specs),
		)
	}

	// O mestre pode responder fora de ordem: reorganiza pelo index.
	plan := make([]*pb.BlockLocation, len(specs))
	for _, b := range resp.GetBlocks() {
		if err := validateLocation(b); err != nil {
			return "", nil, err
		}
		i := int(b.GetIndex())
		if i >= len(specs) || plan[i] != nil {
			return "", nil, fmt.Errorf("mestre devolveu index de bloco inválido: %d", i)
		}
		if b.GetSize() != specs[i].GetSize() || b.GetHash() != specs[i].GetHash() {
			return "", nil, fmt.Errorf("bloco %d: tamanho/hash diferentes do enviado", i)
		}
		b.Replicas = uniqueReplicas(b.GetReplicas())
		if len(b.GetReplicas()) < c.cfg.MinReplicas {
			return "", nil, fmt.Errorf(
				"bloco %d: mestre indicou %d réplica(s), mínimo exigido %d",
				i, len(b.GetReplicas()), c.cfg.MinReplicas,
			)
		}
		plan[i] = b
	}

	return resp.GetUploadId(), plan, nil
}

// putReplicas envia o bloco a todas as réplicas em paralelo e devolve os
// node_ids que confirmaram. Falha se menos de MinReplicas confirmarem.
func (c *Client) putReplicas(ctx context.Context, block *pb.BlockLocation, data []byte) ([]string, error) {
	var (
		mu    sync.Mutex
		wg    sync.WaitGroup
		acked []string
		errs  []error
	)

	for _, replica := range block.GetReplicas() {
		wg.Add(1)
		go func(replica *pb.NodeLocation) {
			defer wg.Done()

			err := c.putBlock(ctx, replica, block, data)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, fmt.Errorf("nó %s (%s): %w", replica.GetNodeId(), replica.GetAddress(), err))
				return
			}
			acked = append(acked, replica.GetNodeId())
		}(replica)
	}
	wg.Wait()

	if len(acked) < c.cfg.MinReplicas {
		return nil, fmt.Errorf(
			"bloco %d: %d de %d réplicas confirmaram (mínimo %d): %w",
			block.GetIndex(), len(acked), len(block.GetReplicas()), c.cfg.MinReplicas,
			errors.Join(errs...),
		)
	}
	return acked, nil
}

func (c *Client) putBlock(ctx context.Context, replica *pb.NodeLocation, block *pb.BlockLocation, data []byte) error {
	node, err := c.storage(replica.GetAddress())
	if err != nil {
		return err
	}

	operationID, err := newOperationID()
	if err != nil {
		return err
	}

	req := &pb.PutRequest{
		OperationId: operationID,
		BlockId:     block.GetBlockId(),
		Version:     block.GetVersion(),
		Size:        int64(len(data)),
		Hash:        block.GetHash(),
		Data:        data,
	}

	var ack *pb.PutResponse
	err = c.withRetry(ctx, func(ctx context.Context) error {
		var err error
		ack, err = node.PutBlock(ctx, req)
		return err
	})
	if err != nil {
		return err
	}

	// Confere se quem respondeu é o nó esperado e guardou o bloco certo.
	if ack.GetNodeId() != replica.GetNodeId() {
		return fmt.Errorf("respondeu como nó %q, esperado %q", ack.GetNodeId(), replica.GetNodeId())
	}
	if ack.GetBlockId() != req.GetBlockId() ||
		ack.GetVersion() != req.GetVersion() ||
		ack.GetSize() != req.GetSize() ||
		ack.GetHash() != req.GetHash() {
		return errors.New("confirmação (ACK) não corresponde ao bloco enviado")
	}
	return nil
}

func newOperationID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("gerar operation_id: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}
