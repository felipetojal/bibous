package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"

	storagepb "github.com/felipetojal/bibous/proto"
	mpb "github.com/felipetojal/bibous/proto/masterpb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UploadOptions ajusta um upload específico.
type UploadOptions struct {
	// Overwrite substitui o arquivo se o caminho remoto já existir.
	Overwrite bool
}

// Upload envia o arquivo local localPath para remotePath.
//
//  1. Divide o arquivo em blocos (cifrando, se houver chave) e calcula os hashes.
//  2. PrepareUpload: o mestre reserva os nós de cada bloco.
//  3. Envia cada bloco diretamente a todas as réplicas (StorageService.PutBlock).
//  4. CommitUpload: o mestre relê as réplicas, confere os hashes e publica.
//
// Em caso de falha, o upload é abortado no mestre e os blocos já gravados
// são apagados dos nós (melhor esforço).
func (c *Client) Upload(ctx context.Context, localPath, remotePath string, opts UploadOptions) (*mpb.FileInfo, error) {
	if !opts.Overwrite {
		return c.upload(ctx, localPath, remotePath)
	}

	// Sobrescrita: grava num caminho temporário no mesmo diretório e só
	// então troca, para nunca ficar sem nenhuma versão íntegra.
	if _, err := c.Stat(ctx, remotePath); err != nil {
		if status.Code(err) == codes.NotFound {
			return c.upload(ctx, localPath, remotePath)
		}
		return nil, err
	}

	suffix, err := newOperationID()
	if err != nil {
		return nil, err
	}
	tmp := path.Join(path.Dir(remotePath), "."+path.Base(remotePath)+".bibous-"+suffix[:12])
	if _, err := c.upload(ctx, localPath, tmp); err != nil {
		return nil, err
	}
	if err := c.Delete(ctx, remotePath); err != nil {
		_ = c.Delete(ctx, tmp)
		return nil, fmt.Errorf("substituir %s: %w", remotePath, err)
	}
	if err := c.moveFile(ctx, tmp, remotePath); err != nil {
		return nil, fmt.Errorf("substituir %s (nova versão ficou em %s): %w", remotePath, tmp, err)
	}
	st, err := c.Stat(ctx, remotePath)
	if err != nil {
		return nil, err
	}
	return st, nil
}

func (c *Client) upload(ctx context.Context, localPath, remotePath string) (*mpb.FileInfo, error) {
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

	specs, size, fileHash, err := c.splitFile(f)
	if err != nil {
		return nil, err
	}

	uploadID, plan, err := c.prepareUpload(ctx, remotePath, size, fileHash, specs)
	if err != nil {
		return nil, err
	}

	err = forEach(ctx, len(plan), c.cfg.Parallelism, c.cfg.Progress,
		func(ctx context.Context, i int) error {
			data, err := c.readBlock(f, i)
			if err != nil {
				return err
			}
			if sha256Hex(data) != plan[i].GetSha256() {
				return fmt.Errorf("bloco %d mudou desde a divisão: o arquivo foi alterado durante o upload", i)
			}
			return c.putReplicas(ctx, i, plan[i], data)
		})
	if err == nil {
		commitCtx, cancel := context.WithTimeout(ctx, c.cfg.CommitTimeout)
		var resp *mpb.CommitUploadResponse
		resp, err = c.master.CommitUpload(commitCtx, &mpb.CommitUploadRequest{UploadId: uploadID})
		cancel()
		if err == nil {
			return resp.GetFile(), nil
		}
		err = fmt.Errorf("confirmar upload no mestre: %w", err)
	}

	// Falhou: libera a reserva e apaga o que já foi gravado.
	cleanup, cancel := context.WithTimeout(context.Background(), c.cfg.RPCTimeout)
	defer cancel()
	_, _ = c.master.AbortUpload(cleanup, &mpb.AbortUploadRequest{UploadId: uploadID})
	c.deleteBlocks(cleanup, plan)
	return nil, fmt.Errorf("upload de %s: %w", localPath, err)
}

// readBlock relê o bloco i do arquivo local e o cifra, se for o caso.
func (c *Client) readBlock(f *os.File, i int) ([]byte, error) {
	plain := make([]byte, c.cfg.BlockSize)
	n, err := f.ReadAt(plain, int64(i)*c.cfg.BlockSize)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("ler bloco %d: %w", i, err)
	}
	return c.encodeBlock(uint32(i), plain[:n])
}

// prepareUpload pede o plano ao mestre e devolve os blocos validados, na
// ordem do arquivo.
func (c *Client) prepareUpload(ctx context.Context, remotePath string, size int64, fileHash string, specs []blockSpec) (string, []*mpb.BlockPlacement, error) {
	req := &mpb.PrepareUploadRequest{
		Path:              remotePath,
		Size:              uint64(size),
		Sha256:            fileHash,
		ReplicationFactor: c.cfg.ReplicationFactor,
	}
	for _, s := range specs {
		req.Blocks = append(req.Blocks, &mpb.BlockSpec{Size: uint64(s.size), Sha256: s.hash})
	}

	reqCtx, cancel := c.rpcContext(ctx)
	defer cancel()
	resp, err := c.master.PrepareUpload(reqCtx, req)
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return "", nil, fmt.Errorf("%s já existe (use put -f para sobrescrever): %w", remotePath, err)
		}
		if status.Code(err) == codes.NotFound {
			return "", nil, fmt.Errorf("diretório %s não existe (crie com mkdir -p): %w", path.Dir(remotePath), err)
		}
		return "", nil, fmt.Errorf("preparar upload no mestre: %w", err)
	}

	if resp.GetUploadId() == "" {
		return "", nil, errors.New("mestre não devolveu upload_id")
	}
	if len(resp.GetBlocks()) != len(specs) {
		return "", nil, fmt.Errorf("mestre devolveu %d blocos, esperado %d", len(resp.GetBlocks()), len(specs))
	}
	for i, b := range resp.GetBlocks() {
		if err := validatePlacement(i, b); err != nil {
			return "", nil, err
		}
		if int64(b.GetSize()) != specs[i].size || b.GetSha256() != specs[i].hash {
			return "", nil, fmt.Errorf("bloco %d: tamanho/hash diferentes do enviado", i)
		}
	}
	return resp.GetUploadId(), resp.GetBlocks(), nil
}

// putReplicas envia o bloco a todas as réplicas em paralelo. O mestre só
// aceita o commit se todas tiverem o bloco, então qualquer falha é fatal.
func (c *Client) putReplicas(ctx context.Context, i int, block *mpb.BlockPlacement, data []byte) error {
	replicas := uniqueReplicas(block.GetReplicas())
	errs := make([]error, len(replicas))
	done := make(chan struct{})
	for k, replica := range replicas {
		go func(k int, replica *mpb.NodeEndpoint) {
			defer func() { done <- struct{}{} }()
			if err := c.putBlock(ctx, replica, block, data); err != nil {
				errs[k] = fmt.Errorf("nó %s (%s): %w", replica.GetNodeId(), replica.GetAddress(), err)
			}
		}(k, replica)
	}
	for range replicas {
		<-done
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("bloco %d: %w", i, err)
	}
	return nil
}

func (c *Client) putBlock(ctx context.Context, replica *mpb.NodeEndpoint, block *mpb.BlockPlacement, data []byte) error {
	node, err := c.storage(replica.GetAddress())
	if err != nil {
		return err
	}
	operationID, err := newOperationID()
	if err != nil {
		return err
	}

	req := &storagepb.PutRequest{
		OperationId: operationID,
		BlockId:     block.GetBlockId(),
		Version:     block.GetVersion(),
		Size:        int64(len(data)),
		Hash:        block.GetSha256(),
		Data:        data,
	}

	var ack *storagepb.PutResponse
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
	if ack.GetBlockId() != req.GetBlockId() || ack.GetVersion() != req.GetVersion() ||
		ack.GetSize() != req.GetSize() || ack.GetHash() != req.GetHash() {
		return errors.New("confirmação (ACK) não corresponde ao bloco enviado")
	}
	return nil
}

// deleteBlocks apaga os blocos de todas as réplicas, ignorando erros
// (o bloco pode nem ter chegado a ser gravado).
func (c *Client) deleteBlocks(ctx context.Context, blocks []*mpb.BlockPlacement) int {
	deleted := 0
	for _, b := range blocks {
		for _, r := range b.GetReplicas() {
			if r.GetAddress() == "" {
				continue
			}
			node, err := c.storage(r.GetAddress())
			if err != nil {
				continue
			}
			op, err := newOperationID()
			if err != nil {
				continue
			}
			callCtx, cancel := c.rpcContext(ctx)
			_, err = node.DeleteBlock(callCtx, &storagepb.DeleteRequest{OperationId: op, BlockId: b.GetBlockId(), Version: b.GetVersion()})
			cancel()
			if err == nil {
				deleted++
			}
		}
	}
	return deleted
}

func newOperationID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("gerar operation_id: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}
