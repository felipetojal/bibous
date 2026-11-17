package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/felipetojal/bibous/internal/storage"
	pb "github.com/felipetojal/bibous/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Client struct{ Timeout time.Duration }

func (c Client) timeout() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	return 10 * time.Second
}
func (c Client) connect(ctx context.Context, r Replica) (*grpc.ClientConn, pb.StorageServiceClient, error) {
	conn, err := grpc.NewClient(r.Address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(storage.MaxRPCMessageBytes), grpc.MaxCallSendMsgSize(storage.MaxRPCMessageBytes)))
	if err != nil {
		return nil, nil, err
	}
	api := pb.NewStorageServiceClient(conn)
	health, err := api.Heartbeat(ctx, &pb.HeartbeatRequest{Nonce: 42})
	if err == nil && (health.NodeId != r.NodeID || health.Nonce != 42) {
		err = fmt.Errorf("identidade diferente em %s", r.Address)
	}
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, api, nil
}

// Uma mesma operação conserva seu ID durante as repetições.
func (c Client) put(ctx context.Context, r Replica, request *pb.PutRequest) error {
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		call, cancel := context.WithTimeout(ctx, c.timeout())
		conn, api, err := c.connect(call, r)
		if err == nil {
			ack, e := api.PutBlock(call, request)
			err = e
			if err == nil && (ack.NodeId != r.NodeID || ack.BlockId != request.BlockId || ack.Version != request.Version || ack.Size != request.Size || ack.Hash != request.Hash) {
				err = fmt.Errorf("ACK inválido de %s", r.NodeID)
			}
			conn.Close()
		}
		cancel()
		if err == nil {
			return nil
		}
		last = err
		code := status.Code(err)
		if code != codes.Unavailable && code != codes.DeadlineExceeded {
			break
		}
	}
	return fmt.Errorf("réplica %s: %w", r.NodeID, last)
}
func (c Client) Upload(ctx context.Context, input, manifestPath string, nodes []Replica) error {
	if err := validateNodes(nodes); err != nil {
		return err
	}
	lock, err := lockManifest(manifestPath)
	if err != nil {
		return err
	}
	defer lock.Close()
	f, err := os.Open(input)
	if err != nil {
		return err
	}
	defer f.Close()
	before, err := f.Stat()
	if err != nil {
		return err
	}
	if !before.Mode().IsRegular() {
		return fmt.Errorf("entrada precisa ser arquivo regular")
	}
	for _, r := range nodes {
		call, cancel := context.WithTimeout(ctx, c.timeout())
		conn, _, e := c.connect(call, r)
		cancel()
		if e != nil {
			return e
		}
		conn.Close()
	}
	fileID, err := NewID()
	if err != nil {
		return err
	}
	m := &Manifest{FormatVersion: 2, State: "uploading", FileID: fileID, FileName: filepath.Base(input), ChunkSize: storage.MaxBlockBytes, Blocks: []ManifestBlock{}}
	if err = saveManifest(manifestPath, m, true); err != nil {
		return err
	}
	fullHash := sha256.New()
	buffer := make([]byte, storage.MaxBlockBytes)
	for index := 0; ; index++ {
		if err = ctx.Err(); err != nil {
			return err
		}
		n, readErr := io.ReadFull(f, buffer)
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			return readErr
		}
		if n == 0 {
			break
		}
		data := buffer[:n]
		b := ManifestBlock{Index: index, BlockID: fmt.Sprintf("%s-%d", fileID, index), Version: 1, Size: int64(n), Hash: hashBytes(data), Replicas: append([]Replica(nil), nodes...)}
		for i := range b.Replicas {
			b.Replicas[i].Confirmed = false
		}
		m.Blocks = append(m.Blocks, b)
		m.Size += int64(n)
		// Registra TODOS os destinos antes de enviar, inclusive para limpar um Put
		// que teve sucesso no servidor mas perdeu a resposta na rede.
		if err = saveManifest(manifestPath, m, false); err != nil {
			return err
		}
		op, err := NewID()
		if err != nil {
			return err
		}
		request := &pb.PutRequest{OperationId: op, BlockId: b.BlockID, Version: b.Version, Size: b.Size, Hash: b.Hash, Data: data}
		for i, r := range b.Replicas {
			if err = c.put(ctx, r, request); err != nil {
				return fmt.Errorf("upload incompleto; manifesto preservado para delete: %w", err)
			}
			m.Blocks[index].Replicas[i].Confirmed = true
			if err = saveManifest(manifestPath, m, false); err != nil {
				return err
			}
		}
		_, _ = fullHash.Write(data)
		if readErr == io.ErrUnexpectedEOF {
			break
		}
	}
	after, err := f.Stat()
	if err != nil {
		return err
	}
	if before.Size() != after.Size() || before.Size() != m.Size || !before.ModTime().Equal(after.ModTime()) {
		return fmt.Errorf("entrada alterada durante upload; manifesto incompleto preservado")
	}
	m.Hash = hex.EncodeToString(fullHash.Sum(nil))
	m.State = "complete"
	return saveManifest(manifestPath, m, false)
}
func (c Client) fetch(ctx context.Context, b ManifestBlock) ([]byte, error) {
	var failures []error
	for _, r := range b.Replicas {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		call, cancel := context.WithTimeout(ctx, c.timeout())
		conn, api, err := c.connect(call, r)
		var data []byte
		if err == nil {
			response, e := api.GetBlock(call, &pb.GetRequest{BlockId: b.BlockID, Version: b.Version})
			err = e
			if err == nil {
				if response.BlockId != b.BlockID || response.Version != b.Version || response.Size != b.Size || int64(len(response.Data)) != b.Size || response.Hash != b.Hash || hashBytes(response.Data) != b.Hash {
					err = fmt.Errorf("conteúdo ou metadados inválidos")
				} else {
					data = response.Data
				}
			}
			conn.Close()
		}
		cancel()
		if err == nil {
			return data, nil
		}
		log.Printf("tentando próxima réplica: block=%s node=%s erro=%v", b.BlockID, r.NodeID, err)
		failures = append(failures, fmt.Errorf("%s: %w", r.NodeID, err))
	}
	return nil, fmt.Errorf("nenhuma réplica válida para %s: %w", b.BlockID, errors.Join(failures...))
}
func (c Client) Download(ctx context.Context, manifestPath, output string) error {
	m, err := LoadManifest(manifestPath)
	if err != nil {
		return err
	}
	if m.State != "complete" {
		return fmt.Errorf("manifesto não está complete")
	}
	return atomicWrite(output, true, func(w io.Writer) error {
		fullHash := sha256.New()
		var total int64
		for _, b := range m.Blocks {
			data, err := c.fetch(ctx, b)
			if err != nil {
				return err
			}
			n, err := w.Write(data)
			if err != nil {
				return err
			}
			if n != len(data) {
				return io.ErrShortWrite
			}
			_, _ = fullHash.Write(data)
			total += int64(n)
		}
		if total != m.Size || hex.EncodeToString(fullHash.Sum(nil)) != m.Hash {
			return fmt.Errorf("hash/tamanho final do arquivo diferente")
		}
		return nil
	})
}
func (c Client) Delete(ctx context.Context, manifestPath string) error {
	lock, err := lockManifest(manifestPath)
	if err != nil {
		return err
	}
	defer lock.Close()
	m, err := LoadManifest(manifestPath)
	if err != nil {
		return err
	}
	if m.State == "deleted" {
		return nil
	}
	m.State = "deleting"
	if err = saveManifest(manifestPath, m, false); err != nil {
		return err
	}
	var failures []error
	for _, b := range m.Blocks {
		op, e := NewID()
		if e != nil {
			return e
		}
		for _, r := range b.Replicas {
			call, cancel := context.WithTimeout(ctx, c.timeout())
			conn, api, e := c.connect(call, r)
			if e == nil {
				ack, deleteErr := api.DeleteBlock(call, &pb.DeleteRequest{OperationId: op, BlockId: b.BlockID, Version: b.Version})
				e = deleteErr
				if e == nil && ack.NodeId != r.NodeID {
					e = fmt.Errorf("ACK inválido")
				}
				conn.Close()
			}
			cancel()
			if e != nil {
				failures = append(failures, fmt.Errorf("%s/%s: %w", r.NodeID, b.BlockID, e))
			}
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("remoção pendente; repita delete quando os nós voltarem: %w", errors.Join(failures...))
	}
	m.State = "deleted"
	return saveManifest(manifestPath, m, false)
}

// Repara uma das localizações existentes. Não altera o manifesto nem transfere
// os bytes através do CLI; pede que uma origem válida os envie ao destino.
func (c Client) Repair(ctx context.Context, manifestPath, targetNodeID string) error {
	m, err := LoadManifest(manifestPath)
	if err != nil {
		return err
	}
	if m.State != "complete" {
		return fmt.Errorf("manifesto não está complete")
	}
	for _, b := range m.Blocks {
		exists := false
		for _, r := range b.Replicas {
			if r.NodeID == targetNodeID {
				exists = true
			}
		}
		if !exists {
			return fmt.Errorf("destino %s não consta no manifesto", targetNodeID)
		}
		op, err := NewID()
		if err != nil {
			return err
		}
		var failures []error
		copied := false
		for _, source := range b.Replicas {
			if source.NodeID == targetNodeID {
				continue
			}
			call, cancel := context.WithTimeout(ctx, c.timeout())
			conn, api, e := c.connect(call, source)
			if e == nil {
				ack, copyErr := api.CopyBlock(call, &pb.CopyRequest{OperationId: op, BlockId: b.BlockID, Version: b.Version, TargetNodeId: targetNodeID})
				e = copyErr
				if e == nil && (ack.NodeId != targetNodeID || ack.BlockId != b.BlockID || ack.Version != b.Version || ack.Size != b.Size || ack.Hash != b.Hash) {
					e = fmt.Errorf("ACK da cópia inválido")
				}
				conn.Close()
			}
			cancel()
			if e == nil {
				copied = true
				break
			}
			failures = append(failures, e)
		}
		if !copied {
			return fmt.Errorf("não foi possível reparar %s: %w", b.BlockID, errors.Join(failures...))
		}
	}
	return nil
}
func (c Client) Status(ctx context.Context, node Replica) (*pb.StatusResponse, error) {
	call, cancel := context.WithTimeout(ctx, c.timeout())
	defer cancel()
	conn, api, err := c.connect(call, node)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return api.Status(call, &pb.StatusRequest{})
}
func (c Client) Scrub(ctx context.Context, node Replica) (*pb.ScrubResponse, error) {
	op, err := NewID()
	if err != nil {
		return nil, err
	}
	call, cancel := context.WithTimeout(ctx, c.timeout())
	defer cancel()
	conn, api, err := c.connect(call, node)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return api.Scrub(call, &pb.ScrubRequest{OperationId: op})
}
