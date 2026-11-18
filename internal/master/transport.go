package master

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	storagepb "github.com/felipetojal/bibous/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCBlockTransport utiliza exclusivamente os RPCs já definidos em StorageService.
type GRPCBlockTransport struct{ Timeout time.Duration }

// NewGRPCBlockTransport define os timeouts de acesso ao armazenamento.
func NewGRPCBlockTransport() *GRPCBlockTransport {
	return &GRPCBlockTransport{Timeout: 12 * time.Second}
}

func (t *GRPCBlockTransport) client(ctx context.Context, n NodeInfo) (*grpc.ClientConn, storagepb.StorageServiceClient, error) {
	conn, err := grpc.NewClient(n.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxBlockSize+65536), grpc.MaxCallSendMsgSize(MaxBlockSize+65536)),
	)
	if err != nil {
		return nil, nil, err
	}
	return conn, storagepb.NewStorageServiceClient(conn), nil
}

// ReadBlock consulta e verifica os bytes obtidos de um nó.
func (t *GRPCBlockTransport) ReadBlock(ctx context.Context, n NodeInfo, b BlockMetadata) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, t.Timeout)
	defer cancel()
	conn, client, err := t.client(ctx, n)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	response, err := client.GetBlock(ctx, &storagepb.GetRequest{BlockId: b.ID, Version: b.Version})
	if err != nil {
		return nil, err
	}
	if response.GetBlockId() != b.ID || response.GetVersion() != b.Version || response.GetSize() != int64(b.Length) || response.GetHash() != b.Hash {
		return nil, errors.New("storage returned mismatched metadata")
	}
	data := response.GetData()
	digest := sha256.Sum256(data)
	if uint64(len(data)) != b.Length || hex.EncodeToString(digest[:]) != b.Hash {
		return nil, errors.New("storage returned corrupted bytes")
	}
	return data, nil
}

// WriteBlock envia uma réplica e valida a confirmação do destino.
func (t *GRPCBlockTransport) WriteBlock(ctx context.Context, n NodeInfo, b BlockMetadata, data []byte) error {
	if uint64(len(data)) != b.Length {
		return errors.New("wrong replica length")
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != b.Hash {
		return errors.New("wrong replica checksum")
	}
	opID, err := newID()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, t.Timeout)
	defer cancel()
	conn, client, err := t.client(ctx, n)
	if err != nil {
		return err
	}
	defer conn.Close()
	response, err := client.PutBlock(ctx, &storagepb.PutRequest{
		OperationId: opID, BlockId: b.ID, Version: b.Version, Size: int64(b.Length), Hash: b.Hash, Data: data,
	})
	if err != nil {
		return err
	}
	if response.GetNodeId() != n.ID || response.GetBlockId() != b.ID || response.GetVersion() != b.Version || response.GetHash() != b.Hash || response.GetSize() != int64(b.Length) {
		return fmt.Errorf("storage %s returned mismatched write acknowledgement", n.ID)
	}
	return nil
}
