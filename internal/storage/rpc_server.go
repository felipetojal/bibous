package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log"

	pb "github.com/felipetojal/bibous/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	MaxBlockBytes   = 4 * 1024 * 1024
	MaxRPCMessageBytes = MaxBlockBytes + 64*1024
)

type RPCServer struct {
	pb.UnimplementedStorageServiceServer

	store  *Store
	nodeID string
}

func NewRPCServer(store *Store, nodeID string) *RPCServer {
	return &RPCServer{
		store:  store,
		nodeID: nodeID,
	}
}

func validateRPCIdentity(id string, version int64) (int, error) {
	if !blockIDPattern.MatchString(id) {
		return 0, status.Error(
			codes.InvalidArgument,
			"block_id inválido",
		)
	}

	localVersion := int(version)

	if version < 1 || int64(localVersion) != version {
		return 0, status.Error(
			codes.InvalidArgument,
			"version deve ser positiva e caber no tipo int",
		)
	}

	return localVersion, nil
}

// Consulta de saúde chamada pelo cliente.
// Não é o heartbeat periódico enviado ao controlador.
func (s *RPCServer) Heartbeat(
	ctx context.Context,
	req *pb.HeartbeatRequest,
) (*pb.HeartbeatResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	if req == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"requisição ausente",
		)
	}

	return &pb.HeartbeatResponse{
		NodeId: s.nodeID,
		Nonce:  req.GetNonce(),
	}, nil
}

func (s *RPCServer) PutBlock(
	ctx context.Context,
	req *pb.PutRequest,
) (*pb.PutResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	if req == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"requisição ausente",
		)
	}

	version, err := validateRPCIdentity(
		req.GetBlockId(),
		req.GetVersion(),
	)
	if err != nil {
		return nil, err
	}

	if !blockIDPattern.MatchString(req.GetOperationId()) {
		return nil, status.Error(
			codes.InvalidArgument,
			"operation_id inválido",
		)
	}

	if req.GetSize() < 0 || req.GetSize() > MaxBlockBytes {
		return nil, status.Error(
			codes.InvalidArgument,
			"size deve estar entre zero e 4 MiB",
		)
	}

	if int64(len(req.GetData())) != req.GetSize() {
		return nil, status.Error(
			codes.InvalidArgument,
			"size diferente da quantidade de bytes recebidos",
		)
	}

	hashBytes, err := hex.DecodeString(req.GetHash())
	if err != nil || len(hashBytes) != sha256.Size {
		return nil, status.Error(
			codes.InvalidArgument,
			"hash deve ser um SHA-256 hexadecimal",
		)
	}

	// Preserva o hash esperado enviado pelo cliente.
	block := &Block{
		BlockID: req.GetBlockId(),
		Version:  version,
		Size:    req.GetSize(),
		Hash:    hex.EncodeToString(hashBytes),
		State:   WRITING,
	}

	// Validação antes de chamar a persistência.
	if err := validateContent(block, req.GetData()); err != nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"conteúdo não corresponde ao tamanho/hash informado",
		)
	}

	log.Printf(
		"put iniciado node=%s operation=%s block=%s version=%d",
		s.nodeID, req.GetOperationId(), block.BlockID, block.Version,
	)

	if err := s.store.SaveBlock(block, req.GetData()); err != nil {
		log.Printf(
			"put falhou node=%s operation=%s block=%s erro=%v",
			s.nodeID, req.GetOperationId(), block.BlockID, err,
		)

		return nil, status.Error(
			codes.Internal,
			"não foi possível persistir o bloco; consulte o log do nó",
		)
	}

	log.Printf(
		"put concluído node=%s operation=%s block=%s",
		s.nodeID, req.GetOperationId(), block.BlockID,
	)

	return &pb.PutResponse{
		NodeId:  s.nodeID,
		BlockId: block.BlockID,
		Version: int64(block.Version),
		Size:    block.Size,
		Hash:    block.Hash,
	}, nil
}

func (s *RPCServer) GetBlock(
	ctx context.Context,
	req *pb.GetRequest,
) (*pb.GetResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	if req == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"requisição ausente",
		)
	}

	version, err := validateRPCIdentity(
		req.GetBlockId(),
		req.GetVersion(),
	)
	if err != nil {
		return nil, err
	}

	// ReadBlock já consulta o inventário, exige VALID
	// e verifica os bytes contra os metadados persistidos.
	data, err := s.store.ReadBlock(req.GetBlockId(), version)
	if err != nil {
		log.Printf(
			"get falhou node=%s block=%s version=%d erro=%v",
			s.nodeID, req.GetBlockId(), version, err,
		)

		return nil, status.Error(
			codes.Internal,
			"não foi possível ler o bloco; consulte o log do nó",
		)
	}

	if len(data) > MaxBlockBytes {
		return nil, status.Error(
			codes.ResourceExhausted,
			"bloco excede o limite desta API",
		)
	}

	// Calculado sobre bytes que o Store já validou.
	sum := sha256.Sum256(data)

	return &pb.GetResponse{
		BlockId: req.GetBlockId(),
		Version: req.GetVersion(),
		Size:    int64(len(data)),
		Hash:    hex.EncodeToString(sum[:]),
		Data:    data,
	}, nil
}
