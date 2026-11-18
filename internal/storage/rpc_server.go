package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log"
	"time"

	pb "github.com/felipetojal/bibous/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type RPCServer struct {
	pb.UnimplementedStorageServiceServer
	store  *Store
	nodeID string
	peers  map[string]string
}

// O argumento opcional preserva as chamadas antigas com dois argumentos.
func NewRPCServer(s *Store, id string, peers ...map[string]string) *RPCServer {
	r := &RPCServer{store: s, nodeID: id, peers: make(map[string]string)}
	if len(peers) > 0 {
		for id, addr := range peers[0] {
			r.peers[id] = addr
		}
	}
	return r
}
func validateRPCIdentity(id string, version int64) (int, error) {
	v := int(version)
	if !blockIDPattern.MatchString(id) || version < 1 || int64(v) != version {
		return 0, status.Error(codes.InvalidArgument, "identidade inválida")
	}
	return v, nil
}
func operationID(id string) error {
	if !blockIDPattern.MatchString(id) {
		return status.Error(codes.InvalidArgument, "operation_id inválido")
	}
	return nil
}
func rpcError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, ErrUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrDeleted):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, ErrCorrupt):
		return status.Error(codes.DataLoss, err.Error())
	default:
		log.Printf("storage erro=%v", err)
		return status.Error(codes.Internal, "erro de persistência; consulte o log")
	}
}
func (s *RPCServer) Heartbeat(ctx context.Context, r *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "requisição ausente")
	}
	return &pb.HeartbeatResponse{NodeId: s.nodeID, Nonce: r.Nonce}, nil
}
func (s *RPCServer) PutBlock(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "requisição ausente")
	}
	v, err := validateRPCIdentity(r.BlockId, r.Version)
	if err != nil {
		return nil, err
	}
	if err = operationID(r.OperationId); err != nil {
		return nil, err
	}
	b := &Block{BlockID: r.BlockId, Version: v, Size: r.Size, Hash: r.Hash, State: WRITING}
	if err = validateMetadata(*b); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err = validateContent(b, r.Data); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	err = s.store.SaveBlock(b, r.Data)
	log.Printf("put node=%s operation=%s block=%s version=%d error=%v", s.nodeID, r.OperationId, r.BlockId, r.Version, err)
	if err != nil {
		return nil, rpcError(err)
	}
	return &pb.PutResponse{NodeId: s.nodeID, BlockId: b.BlockID, Version: r.Version, Size: b.Size, Hash: b.Hash}, nil
}
func (s *RPCServer) GetBlock(ctx context.Context, r *pb.GetRequest) (*pb.GetResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "requisição ausente")
	}
	v, err := validateRPCIdentity(r.BlockId, r.Version)
	if err != nil {
		return nil, err
	}
	data, err := s.store.ReadBlock(r.BlockId, v)
	if err != nil {
		return nil, rpcError(err)
	}
	sum := sha256.Sum256(data)
	return &pb.GetResponse{BlockId: r.BlockId, Version: r.Version, Size: int64(len(data)), Hash: hex.EncodeToString(sum[:]), Data: data}, nil
}
func (s *RPCServer) DeleteBlock(ctx context.Context, r *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "requisição ausente")
	}
	v, err := validateRPCIdentity(r.BlockId, r.Version)
	if err != nil {
		return nil, err
	}
	if err = operationID(r.OperationId); err != nil {
		return nil, err
	}
	err = s.store.DeleteBlock(r.BlockId, v)
	log.Printf("delete node=%s operation=%s block=%s version=%d error=%v", s.nodeID, r.OperationId, r.BlockId, r.Version, err)
	if err != nil {
		return nil, rpcError(err)
	}
	return &pb.DeleteResponse{NodeId: s.nodeID}, nil
}
func (s *RPCServer) Status(ctx context.Context, r *pb.StatusRequest) (*pb.StatusResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	v := s.store.Status()
	return &pb.StatusResponse{NodeId: s.nodeID, ValidBlocks: v.Valid, CorruptBlocks: v.Corrupt, WritingBlocks: v.Writing, DeletingBlocks: v.Deleting, DeletedBlocks: v.Deleted, ValidBytes: v.ValidBytes, FreeBytes: v.FreeBytes, Writable: v.Writable, Error: v.Error}, nil
}
func (s *RPCServer) Scrub(ctx context.Context, r *pb.ScrubRequest) (*pb.ScrubResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "requisição ausente")
	}
	if err := operationID(r.OperationId); err != nil {
		return nil, err
	}
	v, err := s.store.Scrub()
	log.Printf("scrub node=%s operation=%s checked=%d corrupt=%d error=%v", s.nodeID, r.OperationId, v.Checked, v.Corrupt, err)
	if err != nil {
		return nil, rpcError(err)
	}
	return &pb.ScrubResponse{NodeId: s.nodeID, Checked: v.Checked, Corrupt: v.Corrupt}, nil
}
func (s *RPCServer) CopyBlock(ctx context.Context, r *pb.CopyRequest) (*pb.PutResponse, error) {
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "requisição ausente")
	}
	if err := operationID(r.OperationId); err != nil {
		return nil, err
	}
	addr, ok := s.peers[r.TargetNodeId]
	if !ok || r.TargetNodeId == s.nodeID {
		return nil, status.Error(codes.InvalidArgument, "destino ausente em PEERS ou igual à origem")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// GetBlock verifica o inventário e os bytes. Não mantém mutex durante a rede.
	source, err := s.GetBlock(ctx, &pb.GetRequest{BlockId: r.BlockId, Version: r.Version})
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxRPCMessageBytes), grpc.MaxCallSendMsgSize(MaxRPCMessageBytes)))
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	defer conn.Close()
	client := pb.NewStorageServiceClient(conn)
	health, err := client.Heartbeat(ctx, &pb.HeartbeatRequest{Nonce: 1})
	if err != nil {
		return nil, err
	}
	if health.NodeId != r.TargetNodeId || health.Nonce != 1 {
		return nil, status.Error(codes.FailedPrecondition, "identidade do destino diferente")
	}
	ack, err := client.PutBlock(ctx, &pb.PutRequest{OperationId: r.OperationId, BlockId: source.BlockId, Version: source.Version, Size: source.Size, Hash: source.Hash, Data: source.Data})
	if err != nil {
		return nil, err
	}
	if ack.NodeId != r.TargetNodeId || ack.BlockId != source.BlockId || ack.Version != source.Version || ack.Size != source.Size || ack.Hash != source.Hash {
		return nil, status.Error(codes.DataLoss, "ACK do destino inválido")
	}
	log.Printf("copy source=%s target=%s operation=%s block=%s version=%d", s.nodeID, r.TargetNodeId, r.OperationId, r.BlockId, r.Version)
	return ack, nil
}
