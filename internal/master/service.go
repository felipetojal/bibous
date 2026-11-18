package master

import (
	"context"
	"errors"

	pb "github.com/felipetojal/bibous/proto/masterpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MasterService traduz RPCs recebidos em operações de domínio do mestre.
type MasterService struct {
	pb.UnimplementedMasterServiceServer
	master *Master
}

// NewMasterService conecta as operações RPC ao controlador.
func NewMasterService(master *Master) *MasterService { return &MasterService{master: master} }

func rpcError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, ErrUnavailable):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// RegisterNode recebe o cadastro de um nó de armazenamento.
func (s *MasterService) RegisterNode(ctx context.Context, r *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "missing registration")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	err := s.master.RegisterNode(NodeInfo{ID: r.GetNodeId(), Address: r.GetNodeAddress(), MachineID: r.GetMachineId(), DiskID: r.GetDiskId(), AvailableSize: r.GetAvailableSize(), TotalSize: r.GetTotalSize()})
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &pb.RegisterResponse{NodeId: r.GetNodeId(), Success: true}, nil
}

// Heartbeat registra o sinal de vida e devolve o nonce informado.
func (s *MasterService) Heartbeat(ctx context.Context, r *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "missing heartbeat")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	// O campo opcional diferencia espaço zero da ausência de atualização.
	if err := s.master.Heartbeat(r.GetNodeId(), r.AvailableSize); err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &pb.HeartbeatResponse{NodeId: r.GetNodeId(), Nonce: r.GetNonce()}, nil
}

func (s *MasterService) endpoint(id string) *pb.NodeEndpoint {
	n, ok := s.master.nr.Get(id)
	if !ok {
		return &pb.NodeEndpoint{NodeId: id}
	}
	return &pb.NodeEndpoint{NodeId: id, Address: n.Address, MachineId: n.MachineID, DiskId: n.DiskID}
}

func (s *MasterService) blockInfo(b BlockMetadata) *pb.BlockPlacement {
	out := &pb.BlockPlacement{BlockId: b.ID, Version: b.Version, Size: b.Length, Sha256: b.Hash}
	for _, id := range b.Replicas {
		out.Replicas = append(out.Replicas, s.endpoint(id))
	}
	return out
}

func (s *MasterService) fileInfo(f FileMetadata) *pb.FileInfo {
	out := &pb.FileInfo{Path: f.Path, Size: f.Size, Sha256: f.Hash, ReplicationFactor: uint32(f.ReplicationFactor), CreatedUnix: f.CreatedAt.Unix(), UpdatedUnix: f.UpdatedAt.Unix(), Status: s.master.FileStatus(f)}
	for _, b := range f.Blocks {
		out.Blocks = append(out.Blocks, s.blockInfo(b))
	}
	return out
}

// PrepareUpload devolve as alocações que o cliente deverá preencher.
func (s *MasterService) PrepareUpload(ctx context.Context, r *pb.PrepareUploadRequest) (*pb.PrepareUploadResponse, error) {
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "missing upload")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	specs := make([]BlockSpec, 0, len(r.GetBlocks()))
	for _, b := range r.GetBlocks() {
		if b == nil {
			return nil, status.Error(codes.InvalidArgument, "nil block spec")
		}
		specs = append(specs, BlockSpec{Size: b.GetSize(), Hash: b.GetSha256()})
	}
	p, err := s.master.PrepareUpload(r.GetPath(), r.GetSize(), r.GetSha256(), specs, int(r.GetReplicationFactor()))
	if err != nil {
		return nil, rpcError(err)
	}
	out := &pb.PrepareUploadResponse{UploadId: p.ID, ExpiresUnix: p.ExpiresAt.Unix()}
	for _, b := range p.File.Blocks {
		out.Blocks = append(out.Blocks, s.blockInfo(b))
	}
	return out, nil
}

// CommitUpload solicita a validação e a publicação do arquivo.
func (s *MasterService) CommitUpload(ctx context.Context, r *pb.CommitUploadRequest) (*pb.CommitUploadResponse, error) {
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "missing upload id")
	}
	f, err := s.master.CommitUpload(ctx, r.GetUploadId())
	if err != nil {
		return nil, rpcError(err)
	}
	return &pb.CommitUploadResponse{File: s.fileInfo(f)}, nil
}

// AbortUpload cancela a reserva de upload identificada.
func (s *MasterService) AbortUpload(ctx context.Context, r *pb.AbortUploadRequest) (*pb.SimpleResponse, error) {
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "missing upload id")
	}
	if err := s.master.AbortUpload(r.GetUploadId()); err != nil {
		return nil, rpcError(err)
	}
	return &pb.SimpleResponse{Success: true}, nil
}

// GetFileMetadata retorna os blocos e endereços das réplicas.
func (s *MasterService) GetFileMetadata(ctx context.Context, r *pb.GetFileRequest) (*pb.GetFileResponse, error) {
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "missing path")
	}
	f, err := s.master.GetFile(r.GetPath())
	if err != nil {
		return nil, rpcError(err)
	}
	return &pb.GetFileResponse{File: s.fileInfo(f)}, nil
}

// CreateDirectory cria diretórios por meio do controlador.
func (s *MasterService) CreateDirectory(ctx context.Context, r *pb.DirectoryRequest) (*pb.SimpleResponse, error) {
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "missing path")
	}
	if err := s.master.CreateDirectory(r.GetPath()); err != nil {
		return nil, rpcError(err)
	}
	return &pb.SimpleResponse{Success: true}, nil
}

// DeleteDirectory remove um diretório vazio.
func (s *MasterService) DeleteDirectory(ctx context.Context, r *pb.DirectoryRequest) (*pb.SimpleResponse, error) {
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "missing path")
	}
	if err := s.master.DeleteDirectory(r.GetPath()); err != nil {
		return nil, rpcError(err)
	}
	return &pb.SimpleResponse{Success: true}, nil
}

// ListDirectory retorna o conteúdo imediato do diretório.
func (s *MasterService) ListDirectory(ctx context.Context, r *pb.DirectoryRequest) (*pb.ListDirectoryResponse, error) {
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "missing path")
	}
	dirs, files, err := s.master.ListDirectory(r.GetPath())
	if err != nil {
		return nil, rpcError(err)
	}
	return &pb.ListDirectoryResponse{Directories: dirs, Files: files}, nil
}

// MoveFile renomeia ou move um arquivo lógico.
func (s *MasterService) MoveFile(ctx context.Context, r *pb.MoveFileRequest) (*pb.SimpleResponse, error) {
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "missing paths")
	}
	if err := s.master.MoveFile(r.GetSource(), r.GetDestination()); err != nil {
		return nil, rpcError(err)
	}
	return &pb.SimpleResponse{Success: true}, nil
}

// DeleteFile exclui apenas a referência lógica ao arquivo.
func (s *MasterService) DeleteFile(ctx context.Context, r *pb.FilePathRequest) (*pb.SimpleResponse, error) {
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "missing path")
	}
	if err := s.master.DeleteFile(r.GetPath()); err != nil {
		return nil, rpcError(err)
	}
	return &pb.SimpleResponse{Success: true}, nil
}

// ReportCorruption denuncia uma réplica inválida.
func (s *MasterService) ReportCorruption(ctx context.Context, r *pb.CorruptionReport) (*pb.SimpleResponse, error) {
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "missing corruption report")
	}
	if err := s.master.ReportCorruption(r.GetNodeId(), r.GetBlockId(), r.GetVersion()); err != nil {
		return nil, rpcError(err)
	}
	return &pb.SimpleResponse{Success: true}, nil
}

// GetClusterStatus disponibiliza o resumo de saúde do cluster.
func (s *MasterService) GetClusterStatus(ctx context.Context, r *pb.ClusterStatusRequest) (*pb.ClusterStatusResponse, error) {
	nodes, healthy, under, unavailable, pending := s.master.ClusterStatus()
	out := &pb.ClusterStatusResponse{HealthyFiles: uint64(healthy), UnderReplicatedFiles: uint64(under), UnavailableFiles: uint64(unavailable), PendingUploads: uint64(pending)}
	for _, n := range nodes {
		out.Nodes = append(out.Nodes, &pb.NodeState{NodeId: n.ID, Address: n.Address, MachineId: n.MachineID, DiskId: n.DiskID, AvailableSize: n.AvailableSize, TotalSize: n.TotalSize, LastHeartbeatUnix: n.LastHeartbeat.Unix(), Status: string(n.Status)})
	}
	return out, nil
}

// DrainNode coloca um nó em drenagem planejada.
func (s *MasterService) DrainNode(ctx context.Context, r *pb.DrainNodeRequest) (*pb.SimpleResponse, error) {
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "missing node ID")
	}
	if err := s.master.DrainNode(r.GetNodeId()); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &pb.SimpleResponse{Success: true}, nil
}
