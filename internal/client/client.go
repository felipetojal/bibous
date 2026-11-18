// Package client implementa o lado cliente do Bibous: divide arquivos em
// blocos, consulta o mestre para saber onde gravar/ler cada bloco e
// transfere os bytes diretamente com os nós de armazenamento.
package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pb "github.com/felipetojal/bibous/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// MaxBlockSize é o maior bloco aceito pelo StorageService.PutBlock.
	MaxBlockSize = 4 * 1024 * 1024
	// DefaultBlockSize é usado quando Config.BlockSize é zero.
	DefaultBlockSize = MaxBlockSize

	// Folga para os metadados da mensagem gRPC além dos dados do bloco.
	maxMessageBytes = MaxBlockSize + 64*1024
)

// Config controla o comportamento do cliente.
type Config struct {
	// MasterAddr é o host:porta do ClientService no mestre.
	MasterAddr string

	// BlockSize é o tamanho dos blocos no upload (padrão: 4 MiB).
	BlockSize int64

	// ReplicationFactor pedido ao mestre (0 = padrão do mestre).
	ReplicationFactor uint32

	// Parallelism é o número de blocos transferidos ao mesmo tempo.
	Parallelism int

	// MinReplicas é o mínimo de réplicas que precisam confirmar cada bloco
	// para o upload ser aceito (padrão: 1). O mestre recebe no commit a lista
	// das réplicas que confirmaram e pode re-replicar as que faltaram.
	MinReplicas int

	// RPCTimeout é o limite de cada chamada a um nó ou ao mestre.
	RPCTimeout time.Duration

	// Retries é o número de tentativas extras por réplica em falhas
	// transitórias (Unavailable, DeadlineExceeded...).
	Retries int

	// Progress, se não for nil, é chamado a cada bloco concluído.
	Progress func(done, total int)

	// Chave AES-256 (32 bytes). Se vazia, criptografia desligada.
	EncryptionKey []byte
}

func (c *Config) setDefaults() {
	if c.BlockSize == 0 {
		c.BlockSize = DefaultBlockSize
		if len(c.EncryptionKey) > 0 {
			c.BlockSize -= gcmOverhead
		}
	}
	if c.Parallelism <= 0 {
		c.Parallelism = 4
	}
	if c.MinReplicas <= 0 {
		c.MinReplicas = 1
	}
	if c.RPCTimeout <= 0 {
		c.RPCTimeout = 30 * time.Second
	}
	if c.Retries < 0 {
		c.Retries = 0
	}
}

func (c *Config) validate() error {
	if c.MasterAddr == "" {
		return errors.New("endereço do mestre não informado")
	}
	if len(c.EncryptionKey) > 0 && len(c.EncryptionKey) != 32 {
		return errors.New("a chave de criptografia deve ter exatamente 32 bytes")
	}

	maxAllowed := int64(MaxBlockSize)
	if len(c.EncryptionKey) > 0 {
		maxAllowed -= gcmOverhead
	}
	if c.BlockSize < 1 || c.BlockSize > maxAllowed {
		return fmt.Errorf("block size deve estar entre 1 e %d bytes", maxAllowed)
	}
	return nil
}

// Client conversa com o mestre e com os nós de armazenamento.
// É seguro para uso concorrente.
type Client struct {
	cfg    Config
	master pb.ClientServiceClient

	masterConn *grpc.ClientConn

	mu    sync.Mutex
	nodes map[string]*grpc.ClientConn // address -> conexão
}

// New cria um cliente. As conexões gRPC são preguiçosas: nada é
// contatado até a primeira operação.
func New(cfg Config) (*Client, error) {
	cfg.setDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	conn, err := dial(cfg.MasterAddr)
	if err != nil {
		return nil, fmt.Errorf("conectar ao mestre %s: %w", cfg.MasterAddr, err)
	}

	return &Client{
		cfg:        cfg,
		master:     pb.NewClientServiceClient(conn),
		masterConn: conn,
		nodes:      make(map[string]*grpc.ClientConn),
	}, nil
}

// Close encerra todas as conexões abertas.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	errs := []error{c.masterConn.Close()}
	for addr, conn := range c.nodes {
		errs = append(errs, conn.Close())
		delete(c.nodes, addr)
	}
	return errors.Join(errs...)
}

func dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessageBytes),
			grpc.MaxCallSendMsgSize(maxMessageBytes),
		),
	)
}

// storage devolve (e reaproveita) a conexão com um nó.
func (c *Client) storage(addr string) (pb.StorageServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, ok := c.nodes[addr]; ok {
		return pb.NewStorageServiceClient(conn), nil
	}

	conn, err := dial(addr)
	if err != nil {
		return nil, fmt.Errorf("conectar ao nó %s: %w", addr, err)
	}
	c.nodes[addr] = conn
	return pb.NewStorageServiceClient(conn), nil
}

func (c *Client) rpcContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.cfg.RPCTimeout)
}

// List devolve os arquivos cujo caminho começa com prefix.
func (c *Client) List(ctx context.Context, prefix string) ([]*pb.FileInfo, error) {
	ctx, cancel := c.rpcContext(ctx)
	defer cancel()

	resp, err := c.master.ListFiles(ctx, &pb.ListFilesRequest{Prefix: prefix})
	if err != nil {
		return nil, fmt.Errorf("listar arquivos: %w", err)
	}
	return resp.GetFiles(), nil
}

// Stat devolve os metadados e a localização dos blocos de um arquivo.
func (c *Client) Stat(ctx context.Context, path string) (*pb.GetFileResponse, error) {
	ctx, cancel := c.rpcContext(ctx)
	defer cancel()

	resp, err := c.master.GetFile(ctx, &pb.GetFileRequest{Path: path})
	if err != nil {
		return nil, fmt.Errorf("consultar %s: %w", path, err)
	}
	return resp, nil
}

// Delete pede ao mestre a remoção do arquivo. A remoção dos blocos nos
// nós é responsabilidade do mestre.
func (c *Client) Delete(ctx context.Context, path string) error {
	ctx, cancel := c.rpcContext(ctx)
	defer cancel()

	resp, err := c.master.DeleteFile(ctx, &pb.DeleteFileRequest{Path: path})
	if err != nil {
		return fmt.Errorf("remover %s: %w", path, err)
	}
	if !resp.GetDeleted() {
		return fmt.Errorf("remover %s: mestre não confirmou a remoção", path)
	}
	return nil
}

// Move renomeia ou move um arquivo ou diretório. Só altera metadados no
// mestre: nenhum bloco é copiado.
func (c *Client) Move(ctx context.Context, from, to string, overwrite bool) (*pb.FileInfo, error) {
	ctx, cancel := c.rpcContext(ctx)
	defer cancel()

	resp, err := c.master.Move(ctx, &pb.MoveRequest{From: from, To: to, Overwrite: overwrite})
	if err != nil {
		return nil, fmt.Errorf("mover %s para %s: %w", from, to, err)
	}
	return resp.GetFile(), nil
}

// MakeDir cria um diretório. Com parents, cria também os intermediários.
func (c *Client) MakeDir(ctx context.Context, path string, parents bool) error {
	ctx, cancel := c.rpcContext(ctx)
	defer cancel()

	if _, err := c.master.MakeDir(ctx, &pb.MakeDirRequest{Path: path, Parents: parents}); err != nil {
		return fmt.Errorf("criar diretório %s: %w", path, err)
	}
	return nil
}

// RemoveDir remove um diretório. Sem recursive, o diretório precisa estar vazio.
func (c *Client) RemoveDir(ctx context.Context, path string, recursive bool) error {
	ctx, cancel := c.rpcContext(ctx)
	defer cancel()

	resp, err := c.master.RemoveDir(ctx, &pb.RemoveDirRequest{Path: path, Recursive: recursive})
	if err != nil {
		return fmt.Errorf("remover diretório %s: %w", path, err)
	}
	if !resp.GetDeleted() {
		return fmt.Errorf("remover diretório %s: mestre não confirmou a remoção", path)
	}
	return nil
}

func (c *Client) ReportCorruption(ctx context.Context, blockID, nodeID string) error {
    ctx, cancel := c.rpcContext(ctx)
    defer cancel()

    _, err := c.master.ReportCorruption(ctx, &pb.ReportCorruptionRequest{
        BlockId: blockID,
        NodeId:  nodeID,
    })
    return err
}
