// Package client implementa o lado cliente do Bibous: divide arquivos em
// blocos, consulta o mestre (MasterService) para saber onde gravar/ler cada
// bloco e transfere os bytes diretamente com os nós de armazenamento.
package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	storagepb "github.com/felipetojal/bibous/proto"
	mpb "github.com/felipetojal/bibous/proto/masterpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	// MaxBlockSize é o maior bloco aceito pelo storage e pelo mestre.
	MaxBlockSize = 4 * 1024 * 1024
	// DefaultBlockSize é usado quando Config.BlockSize é zero.
	DefaultBlockSize = MaxBlockSize

	// Folga para os metadados da mensagem gRPC além dos dados do bloco.
	maxMessageBytes = MaxBlockSize + 64*1024
)

// Config controla o comportamento do cliente.
type Config struct {
	// MasterAddr é o host:porta do MasterService.
	MasterAddr string

	// BlockSize é o tamanho dos blocos (do texto claro) no upload.
	// Padrão: 4 MiB, ou 4 MiB menos a sobrecarga da cifra se houver chave.
	BlockSize int64

	// ReplicationFactor pedido ao mestre (0 = padrão do mestre).
	ReplicationFactor uint32

	// Parallelism é o número de blocos transferidos ao mesmo tempo.
	Parallelism int

	// RPCTimeout é o limite de cada chamada a um nó ou ao mestre.
	RPCTimeout time.Duration

	// CommitTimeout é o limite do CommitUpload, em que o mestre relê
	// todas as réplicas para conferir os hashes (padrão: 2 minutos).
	CommitTimeout time.Duration

	// Retries é o número de tentativas extras por réplica em falhas
	// transitórias (Unavailable, DeadlineExceeded...).
	Retries int

	// Progress, se não for nil, é chamado a cada bloco concluído.
	Progress func(done, total int)

	// EncryptionKey é a chave AES-256 (32 bytes). Vazia = sem criptografia.
	// A cifra acontece no cliente: nós e mestre só veem texto cifrado.
	EncryptionKey []byte
}

func (c *Config) encrypted() bool { return len(c.EncryptionKey) > 0 }

func (c *Config) maxPlainBlock() int64 {
	if c.encrypted() {
		return MaxBlockSize - gcmOverhead
	}
	return MaxBlockSize
}

func (c *Config) setDefaults() {
	if c.BlockSize == 0 {
		c.BlockSize = c.maxPlainBlock()
	}
	if c.Parallelism <= 0 {
		c.Parallelism = 4
	}
	if c.RPCTimeout <= 0 {
		c.RPCTimeout = 30 * time.Second
	}
	if c.CommitTimeout <= 0 {
		c.CommitTimeout = 2 * time.Minute
	}
	if c.Retries < 0 {
		c.Retries = 0
	}
}

func (c *Config) validate() error {
	if c.MasterAddr == "" {
		return errors.New("endereço do mestre não informado")
	}
	if c.encrypted() && len(c.EncryptionKey) != 32 {
		return errors.New("a chave de criptografia deve ter exatamente 32 bytes")
	}
	if c.BlockSize < 1 || c.BlockSize > c.maxPlainBlock() {
		return fmt.Errorf("block size deve estar entre 1 e %d bytes", c.maxPlainBlock())
	}
	return nil
}

// Client conversa com o mestre e com os nós de armazenamento.
// É seguro para uso concorrente.
type Client struct {
	cfg    Config
	master mpb.MasterServiceClient

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
		master:     mpb.NewMasterServiceClient(conn),
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
func (c *Client) storage(addr string) (storagepb.StorageServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, ok := c.nodes[addr]; ok {
		return storagepb.NewStorageServiceClient(conn), nil
	}

	conn, err := dial(addr)
	if err != nil {
		return nil, fmt.Errorf("conectar ao nó %s: %w", addr, err)
	}
	c.nodes[addr] = conn
	return storagepb.NewStorageServiceClient(conn), nil
}

func (c *Client) rpcContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.cfg.RPCTimeout)
}
