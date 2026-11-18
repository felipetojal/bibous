package client

import (
	"context"
	"fmt"
	"path"
	"strings"

	mpb "github.com/felipetojal/bibous/proto/masterpb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Operações de metadados: tudo aqui fala só com o mestre, exceto a
// remoção de arquivos, que também apaga os blocos nos nós.

// Entry é um item de diretório. Para arquivos, File traz os metadados.
type Entry struct {
	Path  string
	IsDir bool
	File  *mpb.FileInfo
}

// Stat devolve os metadados, o estado e a localização dos blocos.
func (c *Client) Stat(ctx context.Context, p string) (*mpb.FileInfo, error) {
	ctx, cancel := c.rpcContext(ctx)
	defer cancel()

	resp, err := c.master.GetFileMetadata(ctx, &mpb.GetFileRequest{Path: p})
	if err != nil {
		return nil, fmt.Errorf("consultar %s: %w", p, err)
	}
	if resp.GetFile() == nil {
		return nil, fmt.Errorf("consultar %s: mestre não devolveu metadados", p)
	}
	return resp.GetFile(), nil
}

// List devolve o conteúdo imediato de um diretório: primeiro os
// subdiretórios, depois os arquivos (com tamanho e estado).
func (c *Client) List(ctx context.Context, dir string) ([]Entry, error) {
	if dir == "" {
		dir = "/"
	}
	listCtx, cancel := c.rpcContext(ctx)
	resp, err := c.master.ListDirectory(listCtx, &mpb.DirectoryRequest{Path: dir})
	cancel()
	if err != nil {
		return nil, fmt.Errorf("listar %s: %w", dir, err)
	}

	var out []Entry
	for _, d := range resp.GetDirectories() {
		out = append(out, Entry{Path: d, IsDir: true})
	}
	for _, f := range resp.GetFiles() {
		info, err := c.Stat(ctx, f)
		if err != nil {
			// Pode ter sido removido entre as duas chamadas.
			info = &mpb.FileInfo{Path: f}
		}
		out = append(out, Entry{Path: f, File: info})
	}
	return out, nil
}

// Delete remove o arquivo do mestre e depois apaga seus blocos nos nós.
// A remoção no mestre é o que vale; se algum nó estiver fora, seus blocos
// ficam órfãos e o número apagado é informado em blocksDeleted.
func (c *Client) Delete(ctx context.Context, p string) error {
	_, err := c.delete(ctx, p)
	return err
}

func (c *Client) delete(ctx context.Context, p string) (blocksDeleted int, err error) {
	info, err := c.Stat(ctx, p)
	if err != nil {
		return 0, err
	}

	callCtx, cancel := c.rpcContext(ctx)
	_, err = c.master.DeleteFile(callCtx, &mpb.FilePathRequest{Path: p})
	cancel()
	if err != nil {
		return 0, fmt.Errorf("remover %s: %w", p, err)
	}
	return c.deleteBlocks(ctx, info.GetBlocks()), nil
}

// DeleteVerbose é como Delete, mas informa quantas cópias de blocos foram apagadas.
func (c *Client) DeleteVerbose(ctx context.Context, p string) (int, error) {
	return c.delete(ctx, p)
}

// Move renomeia ou move um arquivo. Com overwrite, remove o destino antes.
// O mestre só move arquivos; diretórios não podem ser movidos.
func (c *Client) Move(ctx context.Context, from, to string, overwrite bool) error {
	if _, err := c.Stat(ctx, from); err != nil {
		if status.Code(err) == codes.NotFound && c.isDir(ctx, from) {
			return fmt.Errorf("mover %s: o mestre só move arquivos, não diretórios", from)
		}
		return err
	}
	// "mv arquivo /dir" move para dentro do diretório, como no shell.
	if c.isDir(ctx, to) {
		to = path.Join(to, path.Base(from))
	}
	if overwrite {
		if _, err := c.Stat(ctx, to); err == nil {
			if err := c.Delete(ctx, to); err != nil {
				return err
			}
		}
	}
	return c.moveFile(ctx, from, to)
}

func (c *Client) moveFile(ctx context.Context, from, to string) error {
	ctx, cancel := c.rpcContext(ctx)
	defer cancel()
	if _, err := c.master.MoveFile(ctx, &mpb.MoveFileRequest{Source: from, Destination: to}); err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return fmt.Errorf("mover %s para %s: destino já existe ou diretório de destino não existe (use -f para sobrescrever): %w", from, to, err)
		}
		return fmt.Errorf("mover %s para %s: %w", from, to, err)
	}
	return nil
}

func (c *Client) isDir(ctx context.Context, p string) bool {
	ctx, cancel := c.rpcContext(ctx)
	defer cancel()
	_, err := c.master.ListDirectory(ctx, &mpb.DirectoryRequest{Path: p})
	return err == nil
}

// MakeDir cria um diretório. Com parents, cria os intermediários e não
// reclama se o diretório já existir (como mkdir -p).
func (c *Client) MakeDir(ctx context.Context, p string, parents bool) error {
	if !parents {
		return c.createDir(ctx, p)
	}
	parts := strings.Split(strings.Trim(p, "/"), "/")
	cur := ""
	for _, part := range parts {
		if part == "" {
			continue
		}
		cur += "/" + part
		if err := c.createDir(ctx, cur); err != nil && status.Code(err) != codes.AlreadyExists {
			return err
		}
	}
	return nil
}

func (c *Client) createDir(ctx context.Context, p string) error {
	ctx, cancel := c.rpcContext(ctx)
	defer cancel()
	if _, err := c.master.CreateDirectory(ctx, &mpb.DirectoryRequest{Path: p}); err != nil {
		switch status.Code(err) {
		case codes.NotFound:
			return fmt.Errorf("criar diretório %s: diretório pai não existe (use -p): %w", p, err)
		case codes.AlreadyExists:
			return fmt.Errorf("criar diretório %s: já existe: %w", p, err)
		}
		return fmt.Errorf("criar diretório %s: %w", p, err)
	}
	return nil
}

// RemoveDir remove um diretório. Sem recursive, ele precisa estar vazio;
// com recursive, remove antes todos os arquivos e subdiretórios.
func (c *Client) RemoveDir(ctx context.Context, p string, recursive bool) error {
	if recursive {
		entries, err := c.List(ctx, p)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if e.IsDir {
				err = c.RemoveDir(ctx, e.Path, true)
			} else {
				err = c.Delete(ctx, e.Path)
			}
			if err != nil {
				return err
			}
		}
	}

	callCtx, cancel := c.rpcContext(ctx)
	defer cancel()
	if _, err := c.master.DeleteDirectory(callCtx, &mpb.DirectoryRequest{Path: p}); err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return fmt.Errorf("remover diretório %s: não está vazio (use -r): %w", p, err)
		}
		return fmt.Errorf("remover diretório %s: %w", p, err)
	}
	return nil
}

// ClusterStatus devolve os nós (vivos, suspeitos, mortos) e a contagem de
// arquivos por estado de replicação.
func (c *Client) ClusterStatus(ctx context.Context) (*mpb.ClusterStatusResponse, error) {
	ctx, cancel := c.rpcContext(ctx)
	defer cancel()
	resp, err := c.master.GetClusterStatus(ctx, &mpb.ClusterStatusRequest{})
	if err != nil {
		return nil, fmt.Errorf("consultar estado do cluster: %w", err)
	}
	return resp, nil
}

// Drain coloca um nó em drenagem: ele para de receber réplicas novas e o
// mestre copia seus blocos para outros nós.
func (c *Client) Drain(ctx context.Context, nodeID string) error {
	ctx, cancel := c.rpcContext(ctx)
	defer cancel()
	if _, err := c.master.DrainNode(ctx, &mpb.DrainNodeRequest{NodeId: nodeID}); err != nil {
		return fmt.Errorf("drenar %s: %w", nodeID, err)
	}
	return nil
}

// IsNotFound indica se o erro veio de um caminho inexistente no mestre.
func IsNotFound(err error) bool { return status.Code(err) == codes.NotFound }
