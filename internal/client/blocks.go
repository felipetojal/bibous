package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/felipetojal/bibous/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Mesma regra de internal/storage/block.go.
var blockIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,128}$`)

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// splitFile lê r uma vez, do início ao fim, e devolve a descrição de cada
// bloco (tamanho e SHA-256) e o SHA-256 do arquivo inteiro. Os bytes não
// ficam em memória: no envio cada bloco é relido com ReadAt.
func splitFile(r io.Reader, blockSize int64) ([]*pb.BlockSpec, int64, string, error) {
	var (
		specs []*pb.BlockSpec
		total int64
		whole = sha256.New()
		buf   = make([]byte, blockSize)
	)

	for index := uint32(0); ; index++ {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			chunk := buf[:n]
			whole.Write(chunk)
			specs = append(specs, &pb.BlockSpec{
				Index: index,
				Size:  int64(n),
				Hash:  sha256Hex(chunk),
			})
			total += int64(n)
		}

		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return nil, 0, "", fmt.Errorf("ler arquivo: %w", err)
		}
	}

	return specs, total, hex.EncodeToString(whole.Sum(nil)), nil
}

// validateLocation confere se um BlockLocation vindo do mestre é utilizável.
func validateLocation(b *pb.BlockLocation) error {
	if b == nil {
		return errors.New("bloco ausente na resposta do mestre")
	}
	if !blockIDPattern.MatchString(b.GetBlockId()) {
		return fmt.Errorf("bloco %d: block_id inválido %q", b.GetIndex(), b.GetBlockId())
	}
	if b.GetVersion() < 1 {
		return fmt.Errorf("bloco %d: versão inválida %d", b.GetIndex(), b.GetVersion())
	}
	if b.GetSize() < 0 || b.GetSize() > MaxBlockSize {
		return fmt.Errorf("bloco %d: tamanho inválido %d", b.GetIndex(), b.GetSize())
	}
	if raw, err := hex.DecodeString(b.GetHash()); err != nil || len(raw) != sha256.Size {
		return fmt.Errorf("bloco %d: hash inválido", b.GetIndex())
	}
	if len(b.GetReplicas()) == 0 {
		return fmt.Errorf("bloco %d: mestre não indicou nenhuma réplica", b.GetIndex())
	}
	for _, r := range b.GetReplicas() {
		if r.GetAddress() == "" || r.GetNodeId() == "" {
			return fmt.Errorf("bloco %d: réplica sem node_id ou endereço", b.GetIndex())
		}
	}
	return nil
}

// uniqueReplicas remove réplicas repetidas (mesmo nó ou mesmo endereço).
func uniqueReplicas(in []*pb.NodeLocation) []*pb.NodeLocation {
	seen := make(map[string]bool, len(in)*2)
	out := make([]*pb.NodeLocation, 0, len(in))
	for _, r := range in {
		if seen["id:"+r.GetNodeId()] || seen["addr:"+r.GetAddress()] {
			continue
		}
		seen["id:"+r.GetNodeId()] = true
		seen["addr:"+r.GetAddress()] = true
		out = append(out, r)
	}
	return out
}

// forEach executa fn(i) para i em [0, n) com no máximo parallelism
// goroutines. Para no primeiro erro e cancela as demais.
func forEach(
	ctx context.Context,
	n, parallelism int,
	progress func(done, total int),
	fn func(ctx context.Context, i int) error,
) error {
	if n == 0 {
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg       sync.WaitGroup
		once     sync.Once
		firstErr error
		done     atomic.Int64
		jobs     = make(chan int)
	)

	if parallelism > n {
		parallelism = n
	}

	for w := 0; w < parallelism; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				if err := fn(ctx, i); err != nil {
					once.Do(func() {
						firstErr = err
						cancel()
					})
					continue
				}
				if progress != nil {
					progress(int(done.Add(1)), n)
				}
			}
		}()
	}

feed:
	for i := 0; i < n; i++ {
		select {
		case jobs <- i:
		case <-ctx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()

	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

// retryable indica falhas em que vale tentar o mesmo nó de novo.
func retryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted, codes.ResourceExhausted:
		return true
	}
	return false
}

// withRetry executa call até retries+1 vezes, com espera exponencial,
// enquanto o erro for transitório.
func (c *Client) withRetry(ctx context.Context, call func(ctx context.Context) error) error {
	var err error
	for attempt := 0; attempt <= c.cfg.Retries; attempt++ {
		if attempt > 0 {
			wait := time.Duration(100<<(attempt-1)) * time.Millisecond
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return errors.Join(err, ctx.Err())
			}
		}

		callCtx, cancel := c.rpcContext(ctx)
		err = call(callCtx)
		cancel()

		if err == nil || !retryable(err) || ctx.Err() != nil {
			return err
		}
	}
	return err
}
