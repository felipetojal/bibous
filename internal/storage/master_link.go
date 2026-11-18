package storage

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"path/filepath"
	"syscall"
	"time"

	mpb "github.com/felipetojal/bibous/proto/masterpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Capacity devolve o espaço total e disponível do disco onde fica dir.
//
// Com limit > 0 o nó se comporta como um disco de limit bytes (útil para
// simular vários discos numa máquina só): total = limit e disponível =
// limit menos o que o diretório de dados já ocupa, sem passar do espaço
// realmente livre no disco.
func Capacity(dir string, limit uint64) (total, available uint64, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, 0, err
	}
	bsize := uint64(st.Bsize)
	total, available = uint64(st.Blocks)*bsize, uint64(st.Bavail)*bsize
	if limit == 0 {
		return total, available, nil
	}

	var used uint64
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() {
			if info, err := d.Info(); err == nil {
				used += uint64(info.Size())
			}
		}
		return nil
	})
	free := uint64(0)
	if used < limit {
		free = limit - used
	}
	return limit, min(free, available), nil
}

// MasterLink mantém o nó registrado no mestre (descoberta automática, RF6)
// e envia heartbeats com o espaço livre (RF5/RF7). Se o mestre reiniciar e
// esquecer o nó, o heartbeat volta NotFound e o nó se registra de novo.
type MasterLink struct {
	MasterAddr    string
	NodeID        string
	AdvertiseAddr string // endereço que o mestre repassa aos clientes
	MachineID     string
	DiskID        string
	DataDir       string
	CapacityLimit uint64 // 0 = usa o tamanho real do disco
	Interval      time.Duration
}

// Run bloqueia até ctx ser cancelado.
func (l *MasterLink) Run(ctx context.Context) {
	conn, err := grpc.NewClient(l.MasterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("mestre: endereço inválido %s: %v", l.MasterAddr, err)
		return
	}
	defer conn.Close()
	master := mpb.NewMasterServiceClient(conn)

	registered := false
	var nonce uint64
	ticker := time.NewTicker(l.Interval)
	defer ticker.Stop()

	for {
		if !registered {
			if err := l.register(ctx, master); err != nil {
				log.Printf("mestre: registro falhou node=%s master=%s erro=%v (tentando de novo)", l.NodeID, l.MasterAddr, err)
			} else {
				registered = true
				log.Printf("mestre: nó registrado node=%s address=%s master=%s", l.NodeID, l.AdvertiseAddr, l.MasterAddr)
			}
		} else {
			nonce++
			if err := l.heartbeat(ctx, master, nonce); err != nil {
				if status.Code(err) == codes.NotFound {
					log.Printf("mestre: não conhece o nó (reiniciou?); registrando de novo node=%s", l.NodeID)
					registered = false
					continue
				}
				log.Printf("mestre: heartbeat falhou node=%s erro=%v", l.NodeID, err)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (l *MasterLink) register(ctx context.Context, master mpb.MasterServiceClient) error {
	total, available, err := Capacity(l.DataDir, l.CapacityLimit)
	if err != nil {
		return fmt.Errorf("medir disco: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = master.RegisterNode(ctx, &mpb.RegisterRequest{
		NodeId:        l.NodeID,
		NodeAddress:   l.AdvertiseAddr,
		AvailableSize: available,
		TotalSize:     total,
		MachineId:     l.MachineID,
		DiskId:        l.DiskID,
	})
	return err
}

func (l *MasterLink) heartbeat(ctx context.Context, master mpb.MasterServiceClient, nonce uint64) error {
	req := &mpb.HeartbeatRequest{NodeId: l.NodeID, Nonce: nonce}
	if _, available, err := Capacity(l.DataDir, l.CapacityLimit); err == nil {
		req.AvailableSize = &available
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := master.Heartbeat(ctx, req)
	return err
}
