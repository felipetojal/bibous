package main

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/felipetojal/bibous/internal/storage"
	pb "github.com/felipetojal/bibous/proto"
	"github.com/joho/godotenv"

	"google.golang.org/grpc"
)

func main() {
	// O .env é opcional quando as variáveis já vêm do ambiente.
	if err := godotenv.Load(".env"); err != nil &&
		!errors.Is(err, os.ErrNotExist) {
		log.Fatalf("carregar .env: %v", err)
	}

	cfg, err := storage.LoadConfig()
	if err != nil {
		log.Fatal(err)
	}

	store, err := storage.NewStore(cfg.DataDir)
	if err != nil {
		log.Fatalf("inicializar armazenamento: %v", err)
	}

	defer store.Close()

	stopScrub := make(chan struct{})
	defer close(stopScrub)
	if cfg.ScrubInterval > 0 {
		go func() {
			ticker := time.NewTicker(cfg.ScrubInterval)
			defer ticker.Stop()
			for {
				select {
				case <-stopScrub:
					return
				case <-ticker.C:
					result, err := store.Scrub()
					log.Printf("scrub periódico node=%s checked=%d corrupt=%d error=%v", cfg.NodeId, result.Checked, result.Corrupt, err)
				}
			}
		}()
	}

	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		log.Fatalf("escutar em %s: %v", cfg.Listen, err)
	}

	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(storage.MaxRPCMessageBytes),
		grpc.MaxSendMsgSize(storage.MaxRPCMessageBytes),
	)

	pb.RegisterStorageServiceServer(
		server,
		storage.NewRPCServer(store, cfg.NodeId, cfg.Peers),
	)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	go func() {
		<-signals
		log.Print("encerrando servidor")

		done := make(chan struct{})

		go func() {
			server.GracefulStop()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(10 * time.Second):
			server.Stop()
		}
	}()

	// Registro automático no mestre e heartbeats periódicos.
	if cfg.MasterAddr != "" {
		linkCtx, stopLink := context.WithCancel(context.Background())
		defer stopLink()
		link := &storage.MasterLink{
			MasterAddr: cfg.MasterAddr, NodeID: cfg.NodeId, AdvertiseAddr: cfg.AdvertiseAddr,
			MachineID: cfg.MachineId, DiskID: cfg.DiskId, DataDir: store.DataDir,
			CapacityLimit: cfg.Capacity,
			Interval:      cfg.HeartbeatInterval,
		}
		go link.Run(linkCtx)
	} else {
		log.Print("MASTER_ADDR não definido: o nó não se registra no mestre")
	}

	log.Printf(
		"storage iniciado node=%s address=%s data_dir=%s",
		cfg.NodeId,
		listener.Addr(),
		store.DataDir,
	)

	if err := server.Serve(listener); err != nil &&
		!errors.Is(err, grpc.ErrServerStopped) {
		log.Fatalf("servidor gRPC: %v", err)
	}
}
