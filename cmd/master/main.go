// O executável master inicializa somente o plano de controle do Bibous.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/felipetojal/bibous/internal/master"
)

func main() {
	// Os parâmetros permitem executar várias configurações sem recompilar.
	endereco := flag.String("listen", "0.0.0.0", "endereço de escuta do mestre")
	porta := flag.String("port", "50051", "porta gRPC do mestre")
	dados := flag.String("data", "./data/master", "diretório dos metadados persistentes")
	replicas := flag.Int("replicas", 3, "fator padrão de replicação")
	relay := flag.Bool("relay-recovery", true, "recuperar réplicas por intermédio do mestre (compatibilidade temporária)")
	flag.Parse()

	// A camada de transporte conhece apenas o protocolo gRPC do armazenamento.
	configuracao := master.DefaultConfig()
	configuracao.DefaultReplicas = *replicas
	configuracao.RelayRecovery = *relay
	transporte := master.NewGRPCBlockTransport()

	// O domínio é inicializado antes do servidor para carregar o último snapshot.
	coordenador, err := master.NewMaster(*dados, configuracao, transporte)
	if err != nil {
		log.Fatalf("inicializar mestre: %v", err)
	}
	servico := master.NewMasterService(coordenador)
	servidor := master.NewRPCServer(*endereco, *porta, servico)

	// Sinais do sistema operacional solicitam parada ordenada das rotinas.
	ctx, cancelar := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancelar()

	// O monitor funciona independentemente das requisições gRPC recebidas.
	go coordenador.RunMaintenance(ctx)

	go func() {
		<-ctx.Done()
		concluido := make(chan struct{})
		go func() {
			servidor.Stop()
			close(concluido)
		}()

		// Evita que um RPC pendurado impeça a saída indefinidamente.
		select {
		case <-concluido:
		case <-time.After(10 * time.Second):
			servidor.ForceStop()
		}
	}()

	log.Printf("mestre iniciado endereço=%s:%s dados=%s recuperação_via_mestre=%t",
		*endereco, *porta, *dados, *relay)
	if err := servidor.Start(); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("servidor gRPC: %v", err)
	}
}
