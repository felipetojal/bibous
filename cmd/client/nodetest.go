package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"time"

	pb "github.com/felipetojal/bibous/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const maxMessageBytes = 4*1024*1024 + 64*1024

// runNodeTest é o teste de fumaça original do storage: conecta direto num
// nó (sem mestre), grava um bloco e lê de volta.
func runNodeTest(args []string) error {
	fs := flag.NewFlagSet("node-test", flag.ExitOnError)
	address := fs.String("addr", "localhost:8081", "endereço do storage")
	blockID := fs.String("id", "B1", "ID do bloco")
	version := fs.Int64("version", 1, "versão do bloco")
	getOnly := fs.Bool("get-only", false, "executar somente download")
	fs.Parse(args)

	conn, err := grpc.NewClient(
		*address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessageBytes),
			grpc.MaxCallSendMsgSize(maxMessageBytes),
		),
	)
	if err != nil {
		return fmt.Errorf("criar cliente: %w", err)
	}
	defer conn.Close()

	client := pb.NewStorageServiceClient(conn)

	// 1. Verificar se o nó responde.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	health, err := client.Heartbeat(ctx, &pb.HeartbeatRequest{
		Nonce: 123,
	})
	cancel()

	if err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}

	if health.GetNonce() != 123 || health.GetNodeId() == "" {
		return fmt.Errorf("resposta de heartbeat inválida")
	}

	log.Printf("conectado ao nó %s", health.GetNodeId())

	data := []byte("first block")
	sum := sha256.Sum256(data)
	expectedHash := hex.EncodeToString(sum[:])

	// 2. Enviar o bloco.
	if !*getOnly {
		var randomID [16]byte
		if _, err := rand.Read(randomID[:]); err != nil {
			return fmt.Errorf("gerar ID da operação: %w", err)
		}

		operationID := hex.EncodeToString(randomID[:])

		ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
		ack, err := client.PutBlock(ctx, &pb.PutRequest{
			OperationId: operationID,
			BlockId:     *blockID,
			Version:     *version,
			Size:        int64(len(data)),
			Hash:        expectedHash,
			Data:        data,
		})
		cancel()

		if err != nil {
			return fmt.Errorf("put: %w", err)
		}

		if ack.GetNodeId() != health.GetNodeId() ||
			ack.GetBlockId() != *blockID ||
			ack.GetVersion() != *version ||
			ack.GetSize() != int64(len(data)) ||
			ack.GetHash() != expectedHash {
			return fmt.Errorf("ACK não corresponde ao bloco enviado")
		}

		log.Printf("upload confirmado: bloco=%s", ack.GetBlockId())
	}

	// 3. Recuperar o bloco.
	ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
	response, err := client.GetBlock(ctx, &pb.GetRequest{
		BlockId: *blockID,
		Version: *version,
	})
	cancel()

	if err != nil {
		return fmt.Errorf("get: %w", err)
	}

	if response.GetBlockId() != *blockID ||
		response.GetVersion() != *version ||
		response.GetSize() != int64(len(data)) {
		return fmt.Errorf("metadados do download divergentes")
	}

	receivedSum := sha256.Sum256(response.GetData())
	receivedHash := hex.EncodeToString(receivedSum[:])

	if receivedHash != expectedHash ||
		response.GetHash() != expectedHash {
		return fmt.Errorf("hash do download divergente")
	}

	if !bytes.Equal(data, response.GetData()) {
		return fmt.Errorf("conteúdo baixado diferente do original")
	}

	log.Printf(
		"SUCESSO: bloco=%s versão=%d conteúdo=%q",
		*blockID, *version, response.GetData(),
	)

	return nil
}
