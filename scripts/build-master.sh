#!/usr/bin/env bash
# Compila o mestre a partir do contrato Protobuf e executa as verificações.
set -euo pipefail
cd "$(dirname "$0")/.."

# Regenera apenas o contrato do mestre; o pacote storagepb permanece intocado.
bash scripts/generate-master.sh

# Atualiza o grafo de dependências usando o go.mod existente.
go mod tidy

# Os testes incluem concorrência, persistência, recuperação e gRPC em memória.
go test -race ./internal/master

# Os dois executáveis são independentes do binário de armazenamento.
mkdir -p bin
go build -o bin/bibous-master ./cmd/master

echo "Compilação concluída: bin/bibous-master"
