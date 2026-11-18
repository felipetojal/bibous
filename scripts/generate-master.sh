#!/usr/bin/env bash
# Regenera as mensagens e os serviços gRPC exclusivos do mestre.
# Os artefatos ficam em proto/masterpb para não misturar os pacotes Go
# masterpb e storagepb existentes no diretório proto.
set -euo pipefail

cd "$(dirname "$0")/.."

# Inclui no PATH os executáveis instalados com `go install`.
export PATH="$(go env GOPATH)/bin:$PATH"

for comando in protoc protoc-gen-go protoc-gen-go-grpc; do
  if ! command -v "$comando" >/dev/null 2>&1; then
    echo "Ferramenta ausente: $comando" >&2
    echo "Instale o protoc e os plugins Go antes de continuar." >&2
    exit 1
  fi
done

# O modo module posiciona os arquivos conforme a opção go_package,
# sem criar pastas duplicadas com o caminho completo do repositório.
protoc -I . \
  --go_out=. \
  --go_opt=module=github.com/felipetojal/bibous \
  --go-grpc_out=. \
  --go-grpc_opt=module=github.com/felipetojal/bibous \
  proto/master.proto

echo "Arquivos gerados em proto/masterpb/"
