#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
command -v protoc >/dev/null || { echo 'Instale protoc (no Fedora: sudo dnf install protobuf-compiler).' >&2; exit 1; }
go version
mkdir -p .tools
GOBIN="$PWD/.tools" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.5
GOBIN="$PWD/.tools" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
PATH="$PWD/.tools:$PATH" protoc \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  proto/storage.proto
