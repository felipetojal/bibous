#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
bash scripts/generate.sh
go mod tidy
gofmt -w internal cmd
go vet ./...
go test -race ./...
mkdir -p bin
go build -o bin/storage ./cmd/storage
go build -o bin/client ./cmd/client
