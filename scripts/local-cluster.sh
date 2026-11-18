#!/usr/bin/env bash
# Sobe um cluster local completo: 1 mestre + N storages (padrão 5), cada
# storage com MACHINE_ID próprio para simular máquinas diferentes (o mestre
# nunca põe duas réplicas na mesma máquina). Funciona em macOS e Linux.
#
#   bash scripts/local-cluster.sh start [N]   compila e sobe tudo
#   bash scripts/local-cluster.sh stop        derruba tudo
#   bash scripts/local-cluster.sh kill <id>   derruba um storage (ex.: storage-2)
#   bash scripts/local-cluster.sh revive <id> sobe de novo um storage
#   bash scripts/local-cluster.sh status      processos rodando
#   bash scripts/local-cluster.sh clean       stop + apaga dados e logs
#
# Portas: mestre 50051; storages 8081, 8082, ...
# Cada storage se apresenta como um disco de CAPACITY (padrão 10GiB), para
# o mestre não recusar nós quando o disco real do computador está cheio.
# Dados em cluster/data, logs em cluster/logs.
set -euo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

MASTER_PORT="${MASTER_PORT:-50051}"
BASE_PORT="${BASE_PORT:-8081}"
REPLICAS="${REPLICAS:-3}"
DIR="$ROOT/cluster"
mkdir -p "$DIR/data" "$DIR/logs" "$DIR/run" "$ROOT/bin"

build() {
  echo "compilando..."
  go build -o bin/master ./cmd/master
  go build -o bin/storage ./cmd/storage
  go build -o bin/client ./cmd/client
}

running() { [[ -f "$DIR/run/$1.pid" ]] && kill -0 "$(cat "$DIR/run/$1.pid")" 2>/dev/null; }

start_master() {
  if running master; then echo "master já está rodando"; return; fi
  nohup "$ROOT/bin/master" -listen 127.0.0.1 -port "$MASTER_PORT" \
    -data "$DIR/data/master" -replicas "$REPLICAS" \
    >>"$DIR/logs/master.log" 2>&1 </dev/null &
  echo $! >"$DIR/run/master.pid"
  echo "master    pid=$!  127.0.0.1:$MASTER_PORT  (replicação padrão: $REPLICAS)"
}

start_storage() {
  local i="$1" id="storage-$1" port=$((BASE_PORT + $1 - 1))
  if running "$id"; then echo "$id já está rodando"; return; fi
  mkdir -p "$DIR/data/$id"
  nohup env NODE_ID="$id" MACHINE_ID="maquina-$i" DISK_ID="disco-1" \
    LISTEN_ADDR="127.0.0.1:$port" DATA_DIR="$DIR/data/$id" \
    MASTER_ADDR="127.0.0.1:$MASTER_PORT" SCRUB_INTERVAL="${SCRUB_INTERVAL:-30s}" \
    CAPACITY="${CAPACITY:-10GiB}" \
    "$ROOT/bin/storage" >>"$DIR/logs/$id.log" 2>&1 </dev/null &
  echo $! >"$DIR/run/$id.pid"
  echo "$id pid=$!  127.0.0.1:$port"
}

stop_one() {
  if running "$1"; then
    kill "$(cat "$DIR/run/$1.pid")" 2>/dev/null || true
    echo "$1 parado"
  fi
  rm -f "$DIR/run/$1.pid"
}

case "${1:-}" in
  start)
    n="${2:-5}"
    build
    start_master
    sleep 1
    for ((i = 1; i <= n; i++)); do start_storage "$i"; done
    echo "$n" >"$DIR/run/count"
    sleep 2
    echo
    "$ROOT/bin/client" -master "127.0.0.1:$MASTER_PORT" status || true
    ;;
  stop)
    for f in "$DIR"/run/*.pid; do [[ -e "$f" ]] && stop_one "$(basename "$f" .pid)"; done
    ;;
  kill)
    [[ -n "${2:-}" ]] || { echo "uso: $0 kill storage-N" >&2; exit 1; }
    pid="$(cat "$DIR/run/$2.pid" 2>/dev/null || true)"
    [[ -n "$pid" ]] && kill -9 "$pid" 2>/dev/null && echo "$2 derrubado (kill -9)"
    rm -f "$DIR/run/$2.pid"
    ;;
  revive)
    [[ "${2:-}" =~ ^storage-([0-9]+)$ ]] || { echo "uso: $0 revive storage-N" >&2; exit 1; }
    start_storage "${BASH_REMATCH[1]}"
    ;;
  status)
    for f in "$DIR"/run/*.pid; do
      [[ -e "$f" ]] || continue
      id="$(basename "$f" .pid)"
      if running "$id"; then echo "$id rodando (pid $(cat "$f"))"; else echo "$id parado"; fi
    done
    ;;
  clean)
    "$0" stop
    rm -rf "$DIR"
    echo "dados e logs apagados"
    ;;
  *)
    sed -n '2,13p' "$0"
    exit 1
    ;;
esac
