#!/usr/bin/env bash
# Linux: três processos independentes; falha de um não derruba os outros.
set -euo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
action="${1:-status}"
target="${2:-all}"
case "$action" in start|stop|restart|status) ;; *) echo 'Uso: cluster.sh start|stop|restart|status [storage-a|storage-b|storage-c|all]' >&2; exit 1;; esac
case "$target" in storage-a|storage-b|storage-c|all) ;; *) echo 'Nó inválido' >&2; exit 1;; esac
mkdir -p run logs data
# Evita duas invocações concorrentes do gerenciador.
exec 9>run/cluster.lock
flock -n 9 || { echo 'Outra operação do cluster está em andamento' >&2; exit 1; }
if [[ "$action" == start || "$action" == restart ]]; then
  [[ -x bin/storage && -x bin/client ]] || { echo 'Execute bash scripts/setup.sh primeiro.' >&2; exit 1; }
fi
alive() {
  local id="$1" recorded_start current_start executable
  [[ -f "run/$id.pid" ]] || return 1
  read -r pid recorded_start < "run/$id.pid"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  [[ -r "/proc/$pid/stat" ]] || return 1
  current_start="$(awk '{print $22}' "/proc/$pid/stat")"
  executable="$(readlink "/proc/$pid/exe" 2>/dev/null || true)"
  [[ "$recorded_start" == "$current_start" && ( "$executable" == "$ROOT/bin/storage" || "$executable" == "$ROOT/bin/storage (deleted)" ) ]]
}
stop_node() {
  local id="$1"
  if alive "$id"; then
    kill -TERM "$pid"
    for ((i=0;i<120;i++)); do
      alive "$id" || break
      sleep 0.1
    done
    if alive "$id"; then kill -KILL "$pid"; fi
    echo "$id encerrado"
  fi
  rm -f "run/$id.pid"
}
start_node() {
  local id="$1" port="$2" child start_time
  if alive "$id"; then echo "$id já está rodando (PID $pid)"; return; fi
  mkdir -p "data/$id"
  nohup env NODE_ID="$id" MACHINE_ID=maquina-local DISK_ID=disco-local \
    LISTEN_ADDR="127.0.0.1:$port" DATA_DIR="$ROOT/data/$id" \
    PEERS='storage-a=127.0.0.1:8081,storage-b=127.0.0.1:8082,storage-c=127.0.0.1:8083' \
    SCRUB_INTERVAL="${SCRUB_INTERVAL:-1m}" \
    "$ROOT/bin/storage" 9>&- >>"logs/$id.log" 2>&1 </dev/null &
  child=$!
  # O shell filho pode ainda estar executando nohup/env. O start time é estável.
  if [[ ! -r "/proc/$child/stat" ]]; then echo "$id falhou; veja logs/$id.log" >&2; return 1; fi
  start_time="$(awk '{print $22}' "/proc/$child/stat")"
  printf '%s %s\n' "$child" "$start_time" > "run/$id.pid"
  for ((i=0;i<50;i++)); do
    if "$ROOT/bin/client" status -nodes nodes.json -node "$id" -timeout 300ms >/dev/null 2>&1; then
      if alive "$id"; then echo "$id pronto: porta=$port PID=$child diretório=data/$id"; return; fi
    fi
    kill -0 "$child" 2>/dev/null || break
    sleep 0.1
  done
  echo "$id não ficou pronto; veja logs/$id.log" >&2
  stop_node "$id"
  return 1
}
for entry in storage-a:8081 storage-b:8082 storage-c:8083; do
  id="${entry%:*}";port="${entry#*:}"
  [[ "$target" == all || "$target" == "$id" ]] || continue
  case "$action" in
    start) start_node "$id" "$port" ;;
    stop) stop_node "$id" ;;
    restart) stop_node "$id"; start_node "$id" "$port" ;;
    status) if alive "$id"; then echo "$id PID=$pid"; else echo "$id parado"; fi ;;
  esac
done
