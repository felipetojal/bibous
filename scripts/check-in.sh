#!/usr/bin/env bash
# Teste integrado em diretórios temporários; não altera data/ do usuário.
set -euo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
[[ -x bin/storage && -x bin/client ]] || { echo 'Execute bash scripts/setup.sh primeiro.' >&2; exit 1; }
TASK_DIR="$(mktemp -d)"
pids=()
cleanup() {
  trap - EXIT INT TERM
  for pid in "${pids[@]}"; do kill -TERM "$pid" 2>/dev/null || true; done
  for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
  echo "Arquivos do teste preservados em $TASK_DIR"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
python3 - "$TASK_DIR" <<'PY'
import json, pathlib, socket, sys
root=pathlib.Path(sys.argv[1]); sockets=[];nodes=[]
for node in ['storage-a','storage-b','storage-c']:
    s=socket.socket();s.bind(('127.0.0.1',0));sockets.append(s)
    nodes.append({'node_id':node,'address':f'127.0.0.1:{s.getsockname()[1]}'})
(root/'nodes.json').write_text(json.dumps({'nodes':nodes}))
(root/'peers').write_text(','.join(n['node_id']+'='+n['address'] for n in nodes))
(root/'addresses').write_text('\n'.join(n['address'] for n in nodes)+'\n')
for s in sockets:s.close()
PY
mapfile -t addresses < "$TASK_DIR/addresses"
peers="$(cat "$TASK_DIR/peers")"
start_node() {
  local i="$1" id="$2"
  env NODE_ID="$id" MACHINE_ID=test-machine DISK_ID=test-disk \
    LISTEN_ADDR="${addresses[$i]}" DATA_DIR="$TASK_DIR/$id" \
    PEERS="$peers" SCRUB_INTERVAL=0s \
    "$ROOT/bin/storage" >>"$TASK_DIR/$id.log" 2>&1 &
  pids[$i]=$!
  for ((attempt=0;attempt<50;attempt++)); do
    if bin/client status -nodes "$TASK_DIR/nodes.json" -node "$id" -timeout 300ms >/dev/null 2>&1; then return; fi
    sleep 0.1
  done
  echo "Falha ao iniciar $id" >&2;return 1
}
start_node 0 storage-a
start_node 1 storage-b
start_node 2 storage-c
head -c 11534336 /dev/urandom > "$TASK_DIR/input.bin"
bin/client put -nodes "$TASK_DIR/nodes.json" -file "$TASK_DIR/input.bin" -manifest "$TASK_DIR/manifest.json"
python3 - "$TASK_DIR/manifest.json" <<'PY'
import json,sys
m=json.load(open(sys.argv[1]));assert m['state']=='complete'
assert [b['size'] for b in m['blocks']]==[4194304,4194304,3145728]
assert all(len(b['replicas'])==3 and all(r['confirmed'] for r in b['replicas']) for b in m['blocks'])
PY
# Um processo morto e uma réplica com bytes incorretos.
kill -KILL "${pids[0]}"
wait "${pids[0]}" 2>/dev/null || true
unset 'pids[0]'
python3 - "$TASK_DIR" <<'PY'
import json,pathlib,sys
root=pathlib.Path(sys.argv[1]);b=json.loads((root/'manifest.json').read_text())['blocks'][0]
(root/'storage-b'/f"{b['block_id']}-v{b['version']}.blk").write_bytes(b'corrupted')
PY
bin/client scrub -nodes "$TASK_DIR/nodes.json" -node storage-b
bin/client get -manifest "$TASK_DIR/manifest.json" -out "$TASK_DIR/output.bin" -timeout 1s
cmp "$TASK_DIR/input.bin" "$TASK_DIR/output.bin"
bin/client repair -manifest "$TASK_DIR/manifest.json" -target storage-b -timeout 2s
# Exclusão parcial precisa permanecer recuperável, sem fingir sucesso.
if bin/client delete -manifest "$TASK_DIR/manifest.json" -timeout 1s; then
  echo 'Erro: delete deveria falhar enquanto A está parado.' >&2;exit 1
fi
start_node 0 storage-a
bin/client delete -manifest "$TASK_DIR/manifest.json" -timeout 2s
bin/client status -nodes "$TASK_DIR/nodes.json"
python3 - "$TASK_DIR/manifest.json" <<'PY'
import json,sys
assert json.load(open(sys.argv[1]))['state']=='deleted'
PY
echo 'PASS: 3 réplicas, fallback, corrupção, cópia direta, reinício e remoção pendente.'
