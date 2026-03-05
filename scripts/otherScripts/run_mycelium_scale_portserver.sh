#!/usr/bin/env bash
set -euo pipefail

# ==============
# CONFIG (edit if needed)
# ==============
ENV_NAME="${ENV_NAME:-ids_mycelium}"

IFACE_USE="${IFACE_USE:-vethB}"
DUR_CAPTURE="${DUR_CAPTURE:-30}"          # seconds capture per phase
SLEEP_BETWEEN="${SLEEP_BETWEEN:-1}"

# dataset repetitions
REPS="${REPS:-25}"

# port-range server on nsB
PORT_START="${PORT_START:-18080}"
PORT_END="${PORT_END:-18180}"

# traffic volume per phase (number of connections)
BENIGN_CONN="${BENIGN_CONN:-800}"
EXFIL_CONN="${EXFIL_CONN:-800}"
CONC="${CONC:-40}"                         # parallelism

ROOT="$HOME/work/mycelium_ids"
LAB="$ROOT/lab/flows"
PHASES="$LAB/phases_safe"

RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="$ROOT/runs/$RUN_TS"
LOG="$RUN_DIR/run_mycelium_scale.log"
PIDF="$RUN_DIR/run_mycelium_scale.pid"

mkdir -p "$RUN_DIR" "$LAB" "$PHASES"

# ==============
# Helpers
# ==============
need() { command -v "$1" >/dev/null 2>&1 || { echo "[ERR] missing: $1"; exit 1; }; }
need python
need tcpdump
need tshark

# read B overlay IP (for traffic generation)
B_OV6="$(sudo ip netns exec nsB ip -br a | awk '/myc0/ {print $3}' | cut -d/ -f1)"
echo "[INFO] B_OV6=$B_OV6" | tee -a "$LOG"

# ensure exporter is packet-aggregation based (no argus/ra)
EXPORTER="$ROOT/scripts/export_lab_flows_pktagg.sh"
PHASE_CAP="$ROOT/scripts/lab_capture_phase.sh"

if [[ ! -x "$EXPORTER" ]]; then
  echo "[ERR] exporter missing: $EXPORTER" | tee -a "$LOG"
  exit 2
fi
if [[ ! -f "$PHASE_CAP" ]]; then
  echo "[ERR] phase capture missing: $PHASE_CAP" | tee -a "$LOG"
  exit 2
fi

# ==============
# Port-range server (single python process) inside nsB
# ==============
PORTSERV_PY="$RUN_DIR/port_server.py"
cat > "$PORTSERV_PY" <<'PY'
import asyncio, argparse

ap=argparse.ArgumentParser()
ap.add_argument("--host", default="::")
ap.add_argument("--start", type=int, required=True)
ap.add_argument("--end", type=int, required=True)
args=ap.parse_args()

async def handle(reader, writer):
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
    finally:
        try: writer.close()
        except: pass

async def main():
    servers=[]
    for p in range(args.start, args.end+1):
        srv = await asyncio.start_server(handle, host=args.host, port=p, family=socket.AF_INET6)  # noqa
        servers.append(srv)

    addrs=[]
    for s in servers:
        addrs.extend([str(sock.getsockname()) for sock in s.sockets])
    print("[portserver] listening:", len(addrs), "ports")
    await asyncio.gather(*(s.serve_forever() for s in servers))

if __name__=="__main__":
    import socket
    asyncio.run(main())
PY

# start server in nsB
start_portserver() {
  echo "[INFO] starting portserver nsB :: ${PORT_START}-${PORT_END}" | tee -a "$LOG"
  sudo ip netns exec nsB bash -lc "nohup python3 '$PORTSERV_PY' --start $PORT_START --end $PORT_END > '$RUN_DIR/portserver.log' 2>&1 & echo \$! > '$RUN_DIR/portserver.pid'"
  sleep 0.7
  sudo ip netns exec nsB bash -lc "ss -ltn | grep -q ':$PORT_START ' || { echo '[ERR] portserver not listening'; exit 3; }"
  echo "[OK] portserver up" | tee -a "$LOG"
}

stop_portserver() {
  if [[ -f "$RUN_DIR/portserver.pid" ]]; then
    local pid
    pid="$(cat "$RUN_DIR/portserver.pid")"
    sudo ip netns exec nsB bash -lc "kill $pid 2>/dev/null || true"
    rm -f "$RUN_DIR/portserver.pid"
    echo "[OK] portserver stopped" | tee -a "$LOG"
  fi
}

# ==============
# Traffic generators (many dst ports -> unique flows)
# ==============
benign_burst() {
  echo "[INFO] benign burst: conns=$BENIGN_CONN conc=$CONC" | tee -a "$LOG"
  sudo ip netns exec nsA bash -lc "
python3 - <<'PY'
import random
start=$PORT_START; end=$PORT_END; n=$BENIGN_CONN
ports=[random.randint(start,end) for _ in range(n)]
print(' '.join(map(str,ports)))
PY
" | tr ' ' '\n' | head -n "$BENIGN_CONN" | \
  xargs -I{} -P "$CONC" sudo ip netns exec nsA bash -lc \
  'printf "hi" | nc -6 -w 1 '"$B_OV6"' {} >/dev/null 2>&1 || true'
}

exfil_burst() {
  echo "[INFO] exfil burst: conns=$EXFIL_CONN conc=$CONC" | tee -a "$LOG"
  sudo ip netns exec nsC bash -lc "
python3 - <<'PY'
import random
start=$PORT_START; end=$PORT_END; n=$EXFIL_CONN
ports=[random.randint(start,end) for _ in range(n)]
print(' '.join(map(str,ports)))
PY
" | tr ' ' '\n' | head -n "$EXFIL_CONN" | \
  xargs -I{} -P "$CONC" sudo ip netns exec nsC bash -lc \
  'head -c 20000 </dev/urandom | nc -6 -w 1 '"$B_OV6"' {} >/dev/null 2>&1 || true'
}

# ==============
# Phase capture wrapper (uses existing lab_capture_phase.sh but forces IFACE_USE and exporter)
# ==============
phase_capture() {
  local phase="$1"
  local label="$2"
  local out="$3"
  echo "[INFO] capture phase=$phase label=$label out=$out" | tee -a "$LOG"
  IFACE_USE="$IFACE_USE" DUR="$DUR_CAPTURE" bash "$PHASE_CAP" "$phase" "$label" "$out" >>"$LOG" 2>&1
}

# ==============
# MAIN (run in foreground; wrapper launches this in background)
# ==============
main() {
  echo "[INFO] run_dir=$RUN_DIR" | tee -a "$LOG"
  echo "[INFO] reps=$REPS iface=$IFACE_USE dur_capture=$DUR_CAPTURE ports=$PORT_START-$PORT_END" | tee -a "$LOG"

  # clean phases dir
  rm -rf "$PHASES"
  mkdir -p "$PHASES"

  start_portserver

  for i in $(seq 1 "$REPS"); do
    echo "==============================" | tee -a "$LOG"
    echo "[ITER $i/$REPS]" | tee -a "$LOG"
    echo "==============================" | tee -a "$LOG"

    benign_burst
    phase_capture "benign" 0 "$LAB/benign.csv"
    mv -f "$LAB/benign.csv" "$PHASES/benign_${i}.csv"

    exfil_burst
    phase_capture "attack_exfil" 1 "$LAB/attack_exfil.csv"
    mv -f "$LAB/attack_exfil.csv" "$PHASES/attack_exfil_${i}.csv"

    sleep "$SLEEP_BETWEEN"
  done

  stop_portserver

  # merge + normalize proto + write parquet
  source "$HOME/miniconda3/etc/profile.d/conda.sh"
  conda activate "$ENV_NAME"

  python - <<'PY'
import glob, os, pandas as pd
LAB=os.path.expanduser("~/work/mycelium_ids/lab/flows")
P=os.path.join(LAB,"phases_safe")
files=sorted(glob.glob(os.path.join(P,"*.csv")))
df=pd.concat([pd.read_csv(p) for p in files], ignore_index=True)
df.columns=[c.strip().lower() for c in df.columns]

# normalize proto as string
proto_raw = df.get("proto", pd.Series(["unk"]*len(df))).astype(str).str.strip().str.lower()
def map_proto(x):
    try:
        if x.replace(".","",1).isdigit():
            v=int(float(x))
            return {6:"tcp",17:"udp",58:"ipv6-icmp",1:"icmp"}.get(v, str(v))
    except: pass
    if x in ("tcp","udp","icmp","ipv6-icmp"): return x
    return x if x else "unk"
df["proto"]=proto_raw.map(map_proto).astype(str)

for c in ["dur","sport","dport","spkts","dpkts","sbytes","dbytes","y_true"]:
    if c in df.columns:
        df[c]=pd.to_numeric(df[c], errors="coerce").fillna(0)

out_csv=os.path.join(LAB,"lab_dataset_big_safe.csv")
out_pq=os.path.join(LAB,"lab_dataset_big_safe.parquet")
df.to_csv(out_csv,index=False)
df.to_parquet(out_pq,index=False)

print("[OK] wrote", out_pq, "rows=", len(df), "pos_rate=", float(df["y_true"].mean()))
print(df["attack_type"].value_counts())
PY

  echo "[DONE] dataset ready: $LAB/lab_dataset_big_safe.parquet" | tee -a "$LOG"
  echo "[DONE] log: $LOG" | tee -a "$LOG"
}

main
