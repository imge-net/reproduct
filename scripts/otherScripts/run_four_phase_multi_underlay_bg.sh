#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$HOME/work/mycelium_ids}"
LAB="${LAB:-$ROOT/lab/flows}"
PHASE_DIR="${PHASE_DIR:-$LAB/phases_safe_multi}"
OUT_RUN="${OUT_RUN:-$ROOT/runs/$(date +%Y%m%d_%H%M%S)}"
N="${N:-100}"

# Underlay settings (NO myc0 needed)
SINK_NS="${SINK_NS:-nsB}"
CLIENT_NS="${CLIENT_NS:-nsA}"
ATTACK_NS="${ATTACK_NS:-nsC}"
IFACE_USE="${IFACE_USE:-vethB}"
SINK_IP="${SINK_IP:-10.10.0.3}"

DUR_CAPTURE="${DUR_CAPTURE:-30}"

# Traffic shape
BENIGN_CONN="${BENIGN_CONN:-800}"
BENIGN_CONC="${BENIGN_CONC:-40}"

BURST_CONN="${BURST_CONN:-1200}"
BURST_CONC="${BURST_CONC:-60}"
BURST_DUR="${BURST_DUR:-20}"

LOWSLOW_SECS="${LOWSLOW_SECS:-60}"
LOWSLOW_INTERVAL="${LOWSLOW_INTERVAL:-0.5}"
LOWSLOW_BYTES="${LOWSLOW_BYTES:-256}"

# Ports
PORT_BURST="${PORT_BURST:-18080}"
PORT_SLOW="${PORT_SLOW:-18081}"
PORT_BENIGN="${PORT_BENIGN:-18082}"

mkdir -p "$OUT_RUN" "$PHASE_DIR" "$LAB"

log="$OUT_RUN/lab_capture_four_phase.log"
echo "[INFO] start $(date -Is)" | tee -a "$log"
echo "[INFO] OUT_RUN=$OUT_RUN"  | tee -a "$log"
echo "[INFO] N=$N IFACE_USE=$IFACE_USE DUR_CAPTURE=$DUR_CAPTURE SINK_IP=$SINK_IP" | tee -a "$log"
echo "[INFO] ports: burst=$PORT_BURST slow=$PORT_SLOW benign=$PORT_BENIGN" | tee -a "$log"

# ---- prerequisites: sudo must be already authorized (no password prompts in background) ----
if ! sudo -n true 2>/dev/null; then
  echo "[ERR] sudo is not authorized for non-interactive use." | tee -a "$log"
  echo "[HINT] Run once in this terminal:  sudo -v" | tee -a "$log"
  exit 2
fi

# ---- helpers ----
start_listener () {
  local port="$1"
  # kill anything on that port inside nsB
  sudo ip netns exec "$SINK_NS" bash -lc "fuser -k ${port}/tcp >/dev/null 2>&1 || true"
  # start simple TCP HTTP server
  sudo ip netns exec "$SINK_NS" bash -lc "nohup python3 -m http.server ${port} --bind ${SINK_IP} >/dev/null 2>&1 & echo \$! > /tmp/http_${port}.pid"
}

stop_listener () {
  local port="$1"
  sudo ip netns exec "$SINK_NS" bash -lc "test -f /tmp/http_${port}.pid && kill \$(cat /tmp/http_${port}.pid) >/dev/null 2>&1 || true"
  sudo ip netns exec "$SINK_NS" bash -lc "rm -f /tmp/http_${port}.pid || true"
  sudo ip netns exec "$SINK_NS" bash -lc "fuser -k ${port}/tcp >/dev/null 2>&1 || true"
}

capture_phase () {
  local phase="$1"       # benign / attack_exfil_burst / attack_exfil_lowslow
  local label="$2"       # 0/1
  local iter="$3"

  local pcap="$LAB/_phase_${phase}.pcap"
  local flows="$LAB/_phase_flows.csv"
  local out_csv="$PHASE_DIR/${phase}_${iter}.csv"

  rm -f "$pcap" "$flows" || true

  echo "[INFO] phase=$phase label=$label iter=$iter capture ${SINK_NS}/${IFACE_USE} for ${DUR_CAPTURE}s" | tee -a "$log"

  # start tcpdump (nsB)
  sudo ip netns exec "$SINK_NS" bash -lc "timeout ${DUR_CAPTURE} tcpdump -n -i ${IFACE_USE} -s 0 -w '${pcap}' >/dev/null 2>&1" || true

  # convert pcap -> flows
  conda run -n ids_mycelium python "$ROOT/scripts/pcap_to_flows.py" \
    --pcap "$pcap" --out_csv "$flows" >/dev/null 2>&1 || true

  # if flows empty, write header-only labeled file
  if [ ! -s "$flows" ] || [ "$(wc -l < "$flows")" -le 1 ]; then
    echo "dur,proto,saddr,sport,daddr,dport,spkts,dpkts,sbytes,dbytes,state,y_true,attack_type" > "$out_csv"
    echo "[OK] wrote $out_csv n=0 (empty capture)" | tee -a "$log"
    return 0
  fi

  # append labels
  conda run -n ids_mycelium python - <<PY >/dev/null 2>&1
import pandas as pd
p_in="${flows}"
p_out="${out_csv}"
df=pd.read_csv(p_in)
df["y_true"]=${label}
df["attack_type"]="${phase}"
df.to_csv(p_out, index=False)
PY

  echo "[OK] wrote $out_csv n=$(( $(wc -l < "$out_csv") - 1 ))" | tee -a "$log"
}

burst_conns () {
  local ns="$1"
  local port="$2"
  local conns="$3"
  local conc="$4"

  sudo ip netns exec "$ns" bash -lc "
set -euo pipefail
IP='${SINK_IP}'
PORT='${port}'
CONNS='${conns}'
CONC='${conc}'
worker() {
  local k=\$1
  for i in \$(seq 1 \$k); do
    (echo -e 'GET / HTTP/1.1\r\nHost: x\r\n\r\n' >/dev/tcp/\$IP/\$PORT) >/dev/null 2>&1 || true
  done
}
# split workload into CONC workers
per=\$(( CONNS / CONC ))
rem=\$(( CONNS % CONC ))
pids=()
for w in \$(seq 1 \$CONC); do
  k=\$per
  if [ \$w -le \$rem ]; then k=\$((k+1)); fi
  worker \$k &
  pids+=(\$!)
done
for p in \${pids[@]}; do wait \$p || true; done
"
}

lowslow_send () {
  local ns="$1"
  local port="$2"
  local secs="$3"
  local interval="$4"
  local bytes="$5"

  sudo ip netns exec "$ns" bash -lc "
set -euo pipefail
IP='${SINK_IP}'
PORT='${port}'
SECS='${secs}'
INT='${interval}'
BYTES='${bytes}'
end=\$(( \$(date +%s) + SECS ))
payload=\$(python3 - <<'PY'
n=int(${bytes})
print('A'*n)
PY
)
while [ \$(date +%s) -lt \$end ]; do
  (printf \"%s\" \"\$payload\" >/dev/tcp/\$IP/\$PORT) >/dev/null 2>&1 || true
  sleep \"\$INT\" || true
done
"
}

# ---- start listeners once (stable ports) ----
start_listener "$PORT_BENIGN"
start_listener "$PORT_BURST"
start_listener "$PORT_SLOW"

ok=0; fail=0

for i in $(seq 1 "$N"); do
  echo "==============================" | tee -a "$log"
  echo "[ITER $i/$N] $(date -Is)"      | tee -a "$log"

  # BENIGN
  burst_conns "$CLIENT_NS" "$PORT_BENIGN" "$BENIGN_CONN" "$BENIGN_CONC"
  capture_phase "benign" 0 "$i" || true

  # ATTACK burst
  burst_conns "$ATTACK_NS" "$PORT_BURST" "$BURST_CONN" "$BURST_CONC"
  capture_phase "attack_exfil_burst" 1 "$i" || true

  # ATTACK low&slow
  lowslow_send "$ATTACK_NS" "$PORT_SLOW" "$LOWSLOW_SECS" "$LOWSLOW_INTERVAL" "$LOWSLOW_BYTES"
  capture_phase "attack_exfil_lowslow" 1 "$i" || true

  ok=$((ok+1))
  echo "[OK] iter=$i" | tee -a "$log"
done

stop_listener "$PORT_BENIGN"
stop_listener "$PORT_BURST"
stop_listener "$PORT_SLOW"

count=$(ls -1 "$PHASE_DIR"/*.csv 2>/dev/null | wc -l || true)
echo "[DONE] ok=$ok fail=$fail csv_count=$count" | tee -a "$log"
echo "[DONE] end $(date -Is)" | tee -a "$log"
echo "$OUT_RUN" > "$ROOT/runs/_last_run_four_phase_underlay.txt"
