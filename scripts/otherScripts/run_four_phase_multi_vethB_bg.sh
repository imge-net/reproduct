#!/usr/bin/env bash
set -euo pipefail

# -----------------------
# Config (override via env)
# -----------------------
N="${N:-100}"                    # number of iterations
IFACE_USE="${IFACE_USE:-vethB}"  # capture interface in nsB
DUR_CAPTURE="${DUR_CAPTURE:-30}" # seconds per phase capture
CLEAN="${CLEAN:-1}"              # 1: clean phase dir at start

# workload knobs (can be overridden)
BENIGN_CONNS="${BENIGN_CONNS:-800}"
BENIGN_CONC="${BENIGN_CONC:-40}"

BURST_CONNS="${BURST_CONNS:-1200}"
BURST_CONC="${BURST_CONC:-60}"
BURST_DUR="${BURST_DUR:-20}"

SLOW_SECS="${SLOW_SECS:-60}"
SLOW_INTERVAL="${SLOW_INTERVAL:-0.5}"
SLOW_BYTES="${SLOW_BYTES:-256}"

# ports
PORT_BENIGN="${PORT_BENIGN:-18082}"
PORT_BURST="${PORT_BURST:-18080}"
PORT_SLOW="${PORT_SLOW:-18081}"

ROOT="${ROOT:-$HOME/work/mycelium_ids}"
SCRIPTS="$ROOT/scripts"
LAB="$ROOT/lab/flows"
PHASE_DIR="${PHASE_DIR:-$LAB/phases_safe_multi}"

# -----------------------
# Helpers
# -----------------------
need() { command -v "$1" >/dev/null 2>&1 || { echo "[ERR] missing cmd: $1"; exit 2; }; }

get_sink_ip() {
  # Prefer nsB vethB IPv4 (stable underlay)
  sudo ip netns exec nsB ip -4 -o addr show dev "$IFACE_USE" | awk '{print $4}' | cut -d/ -f1 | head -n1
}

pcap_to_flows() {
  local PCAP="$1"
  local OUTCSV="$2"
  conda run -n ids_mycelium python "$SCRIPTS/pcap_to_flows.py" --pcap "$PCAP" --out_csv "$OUTCSV" >/dev/null
}

capture_phase() {
  local PHASE="$1"         # benign | attack_exfil_burst | attack_exfil_lowslow
  local ATTACK_TYPE="$2"   # benign | attack_exfil_burst | attack_exfil_lowslow
  local LABEL="$3"         # 0/1
  local ITER="$4"
  local SINK_IP="$5"

  mkdir -p "$PHASE_DIR" "$LAB"

  local PCAP="$LAB/_phase_${PHASE}.pcap"
  local FLOWCSV="$LAB/_phase_flows.csv"
  local OUTCSV="$PHASE_DIR/${ATTACK_TYPE}_${ITER}.csv"

  echo "[INFO] phase=$PHASE attack_type=$ATTACK_TYPE label=$LABEL capture nsB/$IFACE_USE ${DUR_CAPTURE}s"

  # capture
  sudo ip netns exec nsB bash -lc "timeout ${DUR_CAPTURE} tcpdump -n -i ${IFACE_USE} -s 0 -w '${PCAP}' >/dev/null 2>&1" || true

  # export to flow CSV
  pcap_to_flows "$PCAP" "$FLOWCSV"
  local lines
  lines="$(wc -l < "$FLOWCSV" || true)"
  echo "[INFO] flows exported lines=$lines"

  if [[ "${lines:-0}" -le 1 ]]; then
    echo "[ERR] no flows captured for phase=$PHASE (lines=$lines)."
    return 10
  fi

  # label + attack_type append
  conda run -n ids_mycelium python - <<PY
import pandas as pd
p="$FLOWCSV"
df=pd.read_csv(p)
df["y_true"] = int("$LABEL")
df["attack_type"] = "$ATTACK_TYPE"
df.to_csv("$OUTCSV", index=False)
print("[OK] wrote $OUTCSV rows=", len(df))
PY
}

# traffic generators (must exist)
benign_burst() {
  local SINK_IP="$1"
  # if you have your existing benign generator, call it; else fallback to simple curl bursts
  if [[ -x "$SCRIPTS/lab_benign_burst.sh" ]]; then
    bash "$SCRIPTS/lab_benign_burst.sh" nsA "$SINK_IP" "$PORT_BENIGN" "$BENIGN_CONNS" "$BENIGN_CONC" >/dev/null 2>&1 || true
  else
    # fallback: many short tcp connects from nsA
    sudo ip netns exec nsA bash -lc "python - <<'PY'
import socket, threading, time
ip='$SINK_IP'; port=int('$PORT_BENIGN')
N=int('$BENIGN_CONNS'); conc=int('$BENIGN_CONC')
def one():
    try:
        s=socket.socket(); s.settimeout(1.0); s.connect((ip,port)); s.send(b'ping'); s.close()
    except: pass
i=0
while i<N:
    th=[]
    for _ in range(min(conc, N-i)):
        t=threading.Thread(target=one); t.start(); th.append(t)
    for t in th: t.join()
    i += len(th)
PY" >/dev/null 2>&1 || true
  fi
}

exfil_burst() {
  local SINK_IP="$1"
  if [[ -x "$SCRIPTS/lab_exfil_burst.sh" ]]; then
    bash "$SCRIPTS/lab_exfil_burst.sh" nsC "$SINK_IP" "$PORT_BURST" "$BURST_CONNS" "$BURST_CONC" "$BURST_DUR" >/dev/null 2>&1 || true
  else
    bash "$SCRIPTS/lab_exfil.sh" nsC "$SINK_IP" "$BURST_DUR" "$PORT_BURST" >/dev/null 2>&1 || true
  fi
}

exfil_lowslow() {
  local SINK_IP="$1"
  if [[ -x "$SCRIPTS/lab_exfil_lowslow.sh" ]]; then
    bash "$SCRIPTS/lab_exfil_lowslow.sh" nsC "$SINK_IP" "$PORT_SLOW" "$SLOW_SECS" "$SLOW_INTERVAL" "$SLOW_BYTES" >/dev/null 2>&1 || true
  else
    sudo ip netns exec nsC bash -lc "python - <<'PY'
import socket, time, os
ip='$SINK_IP'; port=int('$PORT_SLOW')
secs=float('$SLOW_SECS'); interval=float('$SLOW_INTERVAL'); chunk=int('$SLOW_BYTES')
t_end=time.time()+secs
payload=b'x'*chunk
while time.time()<t_end:
    try:
        s=socket.socket(); s.settimeout(1.0); s.connect((ip,port)); s.send(payload); s.close()
    except: pass
    time.sleep(interval)
PY" >/dev/null 2>&1 || true
  fi
}

start_sink_servers() {
  local SINK_IP="$1"
  # kill existing listeners on our ports inside nsB
  sudo ip netns exec nsB bash -lc "fuser -k ${PORT_BENIGN}/tcp ${PORT_BURST}/tcp ${PORT_SLOW}/tcp >/dev/null 2>&1 || true"

  # start 3 simple TCP sinks (python) inside nsB
  sudo ip netns exec nsB bash -lc "nohup python - <<'PY' >/dev/null 2>&1 &
import socket, threading
ports=[int('$PORT_BENIGN'), int('$PORT_BURST'), int('$PORT_SLOW')]
def serve(port):
    s=socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('0.0.0.0', port)); s.listen(200)
    while True:
        c,_=s.accept()
        try: c.recv(65535)
        except: pass
        c.close()
for p in ports:
    threading.Thread(target=serve, args=(p,), daemon=True).start()
import time
time.sleep(10**9)
PY" || true
}

# -----------------------
# Main
# -----------------------
need sudo
need tcpdump
need conda

TS="${TS:-$(date +%Y%m%d_%H%M%S)}"
RUN="${RUN:-$ROOT/runs/$TS}"
mkdir -p "$RUN"
echo "$RUN" > "$ROOT/runs/_last_run_four_phase.txt"

LOG="$RUN/lab_capture_four_phase.log"
echo "[INFO] start $(date -Is)" | tee "$LOG"
echo "[INFO] RUN=$RUN"          | tee -a "$LOG"
echo "[INFO] N=$N IFACE_USE=$IFACE_USE DUR_CAPTURE=$DUR_CAPTURE" | tee -a "$LOG"
echo "[INFO] PHASE_DIR=$PHASE_DIR" | tee -a "$LOG"

mkdir -p "$PHASE_DIR" "$LAB"

if [[ "$CLEAN" == "1" ]]; then
  rm -f "$PHASE_DIR"/*.csv || true
  rm -f "$LAB"/_phase_*.pcap "$LAB"/_phase_flows.csv || true
fi

SINK_IP="$(get_sink_ip)"
if [[ -z "${SINK_IP:-}" ]]; then
  echo "[ERR] could not read SINK_IP from nsB/$IFACE_USE" | tee -a "$LOG"
  exit 3
fi
echo "[INFO] SINK_IP=$SINK_IP ports benign=$PORT_BENIGN burst=$PORT_BURST slow=$PORT_SLOW" | tee -a "$LOG"

# ensure sudo is primed (interactive once)
sudo -v

start_sink_servers "$SINK_IP"

ok=0; fail=0
for i in $(seq 1 "$N"); do
  echo "==============================" | tee -a "$LOG"
  echo "[ITER $i/$N] $(date -Is)"      | tee -a "$LOG"

  ITERLOG="$RUN/iter_${i}.log"
  {
    echo "[INFO] iter=$i"

    # BENIGN traffic + capture
    benign_burst "$SINK_IP"
    capture_phase "benign" "benign" "0" "$i" "$SINK_IP"

    # BURST attack + capture
    exfil_burst "$SINK_IP"
    capture_phase "attack_exfil_burst" "attack_exfil_burst" "1" "$i" "$SINK_IP"

    # LOW&SLOW attack + capture
    exfil_lowslow "$SINK_IP"
    capture_phase "attack_exfil_lowslow" "attack_exfil_lowslow" "1" "$i" "$SINK_IP"

    echo "[OK] iter=$i done"
  } >"$ITERLOG" 2>&1 && {
    ok=$((ok+1))
    echo "[OK] iter=$i" | tee -a "$LOG"
  } || {
    fail=$((fail+1))
    echo "[FAIL] iter=$i (see $ITERLOG)" | tee -a "$LOG"
  }

  echo "[INFO] csv_count=$(ls -1 "$PHASE_DIR"/*.csv 2>/dev/null | wc -l || true) ok=$ok fail=$fail" | tee -a "$LOG"
done

echo "[DONE] ok=$ok fail=$fail csv_count=$(ls -1 "$PHASE_DIR"/*.csv 2>/dev/null | wc -l || true)" | tee -a "$LOG"
echo "[DONE] end $(date -Is)" | tee -a "$LOG"
