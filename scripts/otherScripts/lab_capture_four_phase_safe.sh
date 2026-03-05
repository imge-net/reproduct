#!/usr/bin/env bash
set -euo pipefail

# ========================= config =========================
ROOT="${ROOT:-$HOME/work/mycelium_ids}"
LAB="${LAB:-$ROOT/lab/flows}"

ITER="${1:-1}"
NS_SINK="${NS_SINK:-nsB}"
NS_BENIGN_SRC="${NS_BENIGN_SRC:-nsA}"
NS_ATTACK_SRC="${NS_ATTACK_SRC:-nsC}"

IFACE_USE="${IFACE_USE:-vethB}"
DUR_CAPTURE="${DUR_CAPTURE:-30}"

# Underlay IPv4 sink (vethB addr in nsB). No overlay dependency.
SINK_IP="${SINK_IP:-$(sudo ip netns exec "$NS_SINK" ip -4 -o addr show dev "$IFACE_USE" 2>/dev/null | awk '{print $4}' | cut -d/ -f1 | head -n1)}"
if [[ -z "${SINK_IP}" ]]; then
  echo "[ERR] Could not determine SINK_IP from $NS_SINK/$IFACE_USE. Set SINK_IP manually."
  exit 2
fi

# Ports
PORT_BURST="${PORT_BURST:-18080}"
PORT_SLOW="${PORT_SLOW:-18081}"
PORT_BENIGN="${PORT_BENIGN:-18082}"

# Benign load
BENIGN_CONN="${BENIGN_CONN:-800}"
BENIGN_CONC="${BENIGN_CONC:-40}"

# Exfil burst load
EXFIL_BURST_CONN="${EXFIL_BURST_CONN:-1200}"
EXFIL_BURST_CONC="${EXFIL_BURST_CONC:-60}"

# Low&slow params
EXFIL_SLOW_SECS="${EXFIL_SLOW_SECS:-60}"
EXFIL_SLOW_INTERVAL="${EXFIL_SLOW_INTERVAL:-0.5}"
EXFIL_SLOW_BYTES="${EXFIL_SLOW_BYTES:-256}"

PHASE_DIR="${PHASE_DIR:-$LAB/phases_safe_multi}"
mkdir -p "$LAB" "$PHASE_DIR"

OUT_BENIGN="$PHASE_DIR/benign_${ITER}.csv"
OUT_BURST="$PHASE_DIR/attack_exfil_burst_${ITER}.csv"
OUT_SLOW="$PHASE_DIR/attack_exfil_lowslow_${ITER}.csv"

# temp
TMP_PCAP_BENIGN="$LAB/_phase_benign.pcap"
TMP_PCAP_BURST="$LAB/_phase_attack_exfil_burst.pcap"
TMP_PCAP_SLOW="$LAB/_phase_attack_exfil_lowslow.pcap"
TMP_FLOW_CSV="$LAB/_phase_flows.csv"

# ========================= helpers =========================
die(){ echo "[ERR] $*" >&2; exit 1; }

# Export pcaps -> flows via your pcap_to_flows.py (tshark based)
pcap_to_flows () {
  local PCAP="$1"
  local OUTCSV="$2"
  conda run -n ids_mycelium python "$ROOT/scripts/pcap_to_flows.py" \
    --pcap "$PCAP" --out_csv "$OUTCSV" >/dev/null
}

capture_pcap_and_export () {
  local PHASE="$1"
  local PCAP_OUT="$2"
  local CSV_OUT="$3"
  local NS="$4"
  local IFACE="$5"
  local DUR="$6"

  echo "[INFO] pcap capture phase=$PHASE $NS/$IFACE for ${DUR}s -> $PCAP_OUT"
  sudo ip netns exec "$NS" bash -lc "timeout ${DUR} tcpdump -n -i ${IFACE} -s 0 -w '${PCAP_OUT}' >/dev/null 2>&1" || true

  pcap_to_flows "$PCAP_OUT" "$CSV_OUT"
  local lines
  lines="$(wc -l < "$CSV_OUT" || echo 0)"
  echo "[OK] pcap->flows: $CSV_OUT lines=$lines"

  # If only header exists => no traffic captured => fail fast (so iter is counted as FAIL)
  if [[ "$lines" -le 1 ]]; then
    echo "[ERR] no flows captured for phase=$PHASE (lines=$lines)."
    return 1
  fi
}

# Write a labeled phase CSV (attack_type + y_true) from TMP_FLOW_CSV
write_phase_csv () {
  local PHASE="$1"
  local LABEL="$2"
  local ATTACK_TYPE="$3"
  local OUT="$4"

  conda run -n ids_mycelium python - <<PY
import pandas as pd
p="${TMP_FLOW_CSV}"
df=pd.read_csv(p)
df.columns=[c.strip() for c in df.columns]
df["y_true"]=int(${LABEL})
df["attack_type"]="${ATTACK_TYPE}"
df.to_csv("${OUT}", index=False)
print("[OK] wrote ${OUT} n=", len(df))
PY
}

start_http_server () {
  local PORT="$1"
  local PIDFILE="$2"
  # Start server in nsB. Use timeout as a watchdog.
  sudo ip netns exec "$NS_SINK" bash -lc "( timeout $((DUR_CAPTURE+15)) python -m http.server ${PORT} --bind 0.0.0.0 >/dev/null 2>&1 & echo \$! > '${PIDFILE}' )" \
    >/dev/null 2>&1 || true
}

stop_http_server () {
  local PIDFILE="$1"
  sudo ip netns exec "$NS_SINK" bash -lc "test -f '${PIDFILE}' && kill \$(cat '${PIDFILE}') >/dev/null 2>&1 || true" \
    >/dev/null 2>&1 || true
}

# ========================= traffic generators =========================

benign_burst () {
  # Ensure listener exists for benign as well (THIS WAS THE MISSING PIECE)
  start_http_server "$PORT_BENIGN" "/tmp/http_benign.pid"

  sudo ip netns exec "$NS_BENIGN_SRC" bash -lc "
python - <<'PY'
import socket, threading
dst='${SINK_IP}'
port=int('${PORT_BENIGN}')
N=int('${BENIGN_CONN}')
CONC=int('${BENIGN_CONC}')
payload=b'B'*256

def one():
    try:
        s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1.5)
        s.connect((dst,port))
        s.sendall(payload)
        s.close()
    except:
        pass

sem=threading.Semaphore(CONC)
ths=[]
for _ in range(N):
    sem.acquire()
    t=threading.Thread(target=lambda: (one(), sem.release()))
    t.daemon=True
    t.start()
    ths.append(t)
for t in ths:
    t.join()
PY
" >/dev/null 2>&1 || true

  stop_http_server "/tmp/http_benign.pid"
}

exfil_burst () {
  start_http_server "$PORT_BURST" "/tmp/http_burst.pid"

  sudo ip netns exec "$NS_ATTACK_SRC" bash -lc "
python - <<'PY'
import socket, threading
dst='${SINK_IP}'
port=int('${PORT_BURST}')
N=int('${EXFIL_BURST_CONN}')
CONC=int('${EXFIL_BURST_CONC}')
payload=b'X'*1024

def one():
    try:
        s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1.5)
        s.connect((dst,port))
        s.sendall(payload)
        s.close()
    except:
        pass

sem=threading.Semaphore(CONC)
ths=[]
for _ in range(N):
    sem.acquire()
    t=threading.Thread(target=lambda: (one(), sem.release()))
    t.daemon=True
    t.start()
    ths.append(t)
for t in ths:
    t.join()
PY
" >/dev/null 2>&1 || true

  stop_http_server "/tmp/http_burst.pid"
}

exfil_lowslow () {
  start_http_server "$PORT_SLOW" "/tmp/http_slow.pid"

  sudo ip netns exec "$NS_ATTACK_SRC" bash -lc "
python - <<'PY'
import socket, time
dst='${SINK_IP}'
port=int('${PORT_SLOW}')
secs=float('${EXFIL_SLOW_SECS}')
interval=float('${EXFIL_SLOW_INTERVAL}')
bytes_n=int('${EXFIL_SLOW_BYTES}')
payload=b'Z'*bytes_n
t0=time.time()
while time.time()-t0 < secs:
    try:
        s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1.5)
        s.connect((dst,port))
        s.sendall(payload)
        s.close()
    except:
        pass
    time.sleep(interval)
PY
" >/dev/null 2>&1 || true

  stop_http_server "/tmp/http_slow.pid"
}

# ========================= run phases =========================
echo "=============================="
echo "[ITER ${ITER}] four-phase capture IFACE_USE=$IFACE_USE DUR_CAPTURE=$DUR_CAPTURE"
echo "[INFO] sink=$NS_SINK/$IFACE_USE SINK_IP=$SINK_IP"
echo "[INFO] ports: benign=$PORT_BENIGN burst=$PORT_BURST slow=$PORT_SLOW"
echo "[INFO] benign: conns=$BENIGN_CONN conc=$BENIGN_CONC"
echo "[INFO] exfil_burst: conns=$EXFIL_BURST_CONN conc=$EXFIL_BURST_CONC"
echo "[INFO] exfil_lowslow: secs=$EXFIL_SLOW_SECS interval=$EXFIL_SLOW_INTERVAL bytes=$EXFIL_SLOW_BYTES"

# Phase 1: benign
PHASE="benign"
benign_burst
capture_pcap_and_export "$PHASE" "$TMP_PCAP_BENIGN" "$TMP_FLOW_CSV" "$NS_SINK" "$IFACE_USE" "$DUR_CAPTURE"
write_phase_csv "$PHASE" 0 "benign" "$OUT_BENIGN"

# Phase 2: exfil burst
PHASE="attack_exfil_burst"
exfil_burst
capture_pcap_and_export "$PHASE" "$TMP_PCAP_BURST" "$TMP_FLOW_CSV" "$NS_SINK" "$IFACE_USE" "$DUR_CAPTURE"
write_phase_csv "$PHASE" 1 "attack_exfil_burst" "$OUT_BURST"

# Phase 3: low&slow
PHASE="attack_exfil_lowslow"
exfil_lowslow
capture_pcap_and_export "$PHASE" "$TMP_PCAP_SLOW" "$TMP_FLOW_CSV" "$NS_SINK" "$IFACE_USE" "$DUR_CAPTURE"
write_phase_csv "$PHASE" 1 "attack_exfil_lowslow" "$OUT_SLOW"

echo "[DONE] wrote phases into: $PHASE_DIR"
