#!/usr/bin/env bash
set -euo pipefail

# -----------------------
# Config (override via env)
# -----------------------
ROOT="${ROOT:-$HOME/work/mycelium_ids}"
LAB="${LAB:-$ROOT/lab/flows}"
PHASE_DIR="${PHASE_DIR:-$LAB/phases_safe_multi}"

NS_SINK="${NS_SINK:-nsB}"
NS_BENIGN="${NS_BENIGN:-nsA}"
NS_ATTACK="${NS_ATTACK:-nsC}"

IFACE_USE="${IFACE_USE:-vethB}"
DUR_CAPTURE="${DUR_CAPTURE:-30}"

# sink IPv4 (nsB vethB)
SINK_IP="${SINK_IP:-10.10.0.3}"

# ports
PORT_BENIGN="${PORT_BENIGN:-18082}"
PORT_BURST="${PORT_BURST:-18080}"
PORT_SLOW="${PORT_SLOW:-18081}"

# traffic intensity knobs
BENIGN_CONN="${BENIGN_CONN:-800}"
BENIGN_CONC="${BENIGN_CONC:-40}"

BURST_CONN="${BURST_CONN:-1200}"
BURST_CONC="${BURST_CONC:-60}"
BURST_DUR="${BURST_DUR:-20}"

SLOW_SECS="${SLOW_SECS:-60}"
SLOW_INTERVAL="${SLOW_INTERVAL:-0.5}"
SLOW_BYTES="${SLOW_BYTES:-256}"

ITER="${1:-1}"

mkdir -p "$LAB" "$PHASE_DIR"

pcap_to_flows_py="$ROOT/scripts/pcap_to_flows.py"
if [[ ! -f "$pcap_to_flows_py" ]]; then
  echo "[ERR] missing $pcap_to_flows_py"
  exit 2
fi

# -----------------------
# helpers
# -----------------------
log(){ echo "[INFO] $*"; }
err(){ echo "[ERR] $*" >&2; }

kill_port_listeners() {
  # kill python/http listeners inside nsB (best-effort)
  sudo ip netns exec "$NS_SINK" bash -lc "fuser -k ${PORT_BENIGN}/tcp ${PORT_BURST}/tcp ${PORT_SLOW}/tcp >/dev/null 2>&1 || true"
}

start_sink() {
  local PORT="$1"
  # minimal TCP sink: python http.server is enough to accept connections
  sudo ip netns exec "$NS_SINK" bash -lc \
    "nohup python3 -m http.server ${PORT} --bind ${SINK_IP} >/dev/null 2>&1 & echo \$! > /tmp/sink_${PORT}.pid"
}

stop_sink() {
  local PORT="$1"
  sudo ip netns exec "$NS_SINK" bash -lc \
    "test -f /tmp/sink_${PORT}.pid && kill \$(cat /tmp/sink_${PORT}.pid) >/dev/null 2>&1 || true; rm -f /tmp/sink_${PORT}.pid || true"
}

capture_with_trigger() {
  local PHASE="$1"         # benign / attack_exfil_burst / attack_exfil_lowslow
  local OUT_CSV="$2"
  local OUT_PCAP="$3"

  # 1) start capture (background)
  log "pcap capture phase=${PHASE} ${NS_SINK}/${IFACE_USE} ${DUR_CAPTURE}s -> ${OUT_PCAP}"
  sudo ip netns exec "$NS_SINK" bash -lc \
    "timeout ${DUR_CAPTURE} tcpdump -n -i ${IFACE_USE} -s 0 -w '${OUT_PCAP}' >/dev/null 2>&1" &
  local CAP_PID=$!

  # 2) start traffic while capture is running
  case "$PHASE" in
    benign)
      kill_port_listeners
      start_sink "$PORT_BENIGN"
      log "benign traffic: nsA -> ${SINK_IP}:${PORT_BENIGN} conns=${BENIGN_CONN} conc=${BENIGN_CONC}"
      sudo ip netns exec "$NS_BENIGN" bash -lc "
        python3 - <<'PY'
import asyncio, os, socket
dst=os.environ.get('DST','10.10.0.3')
port=int(os.environ.get('PORT','18082'))
N=int(os.environ.get('N','800'))
CONC=int(os.environ.get('CONC','40'))
async def one():
    try:
        r,w=await asyncio.open_connection(dst,port)
        w.write(b'GET / HTTP/1.1\\r\\nHost: x\\r\\n\\r\\n')
        await w.drain()
        w.close()
        await w.wait_closed()
    except Exception:
        pass
async def main():
    sem=asyncio.Semaphore(CONC)
    async def run1():
        async with sem:
            await one()
    await asyncio.gather(*[run1() for _ in range(N)])
asyncio.run(main())
PY" \
      DST="$SINK_IP" PORT="$PORT_BENIGN" N="$BENIGN_CONN" CONC="$BENIGN_CONC" >/dev/null 2>&1 || true
      stop_sink "$PORT_BENIGN"
      ;;
    attack_exfil_burst)
      kill_port_listeners
      start_sink "$PORT_BURST"
      log "burst exfil: nsC -> ${SINK_IP}:${PORT_BURST} conns=${BURST_CONN} conc=${BURST_CONC}"
      sudo ip netns exec "$NS_ATTACK" bash -lc "
        python3 - <<'PY'
import asyncio, os, random
dst=os.environ.get('DST','10.10.0.3')
port=int(os.environ.get('PORT','18080'))
N=int(os.environ.get('N','1200'))
CONC=int(os.environ.get('CONC','60'))
payload=b'x'*1024
async def one():
    try:
        r,w=await asyncio.open_connection(dst,port)
        w.write(b'POST / HTTP/1.1\\r\\nHost: x\\r\\nContent-Length: 1024\\r\\n\\r\\n'+payload)
        await w.drain()
        w.close()
        await w.wait_closed()
    except Exception:
        pass
async def main():
    sem=asyncio.Semaphore(CONC)
    async def run1():
        async with sem:
            await one()
    await asyncio.gather(*[run1() for _ in range(N)])
asyncio.run(main())
PY" \
      DST="$SINK_IP" PORT="$PORT_BURST" N="$BURST_CONN" CONC="$BURST_CONC" >/dev/null 2>&1 || true
      stop_sink "$PORT_BURST"
      ;;
    attack_exfil_lowslow)
      kill_port_listeners
      start_sink "$PORT_SLOW"
      log "low&slow exfil: nsC -> ${SINK_IP}:${PORT_SLOW} secs=${SLOW_SECS} interval=${SLOW_INTERVAL} bytes=${SLOW_BYTES}"
      sudo ip netns exec "$NS_ATTACK" bash -lc "
        python3 - <<'PY'
import os, time, socket
dst=os.environ.get('DST','10.10.0.3')
port=int(os.environ.get('PORT','18081'))
secs=float(os.environ.get('SECS','60'))
interval=float(os.environ.get('INTERVAL','0.5'))
nbytes=int(os.environ.get('BYTES','256'))
end=time.time()+secs
payload=b'y'*nbytes
while time.time()<end:
    try:
        s=socket.create_connection((dst,port),timeout=2)
        s.sendall(b'POST / HTTP/1.1\\r\\nHost: x\\r\\nContent-Length: '+str(len(payload)).encode()+b'\\r\\n\\r\\n'+payload)
        s.close()
    except Exception:
        pass
    time.sleep(interval)
PY" \
      DST="$SINK_IP" PORT="$PORT_SLOW" SECS="$SLOW_SECS" INTERVAL="$SLOW_INTERVAL" BYTES="$SLOW_BYTES" >/dev/null 2>&1 || true
      stop_sink "$PORT_SLOW"
      ;;
    *)
      err "unknown phase=$PHASE"
      kill "$CAP_PID" >/dev/null 2>&1 || true
      return 1
      ;;
  esac

  # 3) wait capture end
  wait "$CAP_PID" >/dev/null 2>&1 || true

  # 4) pcap -> flows
  conda run -n ids_mycelium python "$pcap_to_flows_py" --pcap "$OUT_PCAP" --out_csv "$OUT_CSV" >/dev/null 2>&1 || {
    err "pcap_to_flows failed phase=$PHASE"
    return 1
  }

  local LINES
  LINES="$(wc -l < "$OUT_CSV" | tr -d ' ')"
  log "flows exported lines=${LINES} phase=${PHASE}"
  if [[ "$LINES" -le 1 ]]; then
    err "no flows captured for phase=$PHASE (lines=$LINES)."
    return 1
  fi
  return 0
}

label_and_save() {
  local PHASE="$1"
  local LABEL="$2"
  local ATTACK_TYPE="$3"
  local OUT_CSV="$4"

  # add y_true + attack_type columns (expects exporter has header)
  conda run -n ids_mycelium python - <<'PY' "$OUT_CSV" "$LABEL" "$ATTACK_TYPE"
import sys, pandas as pd
p=sys.argv[1]; y=int(sys.argv[2]); at=sys.argv[3]
df=pd.read_csv(p)
df["y_true"]=y
df["attack_type"]=at
df.to_csv(p, index=False)
PY
}

# -----------------------
# main
# -----------------------
echo "=============================="
echo "[ITER ${ITER}] four-phase capture IFACE_USE=${IFACE_USE} DUR_CAPTURE=${DUR_CAPTURE}"
echo "[INFO] sink=${NS_SINK}/${IFACE_USE} SINK_IP=${SINK_IP}"
echo "[INFO] ports: benign=${PORT_BENIGN} burst=${PORT_BURST} slow=${PORT_SLOW}"

# paths
PH_BENIGN="${PHASE_DIR}/benign_${ITER}.csv"
PH_BURST="${PHASE_DIR}/attack_exfil_burst_${ITER}.csv"
PH_SLOW="${PHASE_DIR}/attack_exfil_lowslow_${ITER}.csv"

PCAP_BENIGN="${LAB}/_phase_benign.pcap"
PCAP_BURST="${LAB}/_phase_attack_exfil_burst.pcap"
PCAP_SLOW="${LAB}/_phase_attack_exfil_lowslow.pcap"
TMP_CSV="${LAB}/_phase_flows.csv"

# benign
capture_with_trigger "benign" "$TMP_CSV" "$PCAP_BENIGN"
cp -f "$TMP_CSV" "$PH_BENIGN"
label_and_save "benign" 0 "benign" "$PH_BENIGN"

# burst
capture_with_trigger "attack_exfil_burst" "$TMP_CSV" "$PCAP_BURST"
cp -f "$TMP_CSV" "$PH_BURST"
label_and_save "attack_exfil_burst" 1 "attack_exfil_burst" "$PH_BURST"

# low&slow
capture_with_trigger "attack_exfil_lowslow" "$TMP_CSV" "$PCAP_SLOW"
cp -f "$TMP_CSV" "$PH_SLOW"
label_and_save "attack_exfil_lowslow" 1 "attack_exfil_lowslow" "$PH_SLOW"

echo "[DONE] wrote: $PH_BENIGN $PH_BURST $PH_SLOW"
