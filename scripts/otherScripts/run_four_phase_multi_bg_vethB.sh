#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$HOME/work/mycelium_ids}"
RUNS="$ROOT/runs"
LAB="$ROOT/lab/flows"
SCRIPT_CAPTURE="$ROOT/scripts/lab_capture_four_phase_safe.sh"
PCAP2FLOWS="$ROOT/scripts/pcap_to_flows.py"

N="${N:-100}"
IFACE_USE="${IFACE_USE:-vethB}"
DUR_CAPTURE="${DUR_CAPTURE:-30}"
CLEAN="${CLEAN:-1}"
RETRY_PER_PHASE="${RETRY_PER_PHASE:-3}"

# traffic knobs (override via env)
BENIGN_CONN="${BENIGN_CONN:-800}"
BENIGN_CONC="${BENIGN_CONC:-40}"
BURST_CONN="${BURST_CONN:-1200}"
BURST_CONC="${BURST_CONC:-60}"
EXFIL_DUR="${EXFIL_DUR:-20}"
LOWSLOW_SECS="${LOWSLOW_SECS:-60}"
LOWSLOW_INTERVAL="${LOWSLOW_INTERVAL:-0.5}"
LOWSLOW_BYTES="${LOWSLOW_BYTES:-256}"

PORT_BENIGN="${PORT_BENIGN:-18082}"
PORT_BURST="${PORT_BURST:-18080}"
PORT_SLOW="${PORT_SLOW:-18081}"

TS="${TS:-$(date +%Y%m%d_%H%M%S)}"
RUN="$RUNS/$TS"
mkdir -p "$RUN"
echo "$RUN" > "$RUNS/_last_run_four_phase.txt"

# Require sudo session upfront (prevents sudo prompting in bg)
if ! sudo -n true 2>/dev/null; then
  echo "[ERR] sudo not cached. Run once interactively: sudo -v"
  exit 2
fi

# Find SINK_IP from nsB IFACE_USE (IPv4)
SINK_IP="$(sudo ip netns exec nsB ip -o -4 addr show dev "$IFACE_USE" | awk '{print $4}' | cut -d/ -f1 | head -n1 || true)"
if [ -z "$SINK_IP" ]; then
  echo "[ERR] could not detect SINK_IP on nsB/$IFACE_USE"
  sudo ip netns exec nsB ip -br a || true
  exit 3
fi

echo "[INFO] start $(date -Is)" | tee -a "$RUN/lab_capture_four_phase.log"
echo "[INFO] RUN=$RUN"         | tee -a "$RUN/lab_capture_four_phase.log"
echo "[INFO] N=$N IFACE_USE=$IFACE_USE DUR_CAPTURE=$DUR_CAPTURE SINK_IP=$SINK_IP" | tee -a "$RUN/lab_capture_four_phase.log"

# Optionally clean phase dir
PHASE_DIR="$LAB/phases_safe_multi"
mkdir -p "$PHASE_DIR"
if [ "$CLEAN" = "1" ]; then
  rm -f "$PHASE_DIR"/*.csv 2>/dev/null || true
fi

# helper: capture pcap then pcap->flows csv
pcap_to_csv() {
  local phase="$1"
  local pcap="$2"
  local outcsv="$3"
  local dur="$4"
  sudo ip netns exec nsB bash -lc "timeout ${dur} tcpdump -n -i ${IFACE_USE} -s 0 -w '${pcap}' >/dev/null 2>&1" || true
  conda run -n ids_mycelium python "$PCAP2FLOWS" --pcap "$pcap" --out_csv "$outcsv" >/dev/null 2>&1 || true
  local lines
  lines="$(wc -l < "$outcsv" 2>/dev/null || echo 0)"
  echo "$lines"
}

# helper: start TCP sink (simple python) in nsB
start_sink() {
  local port="$1"
  # kill any old listener on that port inside nsB
  sudo ip netns exec nsB bash -lc "fuser -k -n tcp ${port} >/dev/null 2>&1 || true"
  # start a simple TCP server that accepts connections and discards
  sudo ip netns exec nsB bash -lc "nohup python3 - <<'PY' >/dev/null 2>&1 &
import socket, threading
HOST='0.0.0.0'
PORT=int('${port}')
def h(c,a):
  try:
    while c.recv(65535):
      pass
  except: pass
  try: c.close()
  except: pass
s=socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((HOST,PORT))
s.listen(512)
while True:
  c,a=s.accept()
  threading.Thread(target=h,args=(c,a),daemon=True).start()
PY"
}

# helper: generate benign traffic nsA->nsB
gen_benign() {
  local n="$1" conc="$2" port="$3"
  sudo ip netns exec nsA bash -lc "python3 - <<'PY'
import socket, threading, time
dst='${SINK_IP}'; port=int('${port}')
N=int('${n}'); CONC=int('${conc}')
def worker(k):
  try:
    s=socket.create_connection((dst,port),timeout=2)
    s.sendall(b'GET / HTTP/1.0\\r\\n\\r\\n')
    s.close()
  except: pass
for i in range(0,N,CONC):
  th=[]
  for k in range(min(CONC,N-i)):
    t=threading.Thread(target=worker,args=(k,),daemon=True)
    t.start(); th.append(t)
  for t in th: t.join()
PY"
}

# helper: generate burst exfil nsC->nsB
gen_burst() {
  local n="$1" conc="$2" port="$3" dur="$4"
  sudo ip netns exec nsC bash -lc "python3 - <<'PY'
import socket, threading, time, os
dst='${SINK_IP}'; port=int('${port}')
N=int('${n}'); CONC=int('${conc}')
payload=b'x'*4096
def worker():
  try:
    s=socket.create_connection((dst,port),timeout=2)
    end=time.time()+float('${dur}')
    while time.time()<end:
      s.sendall(payload)
    s.close()
  except: pass
for i in range(0,N,CONC):
  th=[]
  for _ in range(min(CONC,N-i)):
    t=threading.Thread(target=worker,daemon=True)
    t.start(); th.append(t)
  for t in th: t.join()
PY"
}

# helper: generate low&slow exfil nsC->nsB
gen_lowslow() {
  local secs="$1" interval="$2" bytes="$3" port="$4"
  sudo ip netns exec nsC bash -lc "python3 - <<'PY'
import socket, time, os
dst='${SINK_IP}'; port=int('${port}')
secs=float('${secs}'); interval=float('${interval}'); nbytes=int('${bytes}')
payload=b'z'*nbytes
end=time.time()+secs
try:
  s=socket.create_connection((dst,port),timeout=2)
  while time.time()<end:
    try: s.sendall(payload)
    except: break
    time.sleep(interval)
  s.close()
except: pass
PY"
}

one_iter() {
  local i="$1"
  local base="$PHASE_DIR"
  local pcap_b="$LAB/_phase_benign.pcap"
  local pcap_x1="$LAB/_phase_attack_exfil_burst.pcap"
  local pcap_x2="$LAB/_phase_attack_exfil_lowslow.pcap"
  local tmpcsv="$LAB/_phase_flows.csv"

  echo "==============================" | tee -a "$RUN/lab_capture_four_phase.log"
  echo "[ITER $i/$N] $(date -Is)"      | tee -a "$RUN/lab_capture_four_phase.log"
  echo "[INFO] four-phase capture IFACE_USE=$IFACE_USE DUR_CAPTURE=$DUR_CAPTURE" | tee -a "$RUN/lab_capture_four_phase.log"

  # phase 1: benign
  start_sink "$PORT_BENIGN"
  gen_benign "$BENIGN_CONN" "$BENIGN_CONC" "$PORT_BENIGN"
  local ok1=0
  for r in $(seq 1 "$RETRY_PER_PHASE"); do
    lines="$(pcap_to_csv benign "$pcap_b" "$tmpcsv" "$DUR_CAPTURE")"
    if [ "$lines" -gt 1 ]; then ok1=1; break; fi
    echo "[WARN] benign empty (try $r/$RETRY_PER_PHASE)" | tee -a "$RUN/lab_capture_four_phase.log"
  done
  if [ "$ok1" != "1" ]; then
    echo "[FAIL] iter=$i benign no flows" | tee -a "$RUN/lab_capture_four_phase.log"
    return 1
  fi
  cp -f "$tmpcsv" "$base/benign_${i}.csv"

  # phase 2: burst
  start_sink "$PORT_BURST"
  gen_burst "$BURST_CONN" "$BURST_CONC" "$PORT_BURST" "$EXFIL_DUR"
  local ok2=0
  for r in $(seq 1 "$RETRY_PER_PHASE"); do
    lines="$(pcap_to_csv attack_exfil_burst "$pcap_x1" "$tmpcsv" "$DUR_CAPTURE")"
    if [ "$lines" -gt 1 ]; then ok2=1; break; fi
    echo "[WARN] burst empty (try $r/$RETRY_PER_PHASE)" | tee -a "$RUN/lab_capture_four_phase.log"
  done
  if [ "$ok2" != "1" ]; then
    echo "[FAIL] iter=$i burst no flows" | tee -a "$RUN/lab_capture_four_phase.log"
    return 1
  fi
  cp -f "$tmpcsv" "$base/attack_exfil_burst_${i}.csv"

  # phase 3: low&slow
  start_sink "$PORT_SLOW"
  gen_lowslow "$LOWSLOW_SECS" "$LOWSLOW_INTERVAL" "$LOWSLOW_BYTES" "$PORT_SLOW"
  local ok3=0
  for r in $(seq 1 "$RETRY_PER_PHASE"); do
    lines="$(pcap_to_csv attack_exfil_lowslow "$pcap_x2" "$tmpcsv" "$DUR_CAPTURE")"
    if [ "$lines" -gt 1 ]; then ok3=1; break; fi
    echo "[WARN] lowslow empty (try $r/$RETRY_PER_PHASE)" | tee -a "$RUN/lab_capture_four_phase.log"
  done
  if [ "$ok3" != "1" ]; then
    echo "[FAIL] iter=$i lowslow no flows" | tee -a "$RUN/lab_capture_four_phase.log"
    return 1
  fi
  cp -f "$tmpcsv" "$base/attack_exfil_lowslow_${i}.csv"

  echo "[OK] iter=$i" | tee -a "$RUN/lab_capture_four_phase.log"
}

ok=0; fail=0
for i in $(seq 1 "$N"); do
  if one_iter "$i" >"$RUN/iter_${i}.log" 2>&1; then
    ok=$((ok+1))
  else
    fail=$((fail+1))
    echo "[FAIL] iter=$i (see $RUN/iter_${i}.log)" | tee -a "$RUN/lab_capture_four_phase.log"
  fi
done

count="$(ls -1 "$PHASE_DIR"/*.csv 2>/dev/null | wc -l || true)"
echo "[DONE] ok=$ok fail=$fail csv_count=$count" | tee -a "$RUN/lab_capture_four_phase.log"
echo "[DONE] end $(date -Is)" | tee -a "$RUN/lab_capture_four_phase.log"
