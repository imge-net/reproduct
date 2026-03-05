#!/usr/bin/env bash
set -euo pipefail

ROOT="$HOME/work/mycelium_ids"
LAB="$ROOT/lab/flows"
mkdir -p "$LAB"

B_OV6="${B_OV6:?export B_OV6 first}"
export IFACE_USE="${IFACE_USE:-vethB}"

DUR="${DUR:-25}"
SINK_PORT="${SINK_PORT:-18080}"

# sweep knobs
SRC_START_B="${SRC_START_B:-20000}"
SRC_END_B="${SRC_END_B:-20200}"
SRC_START_E="${SRC_START_E:-21000}"
SRC_END_E="${SRC_END_E:-21200}"

echo "[INFO] IFACE_USE=$IFACE_USE DUR=$DUR sink_port=$SINK_PORT"
echo "[INFO] benign src-ports $SRC_START_B-$SRC_END_B, exfil src-ports $SRC_START_E-$SRC_END_E"

# ---- start single sink in nsB
sudo ip netns exec nsB bash -lc "fuser -k ${SINK_PORT}/tcp 2>/dev/null || true"
sudo ip netns exec nsB bash -lc "nohup nc -6 -lk -p ${SINK_PORT} > /dev/null 2>&1 & echo \$! > /tmp/sink_${SINK_PORT}.pid"
sleep 0.3
sudo ip netns exec nsB bash -lc "ss -ltn | grep -q ':${SINK_PORT} ' || { echo '[ERR] sink not listening'; exit 2; }"

# ---- BENIGN: nsA -> nsB using many different source ports (small payload)
sudo ip netns exec nsA bash -lc "
for p in \$(seq ${SRC_START_B} ${SRC_END_B}); do
  (printf 'hi' | nc -6 -w 1 -p \$p $B_OV6 ${SINK_PORT} >/dev/null 2>&1 || true) &
done
wait
"
DUR="$DUR" bash "$ROOT/scripts/lab_capture_phase.sh" "benign" 0 "$LAB/benign.csv"

# ---- EXFIL: nsC -> nsB using many different source ports (larger payload)
sudo ip netns exec nsC bash -lc "
for p in \$(seq ${SRC_START_E} ${SRC_END_E}); do
  (head -c 20000 </dev/urandom | nc -6 -w 1 -p \$p $B_OV6 ${SINK_PORT} >/dev/null 2>&1 || true) &
done
wait
"
DUR="$DUR" bash "$ROOT/scripts/lab_capture_phase.sh" "attack_exfil" 1 "$LAB/attack_exfil.csv"

# ---- stop sink
sudo ip netns exec nsB bash -lc "kill \$(cat /tmp/sink_${SINK_PORT}.pid) 2>/dev/null || true; rm -f /tmp/sink_${SINK_PORT}.pid"

echo "[OK] wrote $LAB/benign.csv and $LAB/attack_exfil.csv"
