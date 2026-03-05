#!/usr/bin/env bash
set -euo pipefail

# ----------------------------
# Config (env override)
# ----------------------------
N="${N:-100}"
DUR_CAPTURE="${DUR_CAPTURE:-30}"
IFACE_USE="${IFACE_USE:-vethB}"

# Sink IP: nsB içindeki vethB IPv4 (default: 10.10.0.3)
SINK_IP="${SINK_IP:-$(sudo ip netns exec nsB ip -4 -o addr show dev vethB | awk '{print $4}' | cut -d/ -f1 | head -n1)}"
BENIGN_PORT="${BENIGN_PORT:-18082}"
BURST_PORT="${BURST_PORT:-18080}"
SLOW_PORT="${SLOW_PORT:-18081}"

BENIGN_CONN="${BENIGN_CONN:-800}"
BENIGN_CONC="${BENIGN_CONC:-40}"

BURST_CONN="${BURST_CONN:-1200}"
BURST_CONC="${BURST_CONC:-60}"

SLOW_SECS="${SLOW_SECS:-60}"
SLOW_INTERVAL="${SLOW_INTERVAL:-0.5}"
SLOW_BYTES="${SLOW_BYTES:-256}"

ROOT="${ROOT:-$HOME/work/mycelium_ids}"
CAP_SCRIPT="${CAP_SCRIPT:-$ROOT/scripts/lab_capture_four_phase_safe.sh}"

# ----------------------------
# Run dir
# ----------------------------
TS="${TS:-$(date +%Y%m%d_%H%M%S)}"
RUN="${RUN:-$ROOT/runs/$TS}"
PHASE_DIR="${PHASE_DIR:-$RUN/phases_safe_multi}"

mkdir -p "$RUN" "$PHASE_DIR"

echo "$RUN" > "$ROOT/runs/_last_run_four_phase.txt"

LOG_MAIN="$RUN/lab_capture_four_phase.log"
LOG_BG="$RUN/bg_100.log"

# ----------------------------
# Helpers
# ----------------------------
need() { command -v "$1" >/dev/null 2>&1 || { echo "[ERR] missing cmd: $1"; exit 2; }; }

need sudo
need bash
need date

# sudo non-interactive check (nohup'ta patlamasın)
if ! sudo -n true 2>/dev/null; then
  echo "[ERR] sudo needs password. Run once: sudo -v  (then re-run this script)" | tee -a "$LOG_MAIN"
  exit 2
fi

# Make sure netns exist
if ! sudo ip netns list | grep -qE '^nsB'; then
  echo "[ERR] nsB netns missing." | tee -a "$LOG_MAIN"
  exit 2
fi

# ----------------------------
# Start
# ----------------------------
{
  echo "[INFO] start $(date -Is)"
  echo "[INFO] RUN=$RUN"
  echo "[INFO] N=$N IFACE_USE=$IFACE_USE DUR_CAPTURE=$DUR_CAPTURE"
  echo "[INFO] PHASE_DIR=$PHASE_DIR"
  echo "[INFO] SINK_IP=$SINK_IP ports benign=$BENIGN_PORT burst=$BURST_PORT slow=$SLOW_PORT"
  echo "[INFO] script=$CAP_SCRIPT"
} | tee -a "$LOG_MAIN"

ok=0; fail=0

for i in $(seq 1 "$N"); do
  {
    echo "=============================="
    echo "[ITER $i/$N] $(date -Is)"
  } | tee -a "$LOG_MAIN"

  # Her iter için env ile phase_dir’i RUN içine yönlendiriyoruz
  if PHASE_DIR="$PHASE_DIR" \
     IFACE_USE="$IFACE_USE" DUR_CAPTURE="$DUR_CAPTURE" \
     SINK_IP="$SINK_IP" \
     BENIGN_PORT="$BENIGN_PORT" BURST_PORT="$BURST_PORT" SLOW_PORT="$SLOW_PORT" \
     BENIGN_CONN="$BENIGN_CONN" BENIGN_CONC="$BENIGN_CONC" \
     BURST_CONN="$BURST_CONN" BURST_CONC="$BURST_CONC" \
     SLOW_SECS="$SLOW_SECS" SLOW_INTERVAL="$SLOW_INTERVAL" SLOW_BYTES="$SLOW_BYTES" \
     bash "$CAP_SCRIPT" "$i" >"$RUN/iter_${i}.log" 2>&1
  then
    ok=$((ok+1))
    echo "[OK] iter=$i" | tee -a "$LOG_MAIN"
  else
    fail=$((fail+1))
    echo "[FAIL] iter=$i (see $RUN/iter_${i}.log)" | tee -a "$LOG_MAIN"
  fi

  cnt=$(ls -1 "$PHASE_DIR"/*.csv 2>/dev/null | wc -l | tr -d ' ')
  echo "[INFO] csv_count=$cnt ok=$ok fail=$fail" | tee -a "$LOG_MAIN"
done

cnt=$(ls -1 "$PHASE_DIR"/*.csv 2>/dev/null | wc -l | tr -d ' ')
{
  echo "[DONE] ok=$ok fail=$fail csv_count=$cnt"
  echo "[DONE] end $(date -Is)"
} | tee -a "$LOG_MAIN"
