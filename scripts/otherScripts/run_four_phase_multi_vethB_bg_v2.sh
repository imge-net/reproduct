#!/usr/bin/env bash
set -euo pipefail

# --- config (env override allowed) ---
N="${N:-100}"
CLEAN="${CLEAN:-1}"

IFACE_USE="${IFACE_USE:-vethB}"
DUR_CAPTURE="${DUR_CAPTURE:-30}"

PHASE_DIR="${PHASE_DIR:-$HOME/work/mycelium_ids/lab/flows/phases_safe_multi}"
SCRIPT="${SCRIPT:-$HOME/work/mycelium_ids/scripts/lab_capture_four_phase_safe.sh}"

# sink nsB vethB IPv4 (default)
SINK_IP="${SINK_IP:-10.10.0.3}"

# ports
PORT_BENIGN="${PORT_BENIGN:-18082}"
PORT_BURST="${PORT_BURST:-18080}"
PORT_SLOW="${PORT_SLOW:-18081}"

# traffic params (env override)
BENIGN_CONN="${BENIGN_CONN:-800}"
BENIGN_CONC="${BENIGN_CONC:-40}"

BURST_CONN="${BURST_CONN:-1200}"
BURST_CONC="${BURST_CONC:-60}"

SLOW_SECS="${SLOW_SECS:-60}"
SLOW_INTERVAL="${SLOW_INTERVAL:-0.5}"
SLOW_BYTES="${SLOW_BYTES:-256}"

RUN="${RUN:-}"
TS="${TS:-}"

if [[ -z "${RUN}" || -z "${TS}" ]]; then
  TS="$(date +%Y%m%d_%H%M%S)"
  RUN="$HOME/work/mycelium_ids/runs/$TS"
fi

mkdir -p "$RUN"
mkdir -p "$PHASE_DIR"

LOG_MAIN="$RUN/lab_capture_four_phase.log"
LOG_BG="$RUN/bg_100.log"
echo "$RUN" > "$HOME/work/mycelium_ids/runs/_last_run_four_phase.txt"

if [[ "$CLEAN" == "1" ]]; then
  rm -f "$PHASE_DIR"/*.csv 2>/dev/null || true
fi

echo "[INFO] start $(date -Is)" | tee -a "$LOG_MAIN"
echo "[INFO] RUN=$RUN"          | tee -a "$LOG_MAIN"
echo "[INFO] N=$N IFACE_USE=$IFACE_USE DUR_CAPTURE=$DUR_CAPTURE" | tee -a "$LOG_MAIN"
echo "[INFO] PHASE_DIR=$PHASE_DIR" | tee -a "$LOG_MAIN"
echo "[INFO] SINK_IP=$SINK_IP ports benign=$PORT_BENIGN burst=$PORT_BURST slow=$PORT_SLOW" | tee -a "$LOG_MAIN"

ok=0; fail=0

for i in $(seq 1 "$N"); do
  echo "==============================" | tee -a "$LOG_MAIN"
  echo "[ITER $i/$N] $(date -Is)"      | tee -a "$LOG_MAIN"

  # lab_capture_four_phase_safe.sh env contract:
  # - IFACE_USE, DUR_CAPTURE, SINK_IP
  # - PORT_BENIGN, PORT_BURST, PORT_SLOW
  # - BENIGN_CONN, BENIGN_CONC, BURST_CONN, BURST_CONC
  # - SLOW_SECS, SLOW_INTERVAL, SLOW_BYTES
  if IFACE_USE="$IFACE_USE" DUR_CAPTURE="$DUR_CAPTURE" SINK_IP="$SINK_IP" \
      PORT_BENIGN="$PORT_BENIGN" PORT_BURST="$PORT_BURST" PORT_SLOW="$PORT_SLOW" \
      BENIGN_CONN="$BENIGN_CONN" BENIGN_CONC="$BENIGN_CONC" \
      BURST_CONN="$BURST_CONN" BURST_CONC="$BURST_CONC" \
      SLOW_SECS="$SLOW_SECS" SLOW_INTERVAL="$SLOW_INTERVAL" SLOW_BYTES="$SLOW_BYTES" \
      bash "$SCRIPT" "$i" >"$RUN/iter_${i}.log" 2>&1; then
    ok=$((ok+1))
    echo "[OK] iter=$i" | tee -a "$LOG_MAIN"
  else
    fail=$((fail+1))
    echo "[FAIL] iter=$i (see $RUN/iter_${i}.log)" | tee -a "$LOG_MAIN"
  fi

  count=$(ls -1 "$PHASE_DIR"/*.csv 2>/dev/null | wc -l || true)
  echo "[INFO] csv_count=$count ok=$ok fail=$fail" | tee -a "$LOG_MAIN"
done

count=$(ls -1 "$PHASE_DIR"/*.csv 2>/dev/null | wc -l || true)
echo "[DONE] ok=$ok fail=$fail csv_count=$count" | tee -a "$LOG_MAIN"
echo "[DONE] end $(date -Is)" | tee -a "$LOG_MAIN"
