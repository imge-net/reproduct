#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$HOME/work/mycelium_ids}"
RUNS="${RUNS:-$ROOT/runs}"
SCRIPT="${SCRIPT:-$ROOT/scripts/lab_capture_four_phase_safe.sh}"

N="${N:-100}"
CLEAN="${CLEAN:-1}"

IFACE_USE="${IFACE_USE:-vethB}"
DUR_CAPTURE="${DUR_CAPTURE:-30}"

# Optional tuning knobs:
BENIGN_CONN="${BENIGN_CONN:-800}"
BENIGN_CONC="${BENIGN_CONC:-40}"
EXFIL_BURST_CONN="${EXFIL_BURST_CONN:-1200}"
EXFIL_BURST_CONC="${EXFIL_BURST_CONC:-60}"
EXFIL_SLOW_SECS="${EXFIL_SLOW_SECS:-60}"
EXFIL_SLOW_INTERVAL="${EXFIL_SLOW_INTERVAL:-0.5}"
EXFIL_SLOW_BYTES="${EXFIL_SLOW_BYTES:-256}"

TS="${TS:-$(date +%Y%m%d_%H%M%S)}"
RUN="${RUNS}/${TS}"
mkdir -p "$RUN"

# Remember last run path
echo "$RUN" > "$RUNS/_last_run_four_phase.txt"

echo "[INFO] start $(date -Is)" | tee -a "$RUN/lab_capture_four_phase.log"
echo "[INFO] RUN=$RUN"         | tee -a "$RUN/lab_capture_four_phase.log"
echo "[INFO] script=$SCRIPT"   | tee -a "$RUN/lab_capture_four_phase.log"
echo "[INFO] N=$N IFACE_USE=$IFACE_USE DUR_CAPTURE=$DUR_CAPTURE" | tee -a "$RUN/lab_capture_four_phase.log"

# 1) sudo must be non-interactive
if ! sudo -n true 2>/dev/null; then
  echo "[ERR] sudo needs a password (non-interactive). Run once interactively: sudo -v" | tee -a "$RUN/lab_capture_four_phase.log"
  exit 10
fi

# 2) netns sanity
for ns in nsA nsB nsC; do
  if ! sudo ip netns exec "$ns" true 2>/dev/null; then
    echo "[ERR] missing netns: $ns" | tee -a "$RUN/lab_capture_four_phase.log"
    exit 11
  fi
done

# 3) clean phase dir if requested
PHASE_DIR="$ROOT/lab/flows/phases_safe_multi"
mkdir -p "$PHASE_DIR"
if [[ "$CLEAN" == "1" ]]; then
  rm -f "$PHASE_DIR"/*.csv 2>/dev/null || true
fi

ok=0; fail=0
for i in $(seq 1 "$N"); do
  echo "==============================" | tee -a "$RUN/lab_capture_four_phase.log"
  echo "[ITER $i/$N] $(date -Is)"      | tee -a "$RUN/lab_capture_four_phase.log"

  # Refresh sudo timestamp (non-interactive). If it fails, stop early.
  if ! sudo -n true 2>/dev/null; then
    echo "[FAIL] iter=$i: sudo timestamp expired (run: sudo -v)" | tee -a "$RUN/lab_capture_four_phase.log"
    fail=$((fail+1))
    break
  fi

  if env IFACE_USE="$IFACE_USE" DUR_CAPTURE="$DUR_CAPTURE" \
      BENIGN_CONN="$BENIGN_CONN" BENIGN_CONC="$BENIGN_CONC" \
      EXFIL_BURST_CONN="$EXFIL_BURST_CONN" EXFIL_BURST_CONC="$EXFIL_BURST_CONC" \
      EXFIL_SLOW_SECS="$EXFIL_SLOW_SECS" EXFIL_SLOW_INTERVAL="$EXFIL_SLOW_INTERVAL" EXFIL_SLOW_BYTES="$EXFIL_SLOW_BYTES" \
      bash "$SCRIPT" "$i" >"$RUN/iter_${i}.log" 2>&1; then
    ok=$((ok+1))
    echo "[OK] iter=$i" | tee -a "$RUN/lab_capture_four_phase.log"
  else
    fail=$((fail+1))
    echo "[FAIL] iter=$i (see $RUN/iter_${i}.log)" | tee -a "$RUN/lab_capture_four_phase.log"
  fi
done

count=$(ls -1 "$PHASE_DIR"/*.csv 2>/dev/null | wc -l || true)
echo "[DONE] ok=$ok fail=$fail csv_count=$count" | tee -a "$RUN/lab_capture_four_phase.log"
echo "[DONE] end $(date -Is)" | tee -a "$RUN/lab_capture_four_phase.log"

echo "[OK] RUN=$RUN"
