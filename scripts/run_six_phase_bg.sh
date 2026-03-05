#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$HOME/work/mycelium_ids}"

RUN="${RUN:?RUN is required}"
N="${N:-50}"
IFACE_USE="${IFACE_USE:-vethB}"
DUR_CAPTURE="${DUR_CAPTURE:-30}"
SINK_IP="${SINK_IP:-10.10.0.3}"

PHASE_DIR="${PHASE_DIR:-$RUN/phases_six_safe}"
mkdir -p "$RUN" "$PHASE_DIR"

LOG="$RUN/six_phase.log"
: > "$LOG"  # create/clear immediately so tail -f works

echo "[INFO] start $(date -Is)"            | tee -a "$LOG"
echo "[INFO] RUN=$RUN"                     | tee -a "$LOG"
echo "[INFO] N=$N IFACE_USE=$IFACE_USE DUR_CAPTURE=$DUR_CAPTURE SINK_IP=$SINK_IP" | tee -a "$LOG"
echo "[INFO] PHASE_DIR=$PHASE_DIR"         | tee -a "$LOG"
echo "[INFO] script=$ROOT/scripts/lab_capture_six_phase_safe.sh" | tee -a "$LOG"

ok=0; fail=0
for i in $(seq 1 "$N"); do
  echo "==============================" | tee -a "$LOG"
  echo "[ITER $i/$N] $(date -Is)"       | tee -a "$LOG"

  if ROOT="$ROOT" RUN="$RUN" PHASE_DIR="$PHASE_DIR" IFACE_USE="$IFACE_USE" DUR_CAPTURE="$DUR_CAPTURE" SINK_IP="$SINK_IP" \
      bash "$ROOT/scripts/lab_capture_six_phase_safe.sh" "$i" >"$RUN/iter_${i}.log" 2>&1; then
    ok=$((ok+1))
    echo "[OK] iter=$i" | tee -a "$LOG"
  else
    fail=$((fail+1))
    echo "[FAIL] iter=$i (see $RUN/iter_${i}.log)" | tee -a "$LOG"
    tail -n 20 "$RUN/iter_${i}.log" | sed 's/^/[iter-tail] /' | tee -a "$LOG" || true
  fi

  c="$(ls -1 "$PHASE_DIR"/*.csv 2>/dev/null | wc -l || true)"
  echo "[INFO] csv_count=$c ok=$ok fail=$fail" | tee -a "$LOG"
done

c="$(ls -1 "$PHASE_DIR"/*.csv 2>/dev/null | wc -l || true)"
echo "[DONE] ok=$ok fail=$fail csv_count=$c" | tee -a "$LOG"
echo "[DONE] end $(date -Is)" | tee -a "$LOG"
