#!/usr/bin/env bash
set -euo pipefail

N="${1:-100}"
ROOT="$HOME/work/mycelium_ids"
PHASE_DIR="$ROOT/lab/flows/phases_safe_multi"
SCRIPT="$ROOT/scripts/lab_capture_four_phase_safe.sh"

TS="$(date +%Y%m%d_%H%M%S)"
RUN="$ROOT/runs/$TS"
mkdir -p "$RUN"

# Temiz başlangıç (opsiyonel ama önerilir)
rm -f "$PHASE_DIR"/*.csv 2>/dev/null || true

# B_OV6'yi bir kez al (script içinde de auto var ama burada da logluyoruz)
B_OV6="$(sudo ip netns exec nsB ip -br a | awk '/myc0/ {print $3}' | cut -d/ -f1 | head -n1)"
export B_OV6
export IFACE_USE="${IFACE_USE:-vethB}"
export DUR_CAPTURE="${DUR_CAPTURE:-30}"

echo "[INFO] start $(date -Is)" | tee -a "$RUN/lab_capture_four_phase.log"
echo "[INFO] RUN=$RUN"          | tee -a "$RUN/lab_capture_four_phase.log"
echo "[INFO] script=$SCRIPT"    | tee -a "$RUN/lab_capture_four_phase.log"
echo "[INFO] N=$N IFACE_USE=$IFACE_USE DUR_CAPTURE=$DUR_CAPTURE B_OV6=$B_OV6" | tee -a "$RUN/lab_capture_four_phase.log"

ok=0; fail=0
for i in $(seq 1 "$N"); do
  echo "==============================" | tee -a "$RUN/lab_capture_four_phase.log"
  echo "[ITER $i/$N] $(date -Is)"      | tee -a "$RUN/lab_capture_four_phase.log"
  if bash "$SCRIPT" "$i" >"$RUN/iter_${i}.log" 2>&1; then
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

echo "$RUN"
