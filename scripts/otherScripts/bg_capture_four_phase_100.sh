#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   IFACE_USE=vethB DUR_CAPTURE=30 bash scripts/bg_capture_four_phase_100.sh
#
# Output:
#   runs/<TS>/bg_100.pid
#   runs/<TS>/lab_capture_four_phase.log
#   runs/<TS>/iter_*.log

ROOT="$HOME/work/mycelium_ids"
RUNS="$ROOT/runs"
TS="$(date +%Y%m%d_%H%M%S)"
RUN="$RUNS/$TS"
mkdir -p "$RUN"

SCRIPT="$ROOT/scripts/lab_capture_four_phase_safe.sh"
PHASE_DIR="$ROOT/lab/flows/phases_safe_multi"

# ---- hard checks ----
if [ ! -x "$SCRIPT" ]; then
  echo "[ERR] missing executable: $SCRIPT" | tee "$RUN/lab_capture_four_phase.log"
  exit 2
fi

# Arka planda sudo parola soramaz. O yüzden burada daha baştan kontrol ediyoruz.
if ! sudo -n true 2>/dev/null; then
  echo "[ERR] sudo needs a password. Run these ONCE in your terminal, then retry:" | tee "$RUN/lab_capture_four_phase.log"
  echo "  sudo -v" | tee -a "$RUN/lab_capture_four_phase.log"
  echo "  # (then) IFACE_USE=vethB DUR_CAPTURE=30 bash scripts/bg_capture_four_phase_100.sh" | tee -a "$RUN/lab_capture_four_phase.log"
  exit 3
fi

echo "[INFO] start $(date -Is)" | tee "$RUN/lab_capture_four_phase.log"
echo "[INFO] RUN=$RUN" | tee -a "$RUN/lab_capture_four_phase.log"
echo "[INFO] script=$SCRIPT" | tee -a "$RUN/lab_capture_four_phase.log"
echo "[INFO] IFACE_USE=${IFACE_USE:-vethB} DUR_CAPTURE=${DUR_CAPTURE:-30}" | tee -a "$RUN/lab_capture_four_phase.log"

# Eski kalıntılar yüzünden csv_count şişmesin diye sadece "bozuk iter"leri ayıklamak yeterli.
# İstersen tamamen sıfırdan başlatmak için alttaki rm satırını aç:
# rm -f "$PHASE_DIR"/*.csv 2>/dev/null || true

ok=0; fail=0
for i in $(seq 1 100); do
  echo "==============================" | tee -a "$RUN/lab_capture_four_phase.log"
  echo "[ITER $i/100] $(date -Is)" | tee -a "$RUN/lab_capture_four_phase.log"
  if IFACE_USE="${IFACE_USE:-vethB}" DUR_CAPTURE="${DUR_CAPTURE:-30}" bash "$SCRIPT" "$i" >"$RUN/iter_${i}.log" 2>&1; then
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
