#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$HOME/work/mycelium_ids}"

echo "== TOP ($ROOT) =="
ls -lh "$ROOT" | sed -n '1,200p'

echo
echo "== SCRIPTS ($ROOT/scripts) =="
ls -lh "$ROOT/scripts" | sed -n '1,200p'

echo
echo "== LAB/FLOWS ($ROOT/lab/flows) =="
ls -lh "$ROOT/lab/flows" | sed -n '1,200p'
echo
echo "-- subdirs (maxdepth=2) --"
find "$ROOT/lab/flows" -maxdepth 2 -type d -print | sed -n '1,200p'

echo
echo "== FIGURES ($ROOT/figures) =="
ls -lh "$ROOT/figures" 2>/dev/null | sed -n '1,200p' || echo "[WARN] no figures dir"

echo
echo "== RUNS (latest 5) =="
ls -dt "$ROOT/runs"/20* 2>/dev/null | head -n 5 || true

echo
echo "== RUN TREE (latest run) =="
LAST="$(ls -dt "$ROOT/runs"/20* 2>/dev/null | head -n 1 || true)"
echo "LAST=$LAST"
if [[ -n "${LAST}" && -d "${LAST}" ]]; then
  find "$LAST" -maxdepth 2 -type f -printf "%TY-%Tm-%Td %TH:%TM  %9s  %p\n" | sort | sed -n '1,200p'
else
  echo "[WARN] no runs found"
fi

echo
echo "== PHASE DIR COUNTS =="
for d in phases phases3 phases_safe phases_safe_multi phases_safe_multi_node; do
  p="$ROOT/lab/flows/$d"
  if [[ -d "$p" ]]; then
    c="$(ls -1 "$p"/*.csv 2>/dev/null | wc -l || true)"
    echo "$p : csv_count=$c"
  fi
done
