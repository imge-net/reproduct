#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$HOME/work/mycelium_ids}"
BUNDLES="$ROOT/bundles"
mkdir -p "$BUNDLES"

echo "[INFO] ROOT=$ROOT"

LATEST_RUN="$(ls -dt "$ROOT/runs"/20* 2>/dev/null | head -n 1 || true)"
FOUR_RUN_FILE="$ROOT/runs/_last_run_four_phase.txt"
FOUR_RUN=""
if [[ -f "$FOUR_RUN_FILE" ]]; then
  FOUR_RUN="$(cat "$FOUR_RUN_FILE" || true)"
fi

# DL run = latest that has eval_dl_portable
DL_RUN="$(ls -dt "$ROOT/runs"/20*/eval_dl_portable 2>/dev/null | head -n 1 | xargs -r dirname || true)"

echo "[INFO] LATEST_RUN=$LATEST_RUN"
echo "[INFO] FOUR_RUN=$FOUR_RUN"
echo "[INFO] DL_RUN=$DL_RUN"

TS="$(date +%Y%m%d_%H%M%S)"
OUT_ZIP="$BUNDLES/paper_artifacts_plus_${TS}.zip"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

add_if_exists () {
  local src="$1"
  local dst_rel="$2"
  if [[ -e "$src" ]]; then
    mkdir -p "$TMPDIR/$(dirname "$dst_rel")"
    cp -a "$src" "$TMPDIR/$dst_rel"
  fi
}

# core env + meta
add_if_exists "$ROOT/environment.yml" "env/environment.yml"
add_if_exists "$ROOT/conda_list.txt"   "env/conda_list.txt"
add_if_exists "$ROOT/layout_report.txt" "meta/layout_report.txt"

# figures
if [[ -d "$ROOT/figures" ]]; then
  mkdir -p "$TMPDIR/figures"
  cp -a "$ROOT/figures/"* "$TMPDIR/figures/" 2>/dev/null || true
fi

# latest run artifacts
if [[ -n "$LATEST_RUN" && -d "$LATEST_RUN" ]]; then
  add_if_exists "$LATEST_RUN" "runs/latest_run"
fi

# four-phase run artifacts (only logs + key outputs)
if [[ -n "$FOUR_RUN" && -d "$FOUR_RUN" ]]; then
  mkdir -p "$TMPDIR/runs/four_phase"
  cp -a "$FOUR_RUN"/lab_capture_four_phase.log "$TMPDIR/runs/four_phase/" 2>/dev/null || true
  cp -a "$FOUR_RUN"/bg_100.log "$TMPDIR/runs/four_phase/" 2>/dev/null || true
  cp -a "$FOUR_RUN"/eval_exfil2_full_rowid "$TMPDIR/runs/four_phase/" 2>/dev/null || true
fi

# DL artifacts
if [[ -n "$DL_RUN" && -d "$DL_RUN/eval_dl_portable" ]]; then
  add_if_exists "$DL_RUN/eval_dl_portable" "runs/dl/eval_dl_portable"
  add_if_exists "$DL_RUN/dl.nohup.log"     "runs/dl/dl.nohup.log"
fi

echo "[INFO] OUT_ZIP=$OUT_ZIP"
( cd "$TMPDIR" && zip -r "$OUT_ZIP" . >/dev/null )
echo "[OK] wrote $OUT_ZIP"
ls -lh "$OUT_ZIP"
