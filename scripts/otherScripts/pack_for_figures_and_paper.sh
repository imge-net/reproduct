#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$HOME/work/mycelium_ids}"
RUNS="$ROOT/runs"
SCRIPTS="$ROOT/scripts"
OUTDIR="$ROOT/bundles"
TS="$(date +%Y%m%d_%H%M%S)"
OUT="$OUTDIR/paper_plus_fig_inputs_${TS}.zip"
mkdir -p "$OUTDIR"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

log(){ echo "[INFO] $*"; }
warn(){ echo "[WARN] $*" >&2; }

add_file(){
  local p="$1"
  if [[ -f "$p" ]]; then
    mkdir -p "$tmp/$(dirname "${p#$ROOT/}")" 2>/dev/null || true
    cp -f "$p" "$tmp/${p#$ROOT/}"
  else
    warn "missing: $p"
  fi
}

add_latest(){
  local g="$1"
  local p
  p="$(ls -t $g 2>/dev/null | head -n 1 || true)"
  [[ -n "${p:-}" ]] && add_file "$p" || warn "no match: $g"
}

add_all(){
  local g="$1"
  shopt -s nullglob
  for p in $g; do [[ -f "$p" ]] && add_file "$p"; done
  shopt -u nullglob
}

log "collecting environment + key scripts..."
add_file "$ROOT/environment.yml"
add_file "$ROOT/conda_list.txt"

add_file "$SCRIPTS/cicids_prepare.py"
add_file "$SCRIPTS/cicids_baseline.py"
add_file "$SCRIPTS/cicids_binary_tune.py"
add_file "$SCRIPTS/lab_big_eval_tabular.py"
add_file "$SCRIPTS/bootstrap_ci_score.py"
add_file "$SCRIPTS/bootstrap_ci_ext.py"
add_file "$SCRIPTS/run_mycelium_scale_portserver.sh" || true

log "collecting UNSW outputs..."
add_latest "$RUNS/*/unsw_binary_rf_ci.txt"
add_latest "$RUNS/*/unsw_binary.txt"
add_latest "$RUNS/*/unsw_baseline.txt"

log "collecting CICIDS No-Monday outputs..."
add_latest "$RUNS/*/cicids_rf_tune_no_monday_f1_ci_score.txt"
add_latest "$RUNS/*/cicids_rf_tune_no_monday_f1_ci_pred.txt"
add_latest "$RUNS/*/cicids_gbdt_no_monday_ci_score.txt"
add_latest "$RUNS/*/cicids_lgbm_no_monday_ci_score.txt"
add_latest "$RUNS/*/cicids_shift_diag_wed_vs_fri.txt"

log "collecting CICIDS preds (for figures)..."
add_latest "$RUNS/*/cicids_binary_rf_tuned.parquet"
add_latest "$RUNS/*/cicids_lgbm_no_monday_pred.parquet"

log "collecting Mycelium outputs + preds (for figures)..."
add_latest "$RUNS/*/lab_big_eval_tabular_n5873.txt"
add_latest "$RUNS/*/lab_rf_ci_score.txt"
add_latest "$RUNS/*/lab_hgbdt_ci_score.txt"
add_latest "$RUNS/*/lab_lgbm_ci_score.txt"
add_latest "$RUNS/*/lab_rf_pred.parquet"
add_latest "$RUNS/*/ports_rf_ci_score.txt"
add_latest "$RUNS/*/ports_rf_pred.parquet"
add_latest "$RUNS/*/window1_run.log"
add_latest "$RUNS/*/lab_dataset_window1.parquet"

log "writing MANIFEST..."
(
  echo "# paper_plus_fig_inputs manifest"
  echo "# ROOT=$ROOT"
  echo "# generated: $(date -Is)"
  echo
  find "$tmp" -type f | sed "s|$tmp/||" | sort
) > "$tmp/MANIFEST.txt"

log "packing: $OUT"
(cd "$tmp" && zip -qr "$OUT" .)

echo "[OK] wrote $OUT"
ls -lh "$OUT"
