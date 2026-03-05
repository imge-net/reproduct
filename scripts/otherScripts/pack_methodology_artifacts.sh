#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$HOME/work/mycelium_ids}"
RUNS="$ROOT/runs"
SCRIPTS="$ROOT/scripts"
OUTDIR="$ROOT/bundles"
TS="$(date +%Y%m%d_%H%M%S)"
OUTBASE="methodology_artifacts_${TS}"
MODE="${MODE:-zip}"   # zip | tar
mkdir -p "$OUTDIR"

tmp="$(mktemp -d)"
cleanup() { rm -rf "$tmp"; }
trap cleanup EXIT

log() { echo "[INFO] $*"; }
warn() { echo "[WARN] $*" >&2; }

# ---- helpers ----
add_file() {
  local p="$1"
  if [[ -f "$p" ]]; then
    mkdir -p "$tmp/$(dirname "${p#$ROOT/}")" 2>/dev/null || true
    cp -f "$p" "$tmp/${p#$ROOT/}"
  else
    warn "missing: $p"
  fi
}

add_glob_first() {
  # copy most recent match for given glob
  local g="$1"
  local p
  p="$(ls -t $g 2>/dev/null | head -n 1 || true)"
  if [[ -n "${p:-}" && -f "$p" ]]; then
    add_file "$p"
  else
    warn "no match: $g"
  fi
}

add_glob_all() {
  local g="$1"
  local p
  shopt -s nullglob
  for p in $g; do
    [[ -f "$p" ]] && add_file "$p"
  done
  shopt -u nullglob
}

# ---- scripts (proof of protocol) ----
log "collecting key scripts..."
add_file "$SCRIPTS/cicids_baseline.py"
add_file "$SCRIPTS/cicids_binary_tune.py"
add_file "$SCRIPTS/cicids_prepare.py"
add_file "$SCRIPTS/lab_big_eval_tabular.py"
add_file "$SCRIPTS/op_points.py"
add_file "$SCRIPTS/bootstrap_ci_score.py"
add_file "$SCRIPTS/bootstrap_ci_ext.py"
add_file "$SCRIPTS/make_budget_pred_from_scores.py" || true

# ---- environment / versions ----
log "collecting environment info..."
add_file "$ROOT/environment.yml"
add_file "$ROOT/conda_list.txt"

# ---- CICIDS: No-Monday + shift diagnostics (most recent) ----
log "collecting CICIDS artifacts..."
add_glob_first "$RUNS/*/cicids_rf_tune_no_monday_f1_ci_score.txt"
add_glob_first "$RUNS/*/cicids_rf_tune_no_monday_f1_ci_pred.txt"
add_glob_first "$RUNS/*/cicids_lgbm_no_monday_ci_score.txt"
add_glob_first "$RUNS/*/cicids_gbdt_no_monday_ci_score.txt"
add_glob_first "$RUNS/*/cicids_shift_diag_wed_vs_fri.txt"

# also include random-split sanity checks if present
add_glob_first "$RUNS/*/cicids_*random*_ci_score.txt"

# ---- UNSW baseline CI ----
log "collecting UNSW artifacts..."
add_glob_first "$RUNS/*/unsw_binary_rf_ci.txt"
add_glob_first "$RUNS/*/unsw_binary.txt"
add_glob_first "$RUNS/*/unsw_baseline.txt"

# ---- Mycelium: scaled dataset log + eval + CI ----
log "collecting Mycelium scaled artifacts..."
add_glob_first "$RUNS/*/run_mycelium_scale.log"
add_glob_first "$RUNS/*/window1_run.log"
add_glob_first "$RUNS/*/lab_big_eval_tabular_n5873.txt"
add_glob_first "$RUNS/*/lab_big_eval_tabular_n1435.txt"

# portable preds + ci
add_glob_first "$RUNS/*/lab_rf_pred.parquet"
add_glob_first "$RUNS/*/lab_hgbdt_pred.parquet"
add_glob_first "$RUNS/*/lab_lgbm_pred.parquet"
add_glob_first "$RUNS/*/lab_rf_ci_score.txt"
add_glob_first "$RUNS/*/lab_hgbdt_ci_score.txt"
add_glob_first "$RUNS/*/lab_lgbm_ci_score.txt"

# ports diagnostic (if exists)
log "collecting Mycelium +ports diagnostic artifacts..."
add_glob_first "$RUNS/*/lab_ports_eval.txt"
add_glob_first "$RUNS/*/ports_rf_ci_score.txt"
add_glob_first "$RUNS/*/ports_hgbdt_ci_score.txt"
add_glob_first "$RUNS/*/ports_lgbm_ci_score.txt"
add_glob_first "$RUNS/*/ports_rf_pred.parquet"
add_glob_first "$RUNS/*/ports_hgbdt_pred.parquet"
add_glob_first "$RUNS/*/ports_lgbm_pred.parquet"

# budget CI (portable) if present
log "collecting Mycelium budget CI artifacts..."
add_glob_first "$RUNS/*/portable_rf_budget_fpr01_ci_pred.txt"
add_glob_first "$RUNS/*/portable_rf_budget_fpr05_ci_pred.txt"
add_glob_first "$RUNS/*/ports_rf_budget_fpr01_ci_pred.txt"
add_glob_first "$RUNS/*/ports_rf_budget_fpr05_ci_pred.txt"

# window ablation bundle if exists
log "collecting window ablation bundle..."
add_glob_first "$ROOT/bundles/window_ablation_*.zip"

# ---- Mycelium single-shot orientation proof ----
log "collecting Mycelium single-shot orientation artifacts..."
add_glob_first "$RUNS/*/mycelium_single_shot_point.txt"
add_glob_first "$RUNS/*/mycelium_single_shot_ci_score.txt"
add_glob_first "$RUNS/*/mycelium_single_shot_test.parquet"
add_glob_first "$RUNS/*/mycelium_single_shot_calib.parquet"

# ---- manifest ----
log "writing MANIFEST..."
(
  echo "# Manifest for $OUTBASE"
  echo "# ROOT=$ROOT"
  echo "# Generated: $(date -Is)"
  echo
  find "$tmp" -type f | sed "s|$tmp/||" | sort
) > "$tmp/MANIFEST.txt"

# ---- pack ----
OUT="$OUTDIR/$OUTBASE"
log "packing -> $OUT ($MODE)"
if [[ "$MODE" == "tar" ]]; then
  tar -C "$tmp" -czf "${OUT}.tar.gz" .
  echo "[OK] wrote ${OUT}.tar.gz"
  ls -lh "${OUT}.tar.gz"
else
  (cd "$tmp" && zip -qr "${OUT}.zip" .)
  echo "[OK] wrote ${OUT}.zip"
  ls -lh "${OUT}.zip"
fi
