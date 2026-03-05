#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$HOME/work/mycelium_ids}"
RUNS="$ROOT/runs"
BUNDLES="$ROOT/bundles"
TS="$(date +%Y%m%d_%H%M%S)"

OUT_BASE="${OUT_BASE:-paper_artifacts_${TS}}"
OUT_ZIP="$BUNDLES/${OUT_BASE}.zip"
OUT_TGZ="$BUNDLES/${OUT_BASE}.tar.gz"
MODE="${MODE:-zip}"   # zip | tgz

mkdir -p "$BUNDLES"

need() { command -v "$1" >/dev/null 2>&1 || { echo "[ERR] missing: $1"; exit 1; }; }
need find
need sed
need awk
need sort

if [[ "$MODE" == "zip" ]]; then
  need zip
else
  need tar
fi

echo "[INFO] ROOT=$ROOT"
echo "[INFO] RUNS=$RUNS"
echo "[INFO] MODE=$MODE"
echo "[INFO] OUT_BASE=$OUT_BASE"

tmpdir="$(mktemp -d)"
manifest="$tmpdir/MANIFEST.txt"
touch "$manifest"

add_file() {
  local f="$1"
  [[ -f "$f" ]] || return 0
  # copy preserving relative path under ROOT if possible
  local rel
  if [[ "$f" == "$ROOT/"* ]]; then
    rel="${f#$ROOT/}"
    mkdir -p "$tmpdir/$(dirname "$rel")"
    cp -f "$f" "$tmpdir/$rel"
    echo "$rel <- $f" >> "$manifest"
  else
    # fallback
    local base
    base="$(basename "$f")"
    mkdir -p "$tmpdir/_external"
    cp -f "$f" "$tmpdir/_external/$base"
    echo "_external/$base <- $f" >> "$manifest"
  fi
}

add_glob_latest() {
  # pick the newest match across runs
  local pattern="$1"
  local f
  f="$(ls -t $RUNS/*/$pattern 2>/dev/null | head -n 1 || true)"
  [[ -n "$f" ]] && add_file "$f"
}

add_glob_all() {
  local pattern="$1"
  local f
  while IFS= read -r f; do
    add_file "$f"
  done < <(ls -t $RUNS/*/$pattern 2>/dev/null || true)
}

echo "[INFO] collecting key result logs / CI..."

# --- Mycelium main (n=5873) and big eval
add_glob_latest "lab_big_eval_tabular_n5873.txt"
add_glob_latest "lab_big_eval_tabular*.txt"
add_glob_latest "lab_rf_ci_score.txt"
add_glob_latest "lab_hgbdt_ci_score.txt"
add_glob_latest "lab_lgbm_ci_score.txt"
add_glob_latest "lab_rf_pred.parquet"
add_glob_latest "lab_hgbdt_pred.parquet"
add_glob_latest "lab_lgbm_pred.parquet"

# --- +ports diagnostic (portable/ports CI)
add_glob_latest "lab_ports_eval.txt"
add_glob_latest "portable_rf_ci_score.txt"
add_glob_latest "portable_hgbdt_ci_score.txt"
add_glob_latest "portable_lgbm_ci_score.txt"
add_glob_latest "ports_rf_ci_score.txt"
add_glob_latest "ports_hgbdt_ci_score.txt"
add_glob_latest "ports_lgbm_ci_score.txt"

# --- window ablation bundles (zip)
# if you already have a window_ablation zip under bundles, grab the newest
if ls -t "$BUNDLES"/window_ablation_*.zip >/dev/null 2>&1; then
  add_file "$(ls -t "$BUNDLES"/window_ablation_*.zip | head -n 1)"
fi

# --- UNSW / CICIDS baseline files (grab newest if exist)
add_glob_latest "unsw_binary_rf_ci.txt"
add_glob_latest "unsw_binary*.txt"
add_glob_latest "unsw_baseline.txt"

add_glob_latest "cicids_rf_tune_no_monday_f1_ci.txt"
add_glob_latest "cicids_rf_tune_no_monday_f1.txt"
add_glob_latest "cicids_gbdt_no_monday_ci_score.txt"
add_glob_latest "cicids_lgbm_no_monday_ci_score.txt"
add_glob_latest "cicids_*_no_monday*_ci_score.txt"
add_glob_latest "cicids_*_random*_ci_score.txt"
add_glob_latest "cicids_*_test_*_ci_score.txt"
add_glob_latest "cicids_shift_diag_*.txt"

# --- scripts snapshot (reproducibility)
echo "[INFO] collecting scripts snapshot list..."
mkdir -p "$tmpdir/repro"
( cd "$ROOT" && find scripts -maxdepth 1 -type f -printf "%P\n" | sort ) > "$tmpdir/repro/scripts_list.txt"
echo "repro/scripts_list.txt <- $ROOT/scripts (listing)" >> "$manifest"

# capture key scripts (if exist)
for s in \
  scripts/run_mycelium_scale_portserver.sh \
  scripts/export_lab_flows_pktagg.sh \
  scripts/lab_big_eval_tabular.py \
  scripts/lab_big_eval_tabular_ports.py \
  scripts/bootstrap_ci_score.py \
  scripts/bootstrap_ci.py \
  scripts/bootstrap_ci_ext.py \
  scripts/op_points.py \
  scripts/oracle_threshold.py; do
  [[ -f "$ROOT/$s" ]] && add_file "$ROOT/$s"
done

# environment hints (optional)
[[ -f "$ROOT/environment.yml" ]] && add_file "$ROOT/environment.yml"
[[ -f "$ROOT/requirements.txt" ]] && add_file "$ROOT/requirements.txt"

# add manifest at root of bundle

# sanity: show what we got
echo "[INFO] files collected:"
( cd "$tmpdir" && find . -type f | sed 's#^\./##' | sort ) | tee "$BUNDLES/${OUT_BASE}_filelist.txt" >/dev/null
echo "[OK] wrote file list: $BUNDLES/${OUT_BASE}_filelist.txt"

# pack
if [[ "$MODE" == "zip" ]]; then
  ( cd "$tmpdir" && zip -qr "$OUT_ZIP" . )
  echo "[OK] wrote: $OUT_ZIP"
  ls -lh "$OUT_ZIP"
else
  ( cd "$tmpdir" && tar -czf "$OUT_TGZ" . )
  echo "[OK] wrote: $OUT_TGZ"
  ls -lh "$OUT_TGZ"
fi

rm -rf "$tmpdir"
