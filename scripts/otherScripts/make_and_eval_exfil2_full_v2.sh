#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$HOME/work/mycelium_ids}"
ENV_NAME="${ENV_NAME:-ids_mycelium}"

PHASE_DIR="${PHASE_DIR:-$ROOT/lab/flows/phases_safe_multi}"
LAB_DIR="${LAB_DIR:-$ROOT/lab/flows}"

TS="${TS:-$(date +%Y%m%d_%H%M%S)}"
RUN="${RUN:-$ROOT/runs/$TS}"
OUT="${OUT:-$RUN/eval_exfil2_full_rowid}"

mkdir -p "$RUN" "$OUT"

LOG="$RUN/make_and_eval_exfil2_full_v2.log"
exec > >(tee -a "$LOG") 2>&1

echo "[INFO] ROOT=$ROOT"
echo "[INFO] PHASE_DIR=$PHASE_DIR"
echo "[INFO] LAB_DIR=$LAB_DIR"
echo "[INFO] RUN=$RUN"
echo "[INFO] OUT=$OUT"
echo "[INFO] ENV_NAME=$ENV_NAME"
echo "[INFO] LOG=$LOG"

# --- sanity checks
if [[ ! -d "$PHASE_DIR" ]]; then
  echo "[ERR] PHASE_DIR not found: $PHASE_DIR"
  exit 2
fi

CSV_TOTAL=$(ls -1 "$PHASE_DIR"/*.csv 2>/dev/null | wc -l || true)
if [[ "${CSV_TOTAL:-0}" -eq 0 ]]; then
  echo "[ERR] no CSV files in $PHASE_DIR"
  exit 2
fi
echo "[INFO] phase CSV count(total)=$CSV_TOTAL"

# filter out header-only CSVs (<=1 line)
CSV_NONEMPTY=$(for f in "$PHASE_DIR"/*.csv; do
  n=$(wc -l < "$f" || echo 0)
  if [[ "$n" -gt 1 ]]; then echo "$f"; fi
done | wc -l || true)
echo "[INFO] phase CSV count(nonempty)=$CSV_NONEMPTY"
if [[ "${CSV_NONEMPTY:-0}" -eq 0 ]]; then
  echo "[ERR] all CSVs are header-only (no flows). Abort."
  exit 2
fi

DATA_PQ="$LAB_DIR/lab_dataset_exfil2_full.parquet"
DATA_CSV="$LAB_DIR/lab_dataset_exfil2_full.csv"

echo "[INFO] building dataset -> $DATA_PQ"
conda run -n "$ENV_NAME" python - <<'PY'
import glob, os, re
import pandas as pd

PHASE_DIR=os.environ["PHASE_DIR"]
LAB_DIR=os.environ["LAB_DIR"]

pat = re.compile(r"^(benign|attack_exfil_burst|attack_exfil_lowslow)_(\d+)\.csv$")

files = sorted(glob.glob(os.path.join(PHASE_DIR, "*.csv")))
dfs=[]
skipped=0
kept=0

for fp in files:
    base=os.path.basename(fp)
    m=pat.match(base)
    if not m:
        skipped += 1
        continue
    attack_type=m.group(1)
    rep=int(m.group(2))
    # skip header-only
    try:
        if sum(1 for _ in open(fp, "r", encoding="utf-8", errors="ignore")) <= 1:
            skipped += 1
            continue
    except Exception:
        skipped += 1
        continue

    df=pd.read_csv(fp)
    df.columns=[c.strip() for c in df.columns]

    # ensure y_true
    if "y_true" not in df.columns:
        df["y_true"] = 0 if attack_type=="benign" else 1
    # attach metadata
    df["attack_type"]=attack_type
    df["rep"]=rep
    dfs.append(df)
    kept += 1

if not dfs:
    raise SystemExit(f"[ERR] no usable CSV rows found in {PHASE_DIR} (kept=0, skipped={skipped})")

all_df=pd.concat(dfs, ignore_index=True)

out_pq=os.path.join(LAB_DIR,"lab_dataset_exfil2_full.parquet")
out_csv=os.path.join(LAB_DIR,"lab_dataset_exfil2_full.csv")
all_df.to_parquet(out_pq, index=False)
all_df.to_csv(out_csv, index=False)

print("[OK] kept_files=", kept, "skipped_files=", skipped)
print("[OK] wrote", out_pq)
print("[OK] rows=", len(all_df), "pos_rate=", float(all_df["y_true"].mean()))
print(all_df["attack_type"].value_counts().to_string())
print("rep min/max:", int(all_df["rep"].min()), int(all_df["rep"].max()))
PY

if [[ ! -f "$DATA_PQ" ]]; then
  echo "[ERR] parquet not created: $DATA_PQ"
  exit 2
fi

cp -f "$DATA_PQ" "$RUN/"
echo "[OK] copied dataset -> $RUN/$(basename "$DATA_PQ")"

echo "[INFO] evaluating (row_id) ..."
conda run -n "$ENV_NAME" python "$ROOT/scripts/lab_big_eval_tabular_rowid.py" \
  --parquet "$RUN/$(basename "$DATA_PQ")" \
  --outdir "$OUT" \
  2>&1 | tee "$OUT/eval.txt"

echo "[INFO] bootstrap CI (rf/hgbdt/lgbm) ..."
for m in rf hgbdt lgbm; do
  P="$OUT/lab_${m}_pred.parquet"
  if [[ -f "$P" ]]; then
    conda run -n "$ENV_NAME" python "$ROOT/scripts/bootstrap_ci_score.py" \
      --pred "$P" --B 2000 2>&1 | tee "$OUT/lab_${m}_ci_score.txt"
  else
    echo "[WARN] missing pred parquet: $P"
  fi
done

echo "[INFO] RF attack-slices ..."
if [[ -f "$OUT/lab_rf_pred.parquet" ]]; then
  conda run -n "$ENV_NAME" python "$ROOT/scripts/report_attack_slices_rowid.py" \
    --data_parquet "$RUN/$(basename "$DATA_PQ")" \
    --pred_parquet "$OUT/lab_rf_pred.parquet" \
    --out_csv "$OUT/rf_attack_slices.csv" \
    2>&1 | tee "$OUT/rf_attack_slices.txt"
else
  echo "[WARN] missing RF pred parquet for slices."
fi

BUNDLE="$RUN/exfil2_full_bundle_${TS}.zip"
echo "[INFO] bundling -> $BUNDLE"
zip -j "$BUNDLE" \
  "$LOG" \
  "$RUN/$(basename "$DATA_PQ")" \
  "$OUT/eval.txt" \
  "$OUT/"*.txt \
  "$OUT/"*.csv \
  "$OUT/"*.parquet \
  >/dev/null || true

echo "[DONE] dataset=$RUN/$(basename "$DATA_PQ")"
echo "[DONE] outdir=$OUT"
echo "[DONE] bundle=$BUNDLE"
