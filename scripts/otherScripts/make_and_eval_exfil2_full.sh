#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$HOME/work/mycelium_ids}"
PHASE_DIR="${PHASE_DIR:-$ROOT/lab/flows/phases_safe_multi}"
LAB_DIR="${LAB_DIR:-$ROOT/lab/flows}"

TS="$(date +%Y%m%d_%H%M%S)"
RUN="${RUN:-$ROOT/runs/$TS}"
OUT="${OUT:-$RUN/eval_exfil2_full_rowid}"
ENV_NAME="${ENV_NAME:-ids_mycelium}"

mkdir -p "$RUN" "$OUT" "$LAB_DIR"

LOG="$RUN/make_and_eval_exfil2_full.log"
exec > >(tee -a "$LOG") 2>&1

echo "[INFO] ROOT=$ROOT"
echo "[INFO] PHASE_DIR=$PHASE_DIR"
echo "[INFO] LAB_DIR=$LAB_DIR"
echo "[INFO] RUN=$RUN"
echo "[INFO] OUT=$OUT"
echo "[INFO] ENV=$ENV_NAME"
echo "[INFO] log=$LOG"

# ---- sanity checks
if [ ! -d "$PHASE_DIR" ]; then
  echo "[ERR] phases directory missing: $PHASE_DIR"
  exit 2
fi

csv_count="$(ls -1 "$PHASE_DIR"/*.csv 2>/dev/null | wc -l | tr -d ' ')"
echo "[INFO] csv_count=$csv_count"
if [ "$csv_count" -eq 0 ]; then
  echo "[ERR] no CSV files found in $PHASE_DIR"
  echo "[HINT] Did the 100-run capture write files into phases_safe_multi?"
  exit 3
fi

# ---- build full parquet
OUT_PQ="$LAB_DIR/lab_dataset_exfil2_full.parquet"
OUT_CSV="$LAB_DIR/lab_dataset_exfil2_full.csv"

echo "[INFO] building dataset parquet -> $OUT_PQ"
conda run -n "$ENV_NAME" python - <<'PY'
import glob, os, re
import pandas as pd

PHASE_DIR=os.path.expanduser(os.environ.get("PHASE_DIR"))
LAB_DIR=os.path.expanduser(os.environ.get("LAB_DIR"))

files=sorted(glob.glob(os.path.join(PHASE_DIR,"*.csv")))
assert files, f"[ERR] no csv in {PHASE_DIR}"

dfs=[]
pat=re.compile(r"^(benign|attack_exfil_burst|attack_exfil_lowslow)_(\d+)\.csv$")
for fp in files:
    base=os.path.basename(fp)
    m=pat.match(base)
    if not m:
        continue
    attack_type=m.group(1)
    rep=int(m.group(2))
    df=pd.read_csv(fp)
    df.columns=[c.strip() for c in df.columns]
    df["attack_type"]=attack_type
    df["rep"]=rep
    if "y_true" not in df.columns:
        df["y_true"]=0 if attack_type=="benign" else 1
    dfs.append(df)

assert dfs, "[ERR] no matching phase csv files (expected benign_*.csv / attack_exfil_burst_*.csv / attack_exfil_lowslow_*.csv)"

all_df=pd.concat(dfs, ignore_index=True)

out_pq=os.path.join(LAB_DIR,"lab_dataset_exfil2_full.parquet")
out_csv=os.path.join(LAB_DIR,"lab_dataset_exfil2_full.csv")
all_df.to_parquet(out_pq, index=False)
all_df.to_csv(out_csv, index=False)

print("[OK] wrote", out_pq)
print("rows=", len(all_df), "pos_rate=", float(all_df["y_true"].mean()))
print(all_df["attack_type"].value_counts().to_string())
print("rep min/max:", int(all_df["rep"].min()), int(all_df["rep"].max()))
PY

if [ ! -f "$OUT_PQ" ]; then
  echo "[ERR] parquet not created: $OUT_PQ"
  exit 4
fi

cp -f "$OUT_PQ" "$RUN/"
echo "[OK] copied parquet -> $RUN/$(basename "$OUT_PQ")"

# ---- eval (rowid)
echo "[INFO] running eval -> $OUT/eval.txt"
conda run -n "$ENV_NAME" python "$ROOT/scripts/lab_big_eval_tabular_rowid.py" \
  --parquet "$RUN/$(basename "$OUT_PQ")" \
  --outdir "$OUT" \
  2>&1 | tee "$OUT/eval.txt"

# ---- bootstrap CI
for m in rf hgbdt lgbm; do
  P="$OUT/lab_${m}_pred.parquet"
  if [ -f "$P" ]; then
    echo "[INFO] bootstrap CI for $m -> $OUT/lab_${m}_ci_score.txt"
    conda run -n "$ENV_NAME" python "$ROOT/scripts/bootstrap_ci_score.py" \
      --pred "$P" --B 2000 2>&1 | tee "$OUT/lab_${m}_ci_score.txt"
  else
    echo "[WARN] missing pred parquet: $P"
  fi
done

# ---- attack slices (RF)
if [ -f "$OUT/lab_rf_pred.parquet" ]; then
  echo "[INFO] attack slices (RF) -> $OUT/rf_attack_slices.txt"
  conda run -n "$ENV_NAME" python "$ROOT/scripts/report_attack_slices_rowid.py" \
    --data_parquet "$RUN/$(basename "$OUT_PQ")" \
    --pred_parquet "$OUT/lab_rf_pred.parquet" \
    --out_csv "$OUT/rf_attack_slices.csv" \
    2>&1 | tee "$OUT/rf_attack_slices.txt"
else
  echo "[WARN] RF pred parquet missing, skip slice report."
fi

echo "[DONE] artifacts:"
echo "  dataset: $RUN/$(basename "$OUT_PQ")"
echo "  evaldir: $OUT"
echo "  log:     $LOG"
