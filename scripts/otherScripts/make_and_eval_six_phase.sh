#!/usr/bin/env bash
set -euo pipefail
ROOT="${ROOT:-$HOME/work/mycelium_ids}"
RUN="${RUN:?set RUN=/path/to/run}"
PHASE_DIR="${PHASE_DIR:-$RUN/phases_six_safe}"
OUT="${OUT:-$RUN/eval_six_phase_rowid}"
ENV_NAME="${ENV_NAME:-ids_mycelium}"

mkdir -p "$OUT"

echo "[INFO] RUN=$RUN"
echo "[INFO] PHASE_DIR=$PHASE_DIR"
echo "[INFO] OUT=$OUT"

# merge csv -> parquet
PARQ="$RUN/lab_dataset_six_phase.parquet"
conda run -n "$ENV_NAME" python - <<PY
import glob, os, pandas as pd
P=os.path.expanduser("$PHASE_DIR")
files=sorted(glob.glob(os.path.join(P,"*.csv")))
if not files:
    raise SystemExit(f"[ERR] no csv in {P}")
dfs=[pd.read_csv(f) for f in files]
df=pd.concat(dfs, ignore_index=True)
df.to_parquet("$PARQ", index=False)
print("[OK] wrote $PARQ rows=", len(df), "pos_rate=", float(df["y_true"].mean()))
print(df["attack_type"].value_counts().to_string())
PY

# main eval (rf/hgbdt/lgbm) rowid
conda run -n "$ENV_NAME" python "$ROOT/scripts/lab_big_eval_tabular_rowid.py" \
  --parquet "$PARQ" --outdir "$OUT" 2>&1 | tee "$OUT/eval.txt"

# CI
for m in rf hgbdt lgbm; do
  PRED="$OUT/lab_${m}_pred.parquet"
  if [[ -f "$PRED" ]]; then
    conda run -n "$ENV_NAME" python "$ROOT/scripts/bootstrap_ci_score.py" \
      --pred "$PRED" --B 2000 2>&1 | tee "$OUT/lab_${m}_ci_score.txt"
  fi
done

# attack slices (RF)
conda run -n "$ENV_NAME" python "$ROOT/scripts/report_attack_slices_rowid.py" \
  --data_parquet "$PARQ" \
  --pred_parquet "$OUT/lab_rf_pred.parquet" \
  --out_csv "$OUT/rf_attack_slices.csv" 2>&1 | tee "$OUT/rf_attack_slices.txt"

echo "[OK] done: $OUT"
