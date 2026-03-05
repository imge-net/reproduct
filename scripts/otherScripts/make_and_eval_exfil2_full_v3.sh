#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-$HOME/work/mycelium_ids}"
ENV_NAME="${ENV_NAME:-ids_mycelium}"

# Eğer RUN verilmezse “son run”ı kullan
if [[ -z "${RUN:-}" ]]; then
  if [[ -f "$ROOT/runs/_last_run_four_phase.txt" ]]; then
    RUN="$(cat "$ROOT/runs/_last_run_four_phase.txt")"
  else
    echo "[ERR] RUN not set and _last_run_four_phase.txt not found."
    exit 2
  fi
fi

PHASE_DIR="${PHASE_DIR:-$RUN/phases_safe_multi}"
OUT="${OUT:-$RUN/eval_exfil2_full_rowid}"
LAB_DIR="${LAB_DIR:-$ROOT/lab/flows}"
mkdir -p "$OUT" "$LAB_DIR"

LOG="$RUN/make_and_eval_exfil2_full_v3.log"

echo "[INFO] ROOT=$ROOT"       | tee -a "$LOG"
echo "[INFO] RUN=$RUN"         | tee -a "$LOG"
echo "[INFO] PHASE_DIR=$PHASE_DIR" | tee -a "$LOG"
echo "[INFO] OUT=$OUT"         | tee -a "$LOG"
echo "[INFO] ENV_NAME=$ENV_NAME" | tee -a "$LOG"

# 1) CSV var mı?
cnt=$(ls -1 "$PHASE_DIR"/*.csv 2>/dev/null | wc -l | tr -d ' ')
if [[ "$cnt" == "0" ]]; then
  echo "[ERR] no CSV files in $PHASE_DIR" | tee -a "$LOG"
  echo "[HINT] Did you run run_four_phase_multi_vethB_bg_v3.sh with RUN=$RUN ?" | tee -a "$LOG"
  exit 2
fi
echo "[OK] csv_count=$cnt" | tee -a "$LOG"

# 2) Merge -> parquet
DATA_PQ="$LAB_DIR/lab_dataset_exfil2_full.parquet"
DATA_CSV="$LAB_DIR/lab_dataset_exfil2_full.csv"

conda run -n "$ENV_NAME" python - <<'PY' 2>&1 | tee -a "$LOG"
import glob, os, re
import pandas as pd

phase_dir=os.environ["PHASE_DIR"]
out_pq=os.environ["DATA_PQ"]
out_csv=os.environ["DATA_CSV"]

files=sorted(glob.glob(os.path.join(phase_dir,"*.csv")))
assert files, f"no csv in {phase_dir}"

dfs=[]
pat=re.compile(r"^(benign|attack_exfil_burst|attack_exfil_lowslow)_(\d+)\.csv$")
for fp in files:
    base=os.path.basename(fp)
    m=pat.match(base)
    if not m:
        continue
    at=m.group(1); rep=int(m.group(2))
    df=pd.read_csv(fp)
    df.columns=[c.strip() for c in df.columns]
    df["attack_type"]=at
    df["rep"]=rep
    if "y_true" not in df.columns:
        df["y_true"]=0 if at=="benign" else 1
    dfs.append(df)

all_df=pd.concat(dfs, ignore_index=True)
all_df.to_parquet(out_pq, index=False)
all_df.to_csv(out_csv, index=False)

print("[OK] wrote", out_pq)
print("rows=", len(all_df), "pos_rate=", float(all_df["y_true"].mean()))
print(all_df["attack_type"].value_counts().to_string())
print("rep min/max:", int(all_df["rep"].min()), int(all_df["rep"].max()))
PY
export PHASE_DIR
export DATA_PQ
export DATA_CSV

# 3) Copy to RUN snapshot
cp -f "$DATA_PQ" "$RUN/"
SNAP="$RUN/$(basename "$DATA_PQ")"
echo "[OK] snapshot: $SNAP" | tee -a "$LOG"

# 4) Eval (row_id) + CI
conda run -n "$ENV_NAME" python "$ROOT/scripts/lab_big_eval_tabular_rowid.py" \
  --parquet "$SNAP" --outdir "$OUT" 2>&1 | tee "$OUT/eval.txt"

for m in rf hgbdt lgbm; do
  P="$OUT/lab_${m}_pred.parquet"
  if [[ -f "$P" ]]; then
    conda run -n "$ENV_NAME" python "$ROOT/scripts/bootstrap_ci_score.py" \
      --pred "$P" --B 2000 2>&1 | tee "$OUT/lab_${m}_ci_score.txt"
  fi
done

# 5) Attack slices (RF)
conda run -n "$ENV_NAME" python "$ROOT/scripts/report_attack_slices_rowid.py" \
  --data_parquet "$SNAP" \
  --pred_parquet "$OUT/lab_rf_pred.parquet" \
  --out_csv "$OUT/rf_attack_slices.csv" 2>&1 | tee "$OUT/rf_attack_slices.txt"

echo "[DONE] OUT=$OUT" | tee -a "$LOG"
ls -lh "$OUT" | tee -a "$LOG"
