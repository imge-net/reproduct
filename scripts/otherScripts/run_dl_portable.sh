#!/usr/bin/env bash
set -euo pipefail

ENV_NAME="${ENV_NAME:-ids_mycelium}"
PARQUET="${1:-$HOME/work/mycelium_ids/lab/flows/lab_dataset_big_safe.parquet}"

TS="$(date +%Y%m%d_%H%M%S)"
RUN="$HOME/work/mycelium_ids/runs/$TS"
OUT="$RUN/eval_dl_portable"
mkdir -p "$OUT"

echo "[INFO] RUN=$RUN"
echo "[INFO] OUT=$OUT"
echo "[INFO] PARQUET=$PARQUET"
echo "[INFO] ENV=$ENV_NAME"

# MLP
conda run -n "$ENV_NAME" python "$HOME/work/mycelium_ids/scripts/dl_baselines_rowid.py" \
  --parquet "$PARQUET" --outdir "$OUT" \
  --feature_set portable --model mlp \
  --epochs 20 --batch 512 --lr 1e-3 --device cpu \
  2>&1 | tee "$OUT/mlp.log"

# FTT
conda run -n "$ENV_NAME" python "$HOME/work/mycelium_ids/scripts/dl_baselines_rowid.py" \
  --parquet "$PARQUET" --outdir "$OUT" \
  --feature_set portable --model ftt \
  --epochs 20 --batch 512 --lr 1e-3 --device cpu \
  2>&1 | tee "$OUT/ftt.log"

# Bootstrap CI
for m in mlp ftt; do
  P="$OUT/dl_${m}_portable_pred.parquet"
  conda run -n "$ENV_NAME" python "$HOME/work/mycelium_ids/scripts/bootstrap_ci_score.py" \
    --pred "$P" --B 2000 \
    2>&1 | tee "$OUT/dl_${m}_portable_ci_score.txt"
done

echo "[DONE] $OUT"
ls -lh "$OUT" | sed -n '1,200p'
