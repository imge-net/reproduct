#!/usr/bin/env bash
set -euo pipefail

PARQUET="${1:-}"
if [[ -z "$PARQUET" || ! -f "$PARQUET" ]]; then
  echo "[ERR] parquet not found: $PARQUET"
  exit 2
fi

TS="$(date +%Y%m%d_%H%M%S)"
RUN="${RUN:-$HOME/work/mycelium_ids/runs/$TS}"
OUT="$RUN/eval_dl_portable"
ENV_NAME="${ENV_NAME:-ids_mycelium}"

mkdir -p "$OUT"

echo "[INFO] RUN=$RUN"
echo "[INFO] OUT=$OUT"
echo "[INFO] PARQUET=$PARQUET"
echo "[INFO] ENV=$ENV_NAME"

# 1) torch var mı?
if conda run -n "$ENV_NAME" python - <<'PY' >/dev/null 2>&1
import torch
PY
then
  echo "[INFO] torch import OK -> running pytorch baselines (mlp, ftt)"

  conda run -n "$ENV_NAME" python ~/work/mycelium_ids/scripts/dl_baselines_rowid.py \
    --parquet "$PARQUET" --outdir "$OUT" --feature_set portable --model mlp \
    --epochs 20 --batch 512 --lr 1e-3 --device cpu 2>&1 | tee "$OUT/mlp.log"

  conda run -n "$ENV_NAME" python ~/work/mycelium_ids/scripts/dl_baselines_rowid.py \
    --parquet "$PARQUET" --outdir "$OUT" --feature_set portable --model ftt \
    --epochs 20 --batch 512 --lr 1e-3 --device cpu 2>&1 | tee "$OUT/ftt.log"

  for m in mlp ftt; do
    P="$OUT/dl_${m}_portable_pred.parquet"
    if [[ -f "$P" ]]; then
      conda run -n "$ENV_NAME" python ~/work/mycelium_ids/scripts/bootstrap_ci_score.py \
        --pred "$P" --B 2000 2>&1 | tee "$OUT/dl_${m}_portable_ci_score.txt"
    else
      echo "[WARN] missing pred parquet: $P"
    fi
  done

  echo "[OK] DL baselines done: $OUT"
  exit 0
fi

echo "[WARN] torch import FAILED -> fallback to sklearn MLP baseline"
conda run -n "$ENV_NAME" python ~/work/mycelium_ids/scripts/sk_mlp_rowid.py \
  --parquet "$PARQUET" --outdir "$OUT" --feature_set portable \
  2>&1 | tee "$OUT/sk_mlp.log"

P="$OUT/sk_mlp_portable_pred.parquet"
if [[ -f "$P" ]]; then
  conda run -n "$ENV_NAME" python ~/work/mycelium_ids/scripts/bootstrap_ci_score.py \
    --pred "$P" --B 2000 2>&1 | tee "$OUT/sk_mlp_portable_ci_score.txt"
fi

echo "[OK] Fallback baseline done: $OUT"
