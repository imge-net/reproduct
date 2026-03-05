#!/usr/bin/env bash
set -euo pipefail

ENV_NAME="${ENV_NAME:-ids_mycelium}"
ROOT="$HOME/work/mycelium_ids"
LAB="$ROOT/lab/flows"
N="${1:-20}"
SLEEP_BETWEEN="${SLEEP_BETWEEN:-2}"

B_OV6="${B_OV6:?export B_OV6 first}"

mkdir -p "$LAB/phases_safe"

for i in $(seq 1 "$N"); do
  echo "=============================="
  echo "[ITER $i/$N] safe capture"
  echo "=============================="
  bash "$ROOT/scripts/lab_capture_three_phase_safe.sh"
  mv -f "$LAB/benign.csv"       "$LAB/phases_safe/benign_${i}.csv"
  mv -f "$LAB/attack_exfil.csv" "$LAB/phases_safe/attack_exfil_${i}.csv"
  sleep "$SLEEP_BETWEEN"
done

conda run -n "$ENV_NAME" python - <<'PY'
import glob, os, pandas as pd
LAB=os.path.expanduser("~/work/mycelium_ids/lab/flows")
P=os.path.join(LAB,"phases_safe")
dfs=[]
for p in sorted(glob.glob(os.path.join(P,"*.csv"))):
    df=pd.read_csv(p)
    df.columns=[c.strip() for c in df.columns]
    dfs.append(df)
all_df=pd.concat(dfs, ignore_index=True)
out_csv=os.path.join(LAB,"lab_dataset_big_safe.csv")
out_pq=os.path.join(LAB,"lab_dataset_big_safe.parquet")
all_df.to_csv(out_csv,index=False)
all_df.to_parquet(out_pq,index=False)
print("[OK] wrote", out_csv, "rows=", len(all_df), "pos_rate=", float(all_df["y_true"].mean()))
print(all_df["attack_type"].value_counts())
PY

ls -lh "$LAB/lab_dataset_big_safe.csv" "$LAB/lab_dataset_big_safe.parquet"
