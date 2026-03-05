#!/usr/bin/env bash
set -euo pipefail

ENV_NAME="${ENV_NAME:-ids_mycelium}"
ROOT="${ROOT:-$HOME/work/mycelium_ids}"
LAB="$ROOT/lab/flows"
N="${1:-12}"   # 12 -> ~5k; 15 -> ~6k
SLEEP_BETWEEN="${SLEEP_BETWEEN:-3}"

B_OV6="${B_OV6:?export B_OV6 first}"

mkdir -p "$LAB/phases3"

echo "[INFO] B_OV6=$B_OV6 N=$N"

for i in $(seq 1 "$N"); do
  echo "=============================="
  echo "[ITER $i/$N] three-phase capture"
  echo "=============================="

  bash "$ROOT/scripts/lab_capture_three_phase.sh"

  # move phase outputs
  mv -f "$LAB/benign.csv"       "$LAB/phases3/benign_${i}.csv"
  mv -f "$LAB/attack.csv"       "$LAB/phases3/attack_${i}.csv"
  mv -f "$LAB/attack_exfil.csv" "$LAB/phases3/attack_exfil_${i}.csv"

  sleep "$SLEEP_BETWEEN"
done

conda run -n "$ENV_NAME" python - <<'PY'
import glob, os, pandas as pd

LAB=os.path.expanduser("~/work/mycelium_ids/lab/flows")
P=os.path.join(LAB,"phases3")

def load(p, attack_type):
    df=pd.read_csv(p)
    df.columns=[c.strip().lower() for c in df.columns]
    df["attack_type"]=attack_type
    return df

dfs=[]
for p in sorted(glob.glob(os.path.join(P,"benign_*.csv"))):
    dfs.append(load(p,"benign"))
for p in sorted(glob.glob(os.path.join(P,"attack_*.csv"))):
    dfs.append(load(p,"attack_main"))
for p in sorted(glob.glob(os.path.join(P,"attack_exfil_*.csv"))):
    dfs.append(load(p,"attack_exfil"))

all_df=pd.concat(dfs, ignore_index=True)

out_csv=os.path.join(LAB,"lab_dataset_big3.csv")
out_pq =os.path.join(LAB,"lab_dataset_big3.parquet")

all_df.to_csv(out_csv, index=False)
all_df.to_parquet(out_pq, index=False)

print("[OK] wrote", out_csv, "rows=", len(all_df),
      "pos_rate=", float(all_df["y_true"].mean()),
      "types=", dict(all_df["attack_type"].value_counts()))
print("[OK] wrote", out_pq)
PY

ls -lh "$LAB/lab_dataset_big3.csv" "$LAB/lab_dataset_big3.parquet"
