#!/usr/bin/env bash
set -euo pipefail

ENV_NAME="${ENV_NAME:-ids_mycelium}"
ROOT="${ROOT:-$HOME/work/mycelium_ids}"
LAB="$ROOT/lab/flows"
N="${1:-10}"                    # tekrar sayısı (10 => ~4k civarı; 15-20 => 6k-8k)
SLEEP_BETWEEN="${SLEEP_BETWEEN:-3}"

mkdir -p "$LAB/phases"

need() { command -v "$1" >/dev/null 2>&1 || { echo "[ERR] missing: $1"; exit 1; }; }
need conda

run_py() { conda run -n "$ENV_NAME" python "$@"; }

echo "[INFO] repetitions=$N"
echo "[INFO] outputs under $LAB/phases"

for i in $(seq 1 "$N"); do
  echo "=============================="
  echo "[ITER $i/$N] two-phase capture"
  echo "=============================="
  # mevcut iki faz capture script'in
  bash "$ROOT/scripts/lab_capture_two_phase.sh"

  # script’in ürettiği benign.csv / attack.csv'yi iterasyonlu isimle taşı
  mv -f "$LAB/benign.csv" "$LAB/phases/benign_${i}.csv"
  mv -f "$LAB/attack.csv" "$LAB/phases/attack_${i}.csv"

  sleep "$SLEEP_BETWEEN"
done

# merge: tüm benign_* ve attack_* dosyalarını birleştir
OUT_CSV="$LAB/lab_dataset_big.csv"
OUT_PQ="$LAB/lab_dataset_big.parquet"

run_py - <<'PY'
import glob, os, pandas as pd

LAB=os.path.expanduser("~/work/mycelium_ids/lab/flows")
P=os.path.join(LAB, "phases")

ben_files=sorted(glob.glob(os.path.join(P,"benign_*.csv")))
atk_files=sorted(glob.glob(os.path.join(P,"attack_*.csv")))
assert ben_files and atk_files and len(ben_files)==len(atk_files), (len(ben_files),len(atk_files))

def load_one(p):
    df=pd.read_csv(p)
    df.columns=[c.strip().lower() for c in df.columns]
    # normalize expected columns
    ren={"sport":"sport","dport":"dport","srcaddr":"saddr","dstaddr":"daddr"}
    for k,v in ren.items():
        if k in df.columns and v not in df.columns:
            df=df.rename(columns={k:v})
    # ensure y_true exists
    if "y_true" not in df.columns:
        raise SystemExit(f"Missing y_true in {p} cols={list(df.columns)}")
    return df

dfs=[]
for b,a in zip(ben_files, atk_files):
    dfs.append(load_one(b))
    dfs.append(load_one(a))

all_df=pd.concat(dfs, ignore_index=True)

out_csv=os.path.join(LAB,"lab_dataset_big.csv")
all_df.to_csv(out_csv, index=False)

out_pq=os.path.join(LAB,"lab_dataset_big.parquet")
all_df.to_parquet(out_pq, index=False)

print("[OK] wrote", out_csv, "rows=", len(all_df), "pos_rate=", float(all_df["y_true"].mean()))
print("[OK] wrote", out_pq)
PY

echo "[DONE] $OUT_CSV"
ls -lh "$OUT_CSV" "$OUT_PQ"
