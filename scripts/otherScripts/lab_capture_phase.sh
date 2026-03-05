#!/usr/bin/env bash
set -euo pipefail

PHASE="${1:?phase_name}"
LABEL="${2:?0/1}"
OUT="${3:?out_csv}"
DUR="${DUR:-20}"

ROOT="$HOME/work/mycelium_ids"
IFACE_USE="${IFACE_USE:-myc0}"
IFACE_USE="${IFACE_USE:-myc0}"
LAB="$ROOT/lab/flows"
mkdir -p "$LAB"

echo "[INFO] phase=$PHASE label=$LABEL dur=$DUR out=$OUT"

# capture flows on nsB/myc0
DUR="$DUR" IFACE="$IFACE_USE" bash "$ROOT/scripts/export_lab_flows_pktagg.sh" "$LAB/_phase_flows.csv"

# IMPORTANT: run python in the activated env (not conda run)
source "$HOME/miniconda3/etc/profile.d/conda.sh"
conda activate ids_mycelium

python - <<PY
import pandas as pd
df=pd.read_csv("$LAB/_phase_flows.csv")
df.columns=[c.strip() for c in df.columns]

# drop obvious junk rows and Argus control rows
if "SrcAddr" in df.columns:
    df = df[df["SrcAddr"].astype(str).str.strip() != "0"]
if "DstAddr" in df.columns:
    df = df[df["DstAddr"].astype(str).str.strip() != "0"]

df["y_true"]=int("$LABEL")
df["attack_type"]="$PHASE"

df.to_csv("$OUT", index=False)
print("[OK] wrote", "$OUT", "n=", len(df))
PY

# hard check
test -s "$OUT" || { echo "[ERR] phase output missing/empty: $OUT"; exit 3; }

rm -f "$LAB/_phase_flows.csv"
