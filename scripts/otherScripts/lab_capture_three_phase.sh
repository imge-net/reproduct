#!/usr/bin/env bash
set -euo pipefail

ROOT="$HOME/work/mycelium_ids"
LAB="$ROOT/lab/flows"

B_OV6="${B_OV6:?set B_OV6 env var}"
EXFIL_DUR="${EXFIL_DUR:-30}"
EXFIL_PORT="${EXFIL_PORT:-8000}"

mkdir -p "$LAB"

echo "[INFO] B_OV6=$B_OV6 EXFIL_DUR=$EXFIL_DUR EXFIL_PORT=$EXFIL_PORT"

# Phase 1 + 2: your existing two-phase capture (writes benign.csv and attack.csv)
bash "$ROOT/scripts/lab_capture_two_phase.sh"

# Phase 3: exfil generation (nsC -> nsB)
bash "$ROOT/scripts/lab_exfil.sh" nsC "$B_OV6" "$EXFIL_DUR" "$EXFIL_PORT"

# Re-run flow export after exfil to capture it as its own file.
# We assume your two-phase script already knows how to export flows to $LAB/attack.csv etc.
# Here we call a dedicated flow export if you have one; otherwise we re-use what you already used.
# If you already have lab_flows.csv generator, run it and then label with attacker C.
if [[ -f "$ROOT/scripts/export_lab_flows.sh" ]]; then
  bash "$ROOT/scripts/export_lab_flows.sh" "$LAB/lab_flows_exfil.csv"
else
  # fallback: try to reuse an existing flows file if you keep it at lab_flows.csv
  if [[ -f "$LAB/lab_flows.csv" ]]; then
    cp -f "$LAB/lab_flows.csv" "$LAB/lab_flows_exfil.csv"
  else
    echo "[ERR] Could not find export_lab_flows.sh or $LAB/lab_flows.csv"
    echo "      Create scripts/export_lab_flows.sh (Argus export) or ensure lab_flows.csv exists."
    exit 1
  fi
fi

# Need C overlay IP for labeling: read from nsC myc0
C_OV6="$(sudo ip netns exec nsC ip -br a | awk '/myc0/ {print $3}' | cut -d/ -f1)"
echo "[INFO] C_OV6=$C_OV6"

conda run -n ids_mycelium python "$ROOT/scripts/label_lab_flows.py" \
  --in_csv  "$LAB/lab_flows_exfil.csv" \
  --attacker_ip "$C_OV6" \
  --out_csv "$LAB/attack_exfil.csv"

echo "[OK] wrote $LAB/attack_exfil.csv"
