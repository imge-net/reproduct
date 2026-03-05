#!/usr/bin/env bash
set -euo pipefail

# Runs multi-node benign + burst + low&slow cycles, captures flows at nsB/vethB.
# Writes per-iter CSVs into phases_safe_multi_node, then merges at end.

REPS="${REPS:-50}"
DUR_CAPTURE="${DUR_CAPTURE:-30}"
IFACE_USE="${IFACE_USE:-vethB}"
SINK_NS="${SINK_NS:-nsB}"
PHASE_DIR="${PHASE_DIR:-$HOME/work/mycelium_ids/lab/flows/phases_safe_multi_node}"

PORT_BURST="${PORT_BURST:-18080}"
PORT_SLOW="${PORT_SLOW:-18081}"
PORT_BENIGN="${PORT_BENIGN:-18082}"

# define a pool (edit as you wish)
BENIGN_SOURCES=(nsA nsD nsE nsG nsH)
ATTACK_SOURCES=(nsC nsF nsI nsJ)

mkdir -p "$PHASE_DIR"
ROOT="$HOME/work/mycelium_ids"
LAB="$ROOT/lab/flows"

echo "[INFO] REPS=$REPS DUR_CAPTURE=$DUR_CAPTURE IFACE_USE=$IFACE_USE sink=$SINK_NS phase_dir=$PHASE_DIR"

for i in $(seq 1 "$REPS"); do
  bs="${BENIGN_SOURCES[$(( (i-1) % ${#BENIGN_SOURCES[@]} ))]}"
  as="${ATTACK_SOURCES[$(( (i-1) % ${#ATTACK_SOURCES[@]} ))]}"

  echo "=============================="
  echo "[ITER $i/$REPS] benign_src=$bs attack_src=$as"

  # 1) benign phase: bs -> sink on PORT_BENIGN
  # (call your existing benign generator here; placeholder uses your lab scripts if available)
  # capture -> flows
  PHASE="benign"
  OUTCSV="$PHASE_DIR/${PHASE}_${i}.csv"
  PCAP="$LAB/_phase_${PHASE}_${i}.pcap"

  echo "[INFO] benign: $bs -> $SINK_NS port=$PORT_BENIGN"
  # TODO: replace with your existing benign traffic generator script call if needed.

  sudo ip netns exec "$SINK_NS" bash -lc "timeout ${DUR_CAPTURE} tcpdump -n -i ${IFACE_USE} -s 0 -w '${PCAP}' >/dev/null 2>&1" || true
  conda run -n ids_mycelium python "$ROOT/scripts/pcap_to_flows.py" --pcap "$PCAP" --out_csv "$LAB/_phase_flows.csv" >/dev/null
  conda run -n ids_mycelium python "$ROOT/scripts/label_phase.py" --in_csv "$LAB/_phase_flows.csv" --out_csv "$OUTCSV" --label 0 --attack_type benign >/dev/null
  echo "[OK] wrote $OUTCSV"

  # 2) attack burst: as -> sink on PORT_BURST
  PHASE="attack_exfil_burst"
  OUTCSV="$PHASE_DIR/${PHASE}_${i}.csv"
  PCAP="$LAB/_phase_${PHASE}_${i}.pcap"

  echo "[INFO] burst: $as -> $SINK_NS port=$PORT_BURST"
  # TODO: replace with your burst generator (your lab_exfil_burst / portserver approach)

  sudo ip netns exec "$SINK_NS" bash -lc "timeout ${DUR_CAPTURE} tcpdump -n -i ${IFACE_USE} -s 0 -w '${PCAP}' >/dev/null 2>&1" || true
  conda run -n ids_mycelium python "$ROOT/scripts/pcap_to_flows.py" --pcap "$PCAP" --out_csv "$LAB/_phase_flows.csv" >/dev/null
  conda run -n ids_mycelium python "$ROOT/scripts/label_phase.py" --in_csv "$LAB/_phase_flows.csv" --out_csv "$OUTCSV" --label 1 --attack_type attack_exfil_burst >/dev/null
  echo "[OK] wrote $OUTCSV"

  # 3) attack low&slow: as -> sink on PORT_SLOW
  PHASE="attack_exfil_lowslow"
  OUTCSV="$PHASE_DIR/${PHASE}_${i}.csv"
  PCAP="$LAB/_phase_${PHASE}_${i}.pcap"

  echo "[INFO] low&slow: $as -> $SINK_NS port=$PORT_SLOW"
  # TODO: replace with your low&slow generator

  sudo ip netns exec "$SINK_NS" bash -lc "timeout ${DUR_CAPTURE} tcpdump -n -i ${IFACE_USE} -s 0 -w '${PCAP}' >/dev/null 2>&1" || true
  conda run -n ids_mycelium python "$ROOT/scripts/pcap_to_flows.py" --pcap "$PCAP" --out_csv "$LAB/_phase_flows.csv" >/dev/null
  conda run -n ids_mycelium python "$ROOT/scripts/label_phase.py" --in_csv "$LAB/_phase_flows.csv" --out_csv "$OUTCSV" --label 1 --attack_type attack_exfil_lowslow >/dev/null
  echo "[OK] wrote $OUTCSV"
done

echo "[DONE] phases in $PHASE_DIR"
