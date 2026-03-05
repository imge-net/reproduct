#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 --outdir DIR --nodes N --profiles P --reps R --dur_capture SEC --iface_use IFACE
EOF
}

OUTDIR=""
NODES=10
PROFILES=6
REPS=20
DUR_CAPTURE=30
IFACE_USE="vethB"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --outdir) OUTDIR="$2"; shift 2;;
    --nodes) NODES="$2"; shift 2;;
    --profiles) PROFILES="$2"; shift 2;;
    --reps) REPS="$2"; shift 2;;
    --dur_capture) DUR_CAPTURE="$2"; shift 2;;
    --iface_use) IFACE_USE="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "[ERR] unknown arg: $1"; usage; exit 2;;
  esac
done

[[ -n "$OUTDIR" ]] || { echo "[ERR] --outdir required"; exit 2; }
mkdir -p "$OUTDIR"

ROOT="$HOME/work/mycelium_ids"
LAB="$ROOT/lab/flows"
mkdir -p "$LAB"

echo "[INFO] outdir=$OUTDIR nodes=$NODES profiles=$PROFILES reps=$REPS dur=$DUR_CAPTURE iface=$IFACE_USE"
date -Is > "$OUTDIR/started_at.txt"

# ---------- helpers ----------
ns_name() { printf "ns%s" "$1"; }  # nsA, nsB, ...
idx_to_letter() { python - <<PY "$1"
i=int(__import__("sys").argv[1])
print(chr(ord("A")+i))
PY
}

cleanup_topo() {
  echo "[INFO] cleanup topology"
  # remove tc qdisc
  for i in $(seq 0 $((NODES-1))); do
    L=$(idx_to_letter "$i")
    NS=$(ns_name "$L")
    sudo ip netns exec "$NS" bash -lc "tc qdisc del dev veth${L} root 2>/dev/null || true" || true
  done

  # remove namespaces
  for i in $(seq 0 $((NODES-1))); do
    L=$(idx_to_letter "$i")
    NS=$(ns_name "$L")
    sudo ip netns del "$NS" 2>/dev/null || true
  done

  # remove bridge
  sudo ip link del br0 2>/dev/null || true
}

make_underlay() {
  echo "[INFO] creating underlay netns+bridge"
  cleanup_topo

  sudo ip link add br0 type bridge
  sudo ip link set br0 up

  for i in $(seq 0 $((NODES-1))); do
    L=$(idx_to_letter "$i")
    NS=$(ns_name "$L")

    sudo ip netns add "$NS"

    # veth pair: vethA (inside nsA) <-> vethA-br (on host bridge)
    sudo ip link add "veth${L}" type veth peer name "veth${L}-br"
    sudo ip link set "veth${L}" netns "$NS"

    sudo ip link set "veth${L}-br" master br0
    sudo ip link set "veth${L}-br" up

    sudo ip netns exec "$NS" bash -lc "
      ip link set lo up
      ip link set veth${L} up
      # give each ns an IPv4 underlay in 10.10.0.0/24
      ip addr add 10.10.0.$((i+1))/24 dev veth${L}
    "
  done
}

apply_profile() {
  local pid="$1"   # profile id
  echo "[INFO] applying latency profile=$pid"

  # simple set of profiles (edit as you like):
  # 0: no delay
  # 1: 5ms
  # 2: 20ms
  # 3: 50ms + 10ms jitter
  # 4: 100ms + 20ms jitter + 1% loss
  # 5: asymmetric: nsC link gets extra delay
  for i in $(seq 0 $((NODES-1))); do
    L=$(idx_to_letter "$i")
    NS=$(ns_name "$L")

    sudo ip netns exec "$NS" bash -lc "tc qdisc del dev veth${L} root 2>/dev/null || true"

    case "$pid" in
      0) sudo ip netns exec "$NS" tc qdisc add dev "veth${L}" root netem delay 0ms ;;
      1) sudo ip netns exec "$NS" tc qdisc add dev "veth${L}" root netem delay 5ms ;;
      2) sudo ip netns exec "$NS" tc qdisc add dev "veth${L}" root netem delay 20ms ;;
      3) sudo ip netns exec "$NS" tc qdisc add dev "veth${L}" root netem delay 50ms 10ms ;;
      4) sudo ip netns exec "$NS" tc qdisc add dev "veth${L}" root netem delay 100ms 20ms loss 1% ;;
      5)
        if [[ "$L" == "C" ]]; then
          sudo ip netns exec "$NS" tc qdisc add dev "veth${L}" root netem delay 120ms 30ms
        else
          sudo ip netns exec "$NS" tc qdisc add dev "veth${L}" root netem delay 20ms
        fi
        ;;
      *) sudo ip netns exec "$NS" tc qdisc add dev "veth${L}" root netem delay 0ms ;;
    esac
  done
}

run_one_profile() {
  local pid="$1"
  local prof_dir="$OUTDIR/profile_${pid}"
  mkdir -p "$prof_dir"

  echo "[INFO] === profile $pid ===" | tee -a "$OUTDIR/progress.log"
  apply_profile "$pid" | tee -a "$prof_dir/profile_apply.log"

  # You can optionally start mycelium here if you have a stable launcher.
  # For now we keep capture on IFACE_USE underlay, which is what your paper reports.

  # Use your existing 3-node roles within N nodes:
  # nsA benign, nsB sink, nsC attacker
  export IFACE_USE="$IFACE_USE"
  export DUR_CAPTURE="$DUR_CAPTURE"
  export B_OV6=""              # underlay mode; no need for overlay ip
  export EXFIL_PORT=18080
  export EXFIL_DUR=20
  export SLEEP_BETWEEN=2

  # Store phases into a dedicated dir so runs do not mix
  PHASE_DIR="$LAB/phases_safe_multi_profile_${pid}"
  rm -rf "$PHASE_DIR"
  mkdir -p "$PHASE_DIR"
  export PHASE_DIR_OVERRIDE="$PHASE_DIR"

  ok=0; fail=0
  for i in $(seq 1 "$REPS"); do
    if bash "$ROOT/scripts/lab_capture_four_phase_safe.sh" "$i" >"$prof_dir/iter_${i}.log" 2>&1; then
      ok=$((ok+1))
    else
      fail=$((fail+1))
    fi
    echo "[PROF $pid] iter $i/$REPS ok=$ok fail=$fail" | tee -a "$OUTDIR/progress.log"
  done

  # merge & eval
  conda run -n ids_mycelium python "$ROOT/scripts/merge_lab_phases_multi.py" \
    --phases_dir "$PHASE_DIR" \
    --out_parquet "$prof_dir/lab_dataset.parquet" \
    > "$prof_dir/merge.txt" 2>&1

  EOUT="$prof_dir/eval"
  mkdir -p "$EOUT"
  conda run -n ids_mycelium python "$ROOT/scripts/lab_big_eval_tabular.py" \
    --parquet "$prof_dir/lab_dataset.parquet" \
    --outdir "$EOUT" \
    > "$EOUT/eval.txt" 2>&1

  for m in rf hgbdt lgbm; do
    P="$EOUT/lab_${m}_pred.parquet"
    if [[ -f "$P" ]]; then
      conda run -n ids_mycelium python "$ROOT/scripts/bootstrap_ci_score.py" \
        --pred "$P" --B 2000 \
        > "$EOUT/lab_${m}_ci_score.txt" 2>&1
    fi
  done

  echo "[DONE] profile=$pid ok=$ok fail=$fail" | tee -a "$OUTDIR/progress.log"
}

# ---------- main ----------
make_underlay

for pid in $(seq 0 $((PROFILES-1))); do
  run_one_profile "$pid"
done

echo "[ALL DONE] see $OUTDIR" | tee -a "$OUTDIR/progress.log"
