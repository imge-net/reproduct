#!/usr/bin/env bash
set -euo pipefail

ROOT=${ROOT:-$HOME/work/mycelium_ids}
RUN=${RUN:-}
N=${N:-10}
IFACE_USE=${IFACE_USE:-vethB}
DUR_CAPTURE=${DUR_CAPTURE:-30}
SINK_IP=${SINK_IP:-10.10.0.3}

if [[ -z "${RUN}" ]]; then
  echo "RUN is required"
  exit 1
fi

PHASE_DIR="$RUN/phases_six_safe"
mkdir -p "$PHASE_DIR"

FLOW_SCRIPT="$ROOT/scripts/pcap_to_flows.py"

echo "[INFO] ROOT=$ROOT"
echo "[INFO] RUN=$RUN"
echo "[INFO] PHASE_DIR=$PHASE_DIR"
echo "[INFO] ITERATIONS=$N"

ensure_servers() {

echo "[INFO] ensuring nsB listeners"

for p in 18080 18081 18082 18083 18084 18085; do
sudo ip netns exec nsB bash -c "nohup python3 -m http.server $p >/dev/null 2>&1 &"
done

}

capture_phase() {

phase=$1
attack_type=$2
label=$3
traffic_func=$4

PCAP="$ROOT/lab/flows/_phase.pcap"
CSV="$PHASE_DIR/${phase}_$(date +%s%N).csv"

echo "[INFO] phase=$phase attack_type=$attack_type label=$label"

sudo timeout "$DUR_CAPTURE" tcpdump -i "$IFACE_USE" -w "$PCAP" >/dev/null 2>&1 &
TCP_PID=$!

sleep 2

$traffic_func &

sleep "$DUR_CAPTURE"

kill "$TCP_PID" >/dev/null 2>&1 || true

if [[ ! -s "$PCAP" ]]; then
echo "[WARN] empty pcap"
return
fi

python "$FLOW_SCRIPT" \
  --pcap "$PCAP" \
  --out_csv "$CSV"

rows=$(wc -l "$CSV" | awk '{print $1}')

echo "[OK] wrote $CSV rows=$rows"

}

benign() {
sudo ip netns exec nsA wrk -t4 -c40 -d20s http://$SINK_IP:18082/
}

scan() {
sudo ip netns exec nsC nping --tcp -p 18080-18085 --rate 20 --count 200 $SINK_IP
}

c2() {
sudo ip netns exec nsC bash -c "
for i in \$(seq 1 40); do
curl -m 0.3 -s http://$SINK_IP:18083/ >/dev/null
sleep 1
done
"
}

lateral() {
sudo ip netns exec nsC hping3 -S -p 18084 -c 200 $SINK_IP
}

exfil_burst() {
sudo ip netns exec nsC wrk -t4 -c80 -d15s http://$SINK_IP:18080/
}

exfil_lowslow() {
sudo ip netns exec nsC bash -c "
for i in \$(seq 1 120); do
curl -m 0.2 -s http://$SINK_IP:18081/ >/dev/null
sleep 0.5
done
"
}

ensure_servers

for i in $(seq 1 "$N"); do

echo "=============================="
echo "[ITER $i/$N] $(date)"

capture_phase benign benign 0 benign
capture_phase scan attack_scan 1 scan
capture_phase c2 attack_c2_beacon 1 c2
capture_phase lateral attack_lateral 1 lateral
capture_phase exfil_burst attack_exfil_burst 1 exfil_burst
capture_phase exfil_lowslow attack_exfil_lowslow 1 exfil_lowslow

done

echo "[OK] dataset generation finished"
