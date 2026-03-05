#!/usr/bin/env bash
set -euo pipefail

LAB="$HOME/work/mycelium_ids/lab"
OUT="$LAB/flows"
mkdir -p "$OUT"

# Overlay IPv6 addresses
A_OV6=$(sudo ip netns exec nsA bash -lc "ip -6 -br addr show dev myc0 | awk '{print \$3}' | cut -d/ -f1")
B_OV6=$(sudo ip netns exec nsB bash -lc "ip -6 -br addr show dev myc0 | awk '{print \$3}' | cut -d/ -f1")
C_OV6=$(sudo ip netns exec nsC bash -lc "ip -6 -br addr show dev myc0 | awk '{print \$3}' | cut -d/ -f1")

echo "[INFO] A=$A_OV6"
echo "[INFO] B=$B_OV6"
echo "[INFO] C=$C_OV6"

# Ensure tools
command -v argus >/dev/null || (sudo apt-get update && sudo apt-get install -y argus-server)
command -v ra >/dev/null || (sudo apt-get update && sudo apt-get install -y argus-client)
command -v curl >/dev/null || (sudo apt-get update && sudo apt-get install -y curl)
command -v iperf3 >/dev/null || (sudo apt-get update && sudo apt-get install -y iperf3)
command -v nmap >/dev/null || (sudo apt-get update && sudo apt-get install -y nmap)

start_argus() {
  local tag="$1"
  sudo ip netns exec nsB bash -lc "
    rm -f /tmp/${tag}.argus /tmp/${tag}.pid
    argus -i myc0 -w /tmp/${tag}.argus &
    echo \$! > /tmp/${tag}.pid
    echo '[OK] argus started tag=${tag} pid=' \$(cat /tmp/${tag}.pid)
  "
}

stop_argus_export() {
  local tag="$1"
  sudo ip netns exec nsB bash -lc "
    kill \$(cat /tmp/${tag}.pid) 2>/dev/null || true
    sleep 1
    # -n: no name resolution (port/service), keep numeric where possible
    ra -n -r /tmp/${tag}.argus -s dur proto saddr sport daddr dport spkts dpkts sbytes dbytes state -c , > /tmp/${tag}.csv
    head -n 3 /tmp/${tag}.csv
  "
  sudo ip netns exec nsB cat /tmp/${tag}.csv > "$OUT/${tag}.csv"
  echo "[OK] wrote $OUT/${tag}.csv"
}

echo
echo "=============================="
echo "[PHASE 1] BENIGN capture"
echo "=============================="

# Start benign services on nsB
sudo ip netns exec nsB bash -lc "
  pkill -f 'iperf3 -s' 2>/dev/null || true
  iperf3 -s -D
  pkill -f 'python3 -m http.server 8000' 2>/dev/null || true
  nohup python3 -m http.server 8000 --bind :: >/tmp/http.log 2>&1 &
"

start_argus benign

# Benign traffic from nsA: http bursts + iperf + ping
sudo ip netns exec nsA bash -lc "
  for i in \$(seq 1 200); do
    curl -g -m 1 -s 'http://[${B_OV6}]:8000' >/dev/null || true
  done
  iperf3 -6 -c ${B_OV6} -t 10 >/dev/null || true
  ping -6 -c 10 ${B_OV6} >/dev/null || true
"

stop_argus_export benign

echo
echo "=============================="
echo "[PHASE 2] ATTACK capture (nmap)"
echo "=============================="

start_argus attack

sudo ip netns exec nsC bash -lc "
  nmap -6 -p 1-2000 -T4 ${B_OV6} >/dev/null || true
"

stop_argus_export attack

echo
echo "[DONE] Two-phase captures created:"
echo "  $OUT/benign.csv"
echo "  $OUT/attack.csv"
