#!/usr/bin/env bash
set -euo pipefail

LAB="$HOME/work/mycelium_ids/lab"
KEYS="$LAB/keys"
LOGS="$LAB/logs"
PIDS="$LAB/pids"
mkdir -p "$LOGS" "$PIDS"

need() { command -v "$1" >/dev/null 2>&1 || { echo "[ERR] missing: $1"; exit 1; }; }
need mycelium

start_one() {
  local NS="$1"
  local KEY="$2"
  local PEER1="$3"
  local PEER2="$4"
  local LOG="$LOGS/$NS.log"
  local PIDF="$PIDS/$NS.pid"

  echo "[INFO] start $NS  key=$KEY"
  # Not: --peers ve -p README'deki kullanım ile uyumlu. Varsayılan port 9651. :contentReference[oaicite:1]{index=1}
  sudo ip netns exec "$NS" bash -lc "
    nohup mycelium -k '$KEY' -p 9651 --tun-name myc0 --peers '$PEER1' '$PEER2' > '$LOG' 2>&1 &
    echo \$! > '$PIDF'
  "
  echo "[OK] started $NS (pid file: $PIDF)"
}

stop_one() {
  local NS="$1"
  local PIDF="$PIDS/$NS.pid"
  if [[ -f "$PIDF" ]]; then
    sudo kill "$(cat "$PIDF")" 2>/dev/null || true
    rm -f "$PIDF"
    echo "[OK] stopped $NS"
  else
    echo "[INFO] no pid for $NS"
  fi
}

inspect_one() {
  local NS="$1"
  local KEY="$2"
  sudo ip netns exec "$NS" mycelium -k "$KEY" inspect --json
}

case "${1:-}" in
  startB)
    start_one nsB "$KEYS/nsB.key" "tcp://10.10.0.1:9651" "tcp://10.10.0.3:9651"
    ;;
  startAll)
    start_one nsA "$KEYS/nsA.key" "tcp://10.10.0.2:9651" "tcp://10.10.0.3:9651"
    start_one nsB "$KEYS/nsB.key" "tcp://10.10.0.1:9651" "tcp://10.10.0.3:9651"
    start_one nsC "$KEYS/nsC.key" "tcp://10.10.0.1:9651" "tcp://10.10.0.2:9651"
    ;;
  stopAll)
    stop_one nsA
    stop_one nsB
    stop_one nsC
    ;;
  inspectA)
    inspect_one nsA "$KEYS/nsA.key"
    ;;
  inspectB)
    inspect_one nsB "$KEYS/nsB.key"
    ;;
  inspectC)
    inspect_one nsC "$KEYS/nsC.key"
    ;;
  *)
    echo "Usage: $0 startB|startAll|stopAll|inspectA|inspectB|inspectC"
    exit 1
    ;;
esac
