#!/usr/bin/env bash
set -euo pipefail
NS_SRC="${1:-nsC}"
DST_IP="${2:-10.10.0.3}"
PORT_START="${3:-1}"
PORT_END="${4:-1024}"
SLEEP="${5:-0.002}"   # çok hızlı olursa scheduler/limits etkileyebilir

# Basit TCP connect scan: /dev/tcp kullanır (bash built-in)
sudo ip netns exec "$NS_SRC" bash -lc '
set -euo pipefail
DST="'"$DST_IP"'"
P1='"$PORT_START"'
P2='"$PORT_END"'
SL="'"$SLEEP"'"
for p in $(seq $P1 $P2); do
  # connect dene; başarılı/başarısız fark etmez, deneme yeter
  timeout 0.2 bash -lc "echo >/dev/tcp/$DST/$p" >/dev/null 2>&1 || true
  sleep "$SL"
done
'
echo "[OK] scan done: $NS_SRC -> $DST_IP ports=$PORT_START-$PORT_END"
