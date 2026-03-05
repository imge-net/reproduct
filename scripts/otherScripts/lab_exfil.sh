#!/usr/bin/env bash
set -euo pipefail

NS_SRC="${1:-nsC}"
DST_OV6="${2:?need dst overlay ipv6}"
DUR="${3:-20}"         # seconds
PORT="${4:-18080}"     # use a high port to avoid existing HTTP servers

# Kill anything listening on PORT inside nsB (best-effort)
sudo ip netns exec nsB bash -lc "ss -ltnp | grep -q ':$PORT ' && \
  (echo '[INFO] killing existing listener on :$PORT'; fuser -k ${PORT}/tcp 2>/dev/null || true) || true"

# Start sink in nsB and verify it actually binds
sudo ip netns exec nsB bash -lc "nohup sh -c 'nc -6 -lk -p $PORT > /dev/null' >/tmp/exfil_sink.log 2>&1 & echo \$! > /tmp/exfil_sink.pid"

sleep 0.5
# Verify listener exists
sudo ip netns exec nsB bash -lc "ss -ltn | grep -q ':$PORT ' || { echo '[ERR] sink not listening on :$PORT'; cat /tmp/exfil_sink.log || true; exit 2; }"

# Exfil from nsC -> nsB:PORT (raw bytes)
sudo ip netns exec "$NS_SRC" bash -lc "timeout $DUR sh -c 'head -c 200000000 </dev/urandom | nc -6 $DST_OV6 $PORT' || true"

# Stop sink
sudo ip netns exec nsB bash -lc "kill \$(cat /tmp/exfil_sink.pid) 2>/dev/null || true; rm -f /tmp/exfil_sink.pid"

echo "[OK] exfil done: ${NS_SRC} -> [${DST_OV6}]:${PORT} for ${DUR}s"
