#!/usr/bin/env bash
set -euo pipefail

# Usage: lab_exfil_lowslow.sh <NS> <DST_OV6> <SECONDS> <PORT> [INTERVAL_SEC] [BYTES]
NS="${1:-nsC}"
DST="${2:-}"
SECS="${3:-60}"
PORT="${4:-18080}"
INTERVAL="${5:-0.5}"
BYTES="${6:-256}"

if [[ -z "$DST" ]]; then
  echo "[ERR] DST_OV6 is required"; exit 1
fi

# small payload
PAY="$(python - <<PY
print("A"*int($BYTES))
PY
)"

echo "[INFO] low-and-slow exfil: ns=$NS dst=[$DST]:$PORT secs=$SECS interval=$INTERVAL bytes=$BYTES"

# Start a simple listener in nsB is expected elsewhere; this script only sends.
end=$(( $(date +%s) + SECS ))
i=0
while [[ $(date +%s) -lt $end ]]; do
  i=$((i+1))
  # One short connection per interval
  sudo ip netns exec "$NS" bash -lc "printf '%s' '$PAY' | timeout 2 nc -6 -w 1 '$DST' '$PORT' >/dev/null 2>&1 || true"
  sleep "$INTERVAL"
done
echo "[OK] low-and-slow exfil done: iters=$i"
