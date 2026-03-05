#!/usr/bin/env bash
set -euo pipefail
NS_SRC="${1:-nsC}"
DST_IP="${2:-10.10.0.3}"
DST_PORT="${3:-18083}"     # beacon port (benign/burst/slow ile çakışmasın)
SECS="${4:-120}"
INTERVAL="${5:-1.0}"       # beacon periyodu
BYTES="${6:-64}"

sudo ip netns exec "$NS_SRC" bash -lc '
set -euo pipefail
DST="'"$DST_IP"'"
PORT="'"$DST_PORT"'"
SECS="'"$SECS"'"
INTV="'"$INTERVAL"'"
BYTES="'"$BYTES"'"

end=$(( $(date +%s) + SECS ))
payload=$(python - <<PY
print("A"*int("'"$BYTES"'"))
PY
)

while [ $(date +%s) -lt $end ]; do
  # kısa bağlantı + küçük payload
  timeout 0.3 bash -lc "printf \"%s\" \"$payload\" >/dev/tcp/$DST/$PORT" >/dev/null 2>&1 || true
  sleep "$INTV"
done
'
echo "[OK] c2-beacon done: $NS_SRC -> $DST_IP:$DST_PORT secs=$SECS interval=$INTERVAL bytes=$BYTES"
