#!/usr/bin/env bash
set -euo pipefail
# lateral: nsA "legit" gibi davranır, nsC "attacker" gibi davranır; ikisi de iç ağda hareket eder
SINK_IP="${1:-10.10.0.3}"
# iç ağda başka hedef yoksa bile "çok port + çok bağlantı" paterni oluşsun diye sink'e çeşitli portlar deneriz
PORTS="${2:-22 80 443 445 3389 8080 18080 18081 18082 18083}"
REPS="${3:-200}"
SLEEP="${4:-0.01}"

# nsA -> nsB “service probe” + nsC -> nsB “service probe”
for NS in nsA nsC; do
  sudo ip netns exec "$NS" bash -lc '
  set -euo pipefail
  DST="'"$SINK_IP"'"
  PORTS="'"$PORTS"'"
  REPS='"$REPS"'
  SL='"$SLEEP"'
  for i in $(seq 1 $REPS); do
    for p in $PORTS; do
      timeout 0.2 bash -lc "echo hi >/dev/tcp/$DST/$p" >/dev/null 2>&1 || true
      sleep "$SL"
    done
  done
  '
done

echo "[OK] lateral done: (nsA+nsC) -> $SINK_IP probes reps=$REPS"
