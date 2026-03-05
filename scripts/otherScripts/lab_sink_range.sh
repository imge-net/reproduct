#!/usr/bin/env bash
set -euo pipefail
START="${1:-18080}"
END="${2:-18180}"

# kill previous nc
sudo ip netns exec nsB bash -lc "pkill -f 'nc -6 -lk' 2>/dev/null || true"

sudo ip netns exec nsB bash -lc "
nohup sh -c '
for p in \$(seq $START $END); do
  nc -6 -lk -p \$p > /dev/null 2>&1 &
done
wait
' >/tmp/sink_range.log 2>&1 & echo \$! > /tmp/sink_range.pid
"

sleep 0.5
sudo ip netns exec nsB bash -lc "ss -ltn | awk '{print \$4}' | grep -q ':$START' || { echo '[ERR] sink range not up'; exit 2; }"
echo \"[OK] sink range up: $START-$END\"
