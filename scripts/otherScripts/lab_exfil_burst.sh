#!/usr/bin/env bash
set -euo pipefail
DST_OV6="${1:?need dst}"
PORT="${2:-18080}"
N="${3:-200}"
CONC="${4:-20}"

# nsB sink
sudo ip netns exec nsB bash -lc "ss -ltnp | grep -q ':$PORT ' && fuser -k ${PORT}/tcp 2>/dev/null || true"
sudo ip netns exec nsB bash -lc "nohup sh -c 'nc -6 -lk -p $PORT > /dev/null' >/tmp/exfil_sink.log 2>&1 & echo \$! > /tmp/exfil_sink.pid"
sleep 0.3
sudo ip netns exec nsB bash -lc "ss -ltn | grep -q ':$PORT ' || { echo '[ERR] exfil sink not listening'; exit 2; }"

# nsC'den çok sayıda kısa bağlantı, daha büyük payload
sudo ip netns exec nsC bash -lc "
seq 1 $N | xargs -I{} -P $CONC sh -lc 'head -c 20000 </dev/urandom | nc -6 -w 1 $DST_OV6 $PORT >/dev/null 2>&1 || true'
"

sudo ip netns exec nsB bash -lc "kill \$(cat /tmp/exfil_sink.pid) 2>/dev/null || true; rm -f /tmp/exfil_sink.pid"
echo \"[OK] exfil burst: N=$N CONC=$CONC to [$DST_OV6]:$PORT\"
