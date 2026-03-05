#!/usr/bin/env bash
set -euo pipefail
B_OV6="${1:?need B_OV6}"
PORT="${2:-18081}"
N="${3:-200}"          # kaç bağlantı
CONC="${4:-20}"        # paralellik

# nsB'de sink (TCP accept + discard)
sudo ip netns exec nsB bash -lc "ss -ltnp | grep -q ':$PORT ' && fuser -k ${PORT}/tcp 2>/dev/null || true"
sudo ip netns exec nsB bash -lc "nohup sh -c 'nc -6 -lk -p $PORT > /dev/null' >/tmp/benign_sink.log 2>&1 & echo \$! > /tmp/benign_sink.pid"

sleep 0.3
sudo ip netns exec nsB bash -lc "ss -ltn | grep -q ':$PORT ' || { echo '[ERR] benign sink not listening'; exit 2; }"

# nsA'dan çok sayıda kısa bağlantı (her biri küçük veri)
sudo ip netns exec nsA bash -lc "
seq 1 $N | xargs -I{} -P $CONC sh -lc 'printf \"hi{}\" | nc -6 -w 1 $B_OV6 $PORT >/dev/null 2>&1 || true'
"

sudo ip netns exec nsB bash -lc "kill \$(cat /tmp/benign_sink.pid) 2>/dev/null || true; rm -f /tmp/benign_sink.pid"
echo \"[OK] benign burst: N=$N CONC=$CONC to [$B_OV6]:$PORT\"
