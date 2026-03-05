#!/usr/bin/env bash
set -euo pipefail

OUT="${1:-$HOME/work/mycelium_ids/lab/flows/lab_flows.csv}"
DUR="${DUR:-20}"     # seconds capture
IFACE="${IFACE:-myc0}"

TMPDIR="/tmp/myc_cap_$$"
mkdir -p "$TMPDIR"
PCAP="$TMPDIR/cap.pcap"

# need tools
command -v tcpdump >/dev/null || { echo "[ERR] tcpdump missing"; exit 1; }
command -v argus >/dev/null || { echo "[ERR] argus missing"; exit 1; }
command -v ra >/dev/null || { echo "[ERR] argus ra missing"; exit 1; }

echo "[INFO] capture nsB/$IFACE for ${DUR}s -> $OUT"

sudo ip netns exec nsB bash -lc "timeout $DUR tcpdump -i $IFACE -w $PCAP -n >/dev/null 2>&1 || true"

# Argus convert: pcap -> argus binary -> CSV
ARG="$TMPDIR/argus.bin"
argus -r "$PCAP" -w "$ARG" >/dev/null 2>&1 || true

# CSV header compatible with your label_lab_flows.py mapping (Argus default fields)
ra -r "$ARG" -n -c , -s dur proto saddr sport daddr dport spkts dpkts sbytes dbytes state \
  > "$OUT"

rm -rf "$TMPDIR"
echo "[OK] wrote $OUT"
