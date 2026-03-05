#!/usr/bin/env bash
set -euo pipefail

OUT="${1:-$HOME/work/mycelium_ids/lab/flows/lab_flows.csv}"
DUR="${DUR:-20}"
IFACE="${IFACE:-vethB}"

TMPDIR="/tmp/myc_tshark_$$"
mkdir -p "$TMPDIR"
PCAP="$TMPDIR/cap.pcap"

command -v tcpdump >/dev/null || { echo "[ERR] tcpdump missing"; exit 1; }
command -v tshark >/dev/null || { echo "[ERR] tshark missing"; exit 1; }

echo "[INFO] capture nsB/$IFACE for ${DUR}s -> $OUT"

sudo ip netns exec nsB bash -lc "timeout $DUR tcpdump -i $IFACE -w $PCAP -n >/dev/null 2>&1 || true"

# Parse tshark conversation tables (TCP+UDP) into a simple flow CSV.
tshark -r "$PCAP" -q -z conv,tcp -z conv,udp \
| python - <<'PY' > "$OUT"
import sys, re

print("Dur,Proto,SrcAddr,Sport,DstAddr,Dport,SrcPkts,DstPkts,SrcBytes,DstBytes,State")
txt=sys.stdin.read().splitlines()

def parse(proto_label, header):
    rows=[]
    start=False
    for line in txt:
        if line.strip()==header:
            start=True
            continue
        if not start:
            continue
        if line.strip()=="" or line.strip().startswith("===="):
            continue
        # heuristic end
        if "Conversations" in line and line.strip()!=header:
            if proto_label=="udp":
                break
            continue
        m=re.search(r"(\S+):(\d+)\s+<->\s+(\S+):(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+([\d\.]+)", line)
        if not m:
            continue
        sa,sp,da,dp,spk,sby,dpk,dby,dur=m.groups()
        rows.append((float(dur), proto_label, sa, int(sp), da, int(dp), int(spk), int(dpk), int(sby), int(dby)))
    return rows

rows=parse("tcp","TCP Conversations")+parse("udp","UDP Conversations")
for dur,proto,sa,sp,da,dp,spk,dpk,sby,dby in rows:
    print(f"{dur},{proto},{sa},{sp},{da},{dp},{spk},{dpk},{sby},{dby},OK")
PY

rm -rf "$TMPDIR"
echo "[OK] wrote $OUT"
