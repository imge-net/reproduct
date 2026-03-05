#!/usr/bin/env bash
set -euo pipefail

ROOT="$HOME/work/mycelium_ids"
RUNS="$ROOT/runs"
TS="${1:-$(date +%Y%m%d_%H%M%S)}"
RUN="$RUNS/$TS"
mkdir -p "$RUN"

# Config (override via env)
NS="${NS:-nsB}"
IF1="${IF1:-myc0}"     # overlay
IF2="${IF2:-vethB}"    # underlay
DUR="${DUR:-30}"       # seconds
CAP_FILTER="${CAP_FILTER:-}"   # keep simple, can be refined

# Traffic trigger (optional)
# If you want to generate a controlled burst while capturing:
TRIGGER="${TRIGGER:-0}"     # set 1 to trigger exfil burst during capture
B_OV6="${B_OV6:-}"          # required if TRIGGER=1
EXFIL_DUR="${EXFIL_DUR:-10}"
EXFIL_PORT="${EXFIL_PORT:-18080}"

need() { command -v "$1" >/dev/null 2>&1 || { echo "[ERR] missing: $1"; exit 1; }; }
need tshark
need tcpdump

echo "[INFO] RUN=$RUN"
echo "[INFO] NS=$NS IF1=$IF1 IF2=$IF2 DUR=$DUR filter=$CAP_FILTER"

P1="$RUN/${NS}_${IF1}.pcap"
P2="$RUN/${NS}_${IF2}.pcap"

# Start captures (requires sudo)
echo "[INFO] starting tcpdump on $IF1 and $IF2 ..."
sudo ip netns exec "$NS" tcpdump -i "$IF1" -n -s 0 -w "$P1" ${CAP_FILTER:+$CAP_FILTER} >/dev/null 2>&1 &
PID1=$!
sudo ip netns exec "$NS" tcpdump -i "$IF2" -n -s 0 -w "$P2" ${CAP_FILTER:+$CAP_FILTER} >/dev/null 2>&1 &
PID2=$!

# Optional traffic trigger in parallel (underlay/overlay independent)
if [[ "$TRIGGER" == "1" ]]; then
  if [[ -z "$B_OV6" ]]; then
    echo "[ERR] TRIGGER=1 requires B_OV6 set"; exit 1
  fi
  echo "[INFO] TRIGGER=1: exfil nsC -> [$B_OV6]:$EXFIL_PORT for ${EXFIL_DUR}s"
  bash "$ROOT/scripts/lab_exfil.sh" nsC "$B_OV6" "$EXFIL_DUR" "$EXFIL_PORT" >/dev/null 2>&1 || true
fi

# Wait duration then stop
sleep "$DUR"
echo "[INFO] stopping captures..."
sudo kill "$PID1" 2>/dev/null || true
sudo kill "$PID2" 2>/dev/null || true
wait "$PID1" 2>/dev/null || true
wait "$PID2" 2>/dev/null || true

echo "[OK] pcaps: $P1 , $P2"

# Export to "flow-like" CSV using tshark conversation stats (TCP/UDP only)
# Output columns: dur, proto, saddr, sport, daddr, dport, spkts, dpkts, sbytes, dbytes, state
export_one () {
  local PCAP=""
  local OUT=""

  local TMPDIR
  TMPDIR="1001 27 1001mktemp -d)"
  local ARG="/capture.arg"

  # Convert PCAP -> Argus
  # -r: read pcap, -w: write argus binary
  argus -r "" -w "" 2>/dev/null || true

  # Export selected fields; use comma-separated output
  # We keep the same header as the other pipelines.
  {
    echo "dur,proto,saddr,sport,daddr,dport,spkts,dpkts,sbytes,dbytes,state"
    ra -r "" -n -c , -s dur proto saddr sport daddr dport spkts dpkts sbytes dbytes state 2>/dev/null || true
  } > ""

  rm -rf ""
}


CSV1="$RUN/${NS}_${IF1}_flows.csv"
CSV2="$RUN/${NS}_${IF2}_flows.csv"

echo "[INFO] exporting flow CSVs (tshark conv stats)..."
export_one "$P1" "$CSV1"
export_one "$P2" "$CSV2"

echo "[OK] wrote:"
ls -lh "$CSV1" "$CSV2"
echo "[HINT] Next: python scripts/iface_equivalence_report.py --ref $CSV1 --tgt $CSV2 --out $RUN/iface_equivalence.txt"
