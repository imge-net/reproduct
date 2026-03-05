#!/usr/bin/env bash
set -euo pipefail

OUT="${1:-$HOME/work/mycelium_ids/lab/flows/lab_flows.csv}"
DUR="${DUR:-20}"
IFACE="${IFACE:-vethB}"
WINDOW="${WINDOW:-1.0}"   # seconds

TMPDIR="/tmp/myc_pktagg_$$"
mkdir -p "$TMPDIR"
PCAP="$TMPDIR/cap.pcap"
TSV="$TMPDIR/pkts.tsv"

command -v tcpdump >/dev/null || { echo "[ERR] tcpdump missing"; exit 1; }
command -v tshark  >/dev/null || { echo "[ERR] tshark missing"; exit 1; }

echo "[INFO] pktagg capture nsB/$IFACE for ${DUR}s window=${WINDOW}s -> $OUT"

sudo ip netns exec nsB bash -lc "timeout $DUR tcpdump -i $IFACE -w $PCAP -n >/dev/null 2>&1 || true"

tshark -r "$PCAP" -T fields -E separator=$'\t' -E occurrence=f \
  -e frame.time_epoch \
  -e ip.proto -e ipv6.nxt \
  -e ip.src -e ip.dst \
  -e ipv6.src -e ipv6.dst \
  -e tcp.srcport -e tcp.dstport \
  -e udp.srcport -e udp.dstport \
  -e frame.len \
  > "$TSV" || true

python - <<'PY' "$TSV" "$OUT" "$WINDOW"
import sys, pandas as pd, numpy as np, math

tsv, out, window = sys.argv[1], sys.argv[2], float(sys.argv[3])
cols = ["t","ip_proto","v6_nxt","ip_src","ip_dst","v6_src","v6_dst","tcp_sp","tcp_dp","udp_sp","udp_dp","flen"]
df = pd.read_csv(tsv, sep="\t", names=cols, header=None)

src = df["v6_src"].fillna(df["ip_src"])
dst = df["v6_dst"].fillna(df["ip_dst"])
df["src"] = src.astype(str)
df["dst"] = dst.astype(str)

proto = df["v6_nxt"].fillna(df["ip_proto"]).fillna("")
df["proto_num"] = proto.astype(str)

sp = df["tcp_sp"].fillna(df["udp_sp"])
dp = df["tcp_dp"].fillna(df["udp_dp"])
df["sport"] = pd.to_numeric(sp, errors="coerce").fillna(0).astype(int)
df["dport"] = pd.to_numeric(dp, errors="coerce").fillna(0).astype(int)

df["bytes"] = pd.to_numeric(df["flen"], errors="coerce").fillna(0).astype(int)
df["t"] = pd.to_numeric(df["t"], errors="coerce")
df = df.dropna(subset=["t"])

df = df[(df["src"]!="nan") & (df["dst"]!="nan") & (df["src"]!="") & (df["dst"]!="")]

def proto_name(p):
    if p=="6": return "tcp"
    if p=="17": return "udp"
    if p=="58": return "ipv6-icmp"
    if p=="1": return "icmp"
    return p if p else "unk"

df["proto"] = df["proto_num"].map(proto_name)

# time-window slicing
t0 = df["t"].min()
df["win"] = ((df["t"] - t0) / window).astype(int)

g = df.groupby(["win","proto","src","sport","dst","dport"], dropna=False)
out_df = g.agg(
    dur=("t", lambda x: float(x.max()-x.min()) if len(x)>0 else 0.0),
    spkts=("t","count"),
    sbytes=("bytes","sum"),
).reset_index()

out_df["dpkts"] = 0
out_df["dbytes"] = 0
out_df["state"] = "OK"
out_df = out_df.rename(columns={"src":"saddr","dst":"daddr"})
out_df = out_df[["dur","proto","saddr","sport","daddr","dport","spkts","dpkts","sbytes","dbytes","state"]]

out_df.to_csv(out, index=False)
print("[OK] flows:", len(out_df))
PY

rm -rf "$TMPDIR"
echo "[OK] wrote $OUT"
