import argparse
import subprocess
import pandas as pd
import numpy as np

ap = argparse.ArgumentParser()
ap.add_argument("--pcap", required=True)
ap.add_argument("--out_csv", required=True)
ap.add_argument("--max_packets", type=int, default=0, help="0=all packets")
args = ap.parse_args()

# tshark fields (IPv4/IPv6 + TCP/UDP)
fields = [
    "frame.time_epoch",
    "ip.src","ip.dst","ipv6.src","ipv6.dst",
    "ip.proto","ipv6.nxt",
    "tcp.srcport","tcp.dstport",
    "udp.srcport","udp.dstport",
    "frame.len",
]
cmd = ["tshark","-r",args.pcap,"-T","fields"]
for f in fields:
    cmd += ["-e", f]
cmd += ["-E","separator=,","-E","quote=d","-E","occurrence=f"]
# Filter: only IP packets (v4 or v6) and TCP/UDP
cmd += ["-Y","(ip or ipv6) and (tcp or udp)"]
if args.max_packets and args.max_packets > 0:
    cmd += ["-c", str(args.max_packets)]

proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
if proc.returncode != 0:
    raise SystemExit(f"[ERR] tshark failed:\n{proc.stderr[:2000]}")

lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
if not lines:
    # still write header
    pd.DataFrame(columns=["dur","proto","saddr","sport","daddr","dport","spkts","dpkts","sbytes","dbytes","state"]).to_csv(args.out_csv,index=False)
    print("[WARN] no TCP/UDP packets parsed; wrote empty CSV")
    raise SystemExit(0)

# Parse rows
rows=[]
for ln in lines:
    parts = [p.strip().strip('"') for p in ln.split(",")]
    # tshark might output fewer cols if missing; pad
    if len(parts) < len(fields):
        parts += [""]*(len(fields)-len(parts))
    m = dict(zip(fields, parts))
    t = float(m["frame.time_epoch"]) if m["frame.time_epoch"] else np.nan

    saddr = m["ipv6.src"] or m["ip.src"]
    daddr = m["ipv6.dst"] or m["ip.dst"]

    # proto
    proto = "tcp" if (m["tcp.srcport"] or m["tcp.dstport"]) else "udp"

    sport = m["tcp.srcport"] or m["udp.srcport"] or ""
    dport = m["tcp.dstport"] or m["udp.dstport"] or ""

    try:
        blen = int(m["frame.len"]) if m["frame.len"] else 0
    except:
        blen = 0

    # flow key (directional for now)
    key = (proto, saddr, sport, daddr, dport)
    rows.append((key, t, blen))

# Aggregate to flow-like stats
# We approximate "dur" as (max_t - min_t) in that 5-tuple.
# Packets/bytes split: we keep directional (s->d) only. dpkts/dbytes = 0 (since we don't track reverse key here).
# For equivalence check (myc0 vs vethB) that's sufficient and stable.
from collections import defaultdict
agg = defaultdict(lambda: {"tmin":np.inf,"tmax":-np.inf,"spkts":0,"sbytes":0})
for key,t,blen in rows:
    a=agg[key]
    if np.isfinite(t):
        a["tmin"]=min(a["tmin"], t)
        a["tmax"]=max(a["tmax"], t)
    a["spkts"] += 1
    a["sbytes"] += blen

out=[]
for (proto,saddr,sport,daddr,dport),a in agg.items():
    dur = 0.0 if (not np.isfinite(a["tmin"]) or not np.isfinite(a["tmax"])) else float(a["tmax"]-a["tmin"])
    out.append({
        "dur": dur,
        "proto": proto,
        "saddr": saddr,
        "sport": sport,
        "daddr": daddr,
        "dport": dport,
        "spkts": a["spkts"],
        "dpkts": 0,
        "sbytes": a["sbytes"],
        "dbytes": 0,
        "state": "NA",
    })

df=pd.DataFrame(out)
df.to_csv(args.out_csv, index=False)
print("[OK] wrote", args.out_csv, "rows=", len(df))
