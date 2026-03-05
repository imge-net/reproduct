import argparse, math
import numpy as np
import pandas as pd

def ks_stat(x, y):
    # simple two-sample KS statistic
    x = np.sort(np.asarray(x))
    y = np.sort(np.asarray(y))
    if len(x) == 0 or len(y) == 0:
        return float("nan")
    allv = np.sort(np.unique(np.concatenate([x, y])))
    # empirical CDFs
    cx = np.searchsorted(x, allv, side="right") / len(x)
    cy = np.searchsorted(y, allv, side="right") / len(y)
    return float(np.max(np.abs(cx - cy)))

def psi(ref, tgt, bins=10, eps=1e-8):
    # Population Stability Index using quantile bins from ref
    ref = np.asarray(ref)
    tgt = np.asarray(tgt)
    ref = ref[np.isfinite(ref)]
    tgt = tgt[np.isfinite(tgt)]
    if len(ref) == 0 or len(tgt) == 0:
        return float("nan")
    qs = np.quantile(ref, np.linspace(0, 1, bins + 1))
    # make bin edges strictly increasing
    qs = np.unique(qs)
    if len(qs) < 3:
        return float("nan")
    ref_hist, _ = np.histogram(ref, bins=qs)
    tgt_hist, _ = np.histogram(tgt, bins=qs)
    ref_p = ref_hist / max(ref_hist.sum(), 1)
    tgt_p = tgt_hist / max(tgt_hist.sum(), 1)
    ref_p = np.clip(ref_p, eps, 1.0)
    tgt_p = np.clip(tgt_p, eps, 1.0)
    return float(np.sum((tgt_p - ref_p) * np.log(tgt_p / ref_p)))

def load(csv_path):
    df = pd.read_csv(csv_path)
    df.columns = [c.strip() for c in df.columns]
    # coerce numerics
    for c in ["dur","spkts","dpkts","sbytes","dbytes"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ["proto","state","saddr","daddr"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df

ap = argparse.ArgumentParser()
ap.add_argument("--ref", required=True, help="reference CSV (e.g., myc0)")
ap.add_argument("--tgt", required=True, help="target CSV (e.g., vethB)")
ap.add_argument("--out", required=True)
args = ap.parse_args()

ref = load(args.ref)
tgt = load(args.tgt)

lines = []
def w(s=""):
    lines.append(s)

w("== Interface equivalence report ==")
w(f"ref={args.ref}")
w(f"tgt={args.tgt}")
w("")
w(f"[counts] ref_flows={len(ref)} tgt_flows={len(tgt)}")

# proto dist
def top_counts(df, col, k=10):
    vc = df[col].value_counts(dropna=False).head(k)
    return ", ".join([f"{i}:{int(v)}" for i,v in vc.items()])

if "proto" in ref.columns and "proto" in tgt.columns:
    w(f"[proto] ref: {top_counts(ref,'proto')}") 
    w(f"[proto] tgt: {top_counts(tgt,'proto')}")

if "state" in ref.columns and "state" in tgt.columns:
    w(f"[state] ref: {top_counts(ref,'state')}")
    w(f"[state] tgt: {top_counts(tgt,'state')}")

w("")
w("== Numeric shift (PSI / KS) ==")
num_cols = [c for c in ["dur","spkts","dpkts","sbytes","dbytes"] if c in ref.columns and c in tgt.columns]
rows=[]
for c in num_cols:
    r = ref[c].to_numpy()
    t = tgt[c].to_numpy()
    rows.append((c, psi(r,t), ks_stat(r,t), np.nanmean(r), np.nanmean(t)))
rows = sorted(rows, key=lambda x: (-(x[1] if np.isfinite(x[1]) else -1), -(x[2] if np.isfinite(x[2]) else -1)))
w("feature,psi,ks,mean_ref,mean_tgt")
for c,ps,ks,mr,mt in rows:
    w(f"{c},{ps:.6f},{ks:.6f},{mr:.6g},{mt:.6g}")

# simple verdict
w("")
w("== Quick interpretation ==")
if len(rows) > 0:
    maxpsi = max([r[1] for r in rows if np.isfinite(r[1])] + [float("nan")])
    maxks  = max([r[2] for r in rows if np.isfinite(r[2])] + [float("nan")])
    w(f"max_PSI={maxpsi:.4f} (higher => stronger distribution shift)")
    w(f"max_KS ={maxks:.4f} (higher => stronger distribution shift)")
    w("Rule-of-thumb: PSI < 0.1 small, 0.1-0.25 moderate, >0.25 large (context-dependent).")

with open(args.out, "w", encoding="utf-8") as f:
    f.write("\n".join(lines) + "\n")

print("[OK] wrote", args.out)
