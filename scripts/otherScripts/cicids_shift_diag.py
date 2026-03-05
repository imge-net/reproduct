import argparse, numpy as np, pandas as pd
from scipy.stats import ks_2samp

def psi(expected, actual, bins=10):
    # Population Stability Index (numeric)
    expected = np.asarray(expected, float)
    actual   = np.asarray(actual, float)
    expected = expected[np.isfinite(expected)]
    actual   = actual[np.isfinite(actual)]
    if len(expected) < 50 or len(actual) < 50:
        return np.nan
    qs = np.linspace(0, 1, bins+1)
    cuts = np.unique(np.quantile(expected, qs))
    if len(cuts) < 3:
        return np.nan
    e_counts, _ = np.histogram(expected, bins=cuts)
    a_counts, _ = np.histogram(actual, bins=cuts)
    e = e_counts / max(e_counts.sum(), 1)
    a = a_counts / max(a_counts.sum(), 1)
    eps = 1e-6
    e = np.clip(e, eps, 1)
    a = np.clip(a, eps, 1)
    return float(np.sum((a - e) * np.log(a / e)))

def is_cat(s: pd.Series):
    return s.dtype == "object" or str(s.dtype).startswith("string")

def cat_tv(expected, actual):
    # Total variation distance for categorical distributions
    e = expected.astype(str).fillna("NA").value_counts(normalize=True)
    a = actual.astype(str).fillna("NA").value_counts(normalize=True)
    idx = e.index.union(a.index)
    e = e.reindex(idx, fill_value=0.0)
    a = a.reindex(idx, fill_value=0.0)
    return float(0.5 * np.abs(e - a).sum())

ap = argparse.ArgumentParser()
ap.add_argument("--ref", required=True, help="reference parquet (e.g., Wednesday)")
ap.add_argument("--tgt", required=True, help="target parquet (e.g., Friday)")
ap.add_argument("--out", required=True)
ap.add_argument("--topk", type=int, default=25)
args = ap.parse_args()

Xr = pd.read_parquet(args.ref)
Xt = pd.read_parquet(args.tgt)

# Drop label columns if present
for col in ["Label","label","y_true","y_pred","y_score"]:
    if col in Xr.columns: Xr = Xr.drop(columns=[col])
    if col in Xt.columns: Xt = Xt.drop(columns=[col])

common = [c for c in Xr.columns if c in Xt.columns]
rows = []

for c in common:
    sr, st = Xr[c], Xt[c]
    if is_cat(sr) or is_cat(st):
        tv = cat_tv(sr, st)
        rows.append((c, "cat", np.nan, np.nan, tv, np.nan, np.nan))
    else:
        vr = pd.to_numeric(sr, errors="coerce")
        vt = pd.to_numeric(st, errors="coerce")
        ks = ks_2samp(vr.dropna().to_numpy(), vt.dropna().to_numpy(), alternative="two-sided", mode="auto")
        psi_val = psi(vr.to_numpy(), vt.to_numpy(), bins=10)
        rows.append((c, "num", psi_val, float(ks.statistic), np.nan, float(np.nanmean(vr)), float(np.nanmean(vt))))

df = pd.DataFrame(rows, columns=["feature","type","psi","ks","tv","mean_ref","mean_tgt"])

# Rank: numeric by PSI then KS; categorical by TV
df_num = df[df.type=="num"].copy()
df_cat = df[df.type=="cat"].copy()

df_num["rank_key"] = df_num["psi"].fillna(0) * 10 + df_num["ks"].fillna(0)
df_cat["rank_key"] = df_cat["tv"].fillna(0)

df_top = pd.concat([
    df_num.sort_values("rank_key", ascending=False).head(args.topk),
    df_cat.sort_values("rank_key", ascending=False).head(args.topk),
], ignore_index=True)

with open(args.out, "w") as f:
    f.write("# Shift diagnostic (ref vs target)\n")
    f.write(f"ref={args.ref}\n")
    f.write(f"tgt={args.tgt}\n\n")
    f.write("## Top numeric shifts (by PSI/KS)\n")
    f.write(df_num.sort_values("rank_key", ascending=False).head(args.topk).to_string(index=False))
    f.write("\n\n## Top categorical shifts (by TV distance)\n")
    f.write(df_cat.sort_values("rank_key", ascending=False).head(args.topk).to_string(index=False))
    f.write("\n\n## Notes\n")
    f.write("- PSI ~0.1: small, ~0.25: moderate, >0.5: large shift (rule-of-thumb).\n")
    f.write("- KS statistic closer to 1 means stronger distribution difference.\n")
    f.write("- TV in [0,1], higher means larger categorical drift.\n")

print("[OK] wrote", args.out)
