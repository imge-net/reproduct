import argparse, glob, os
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--phases_dir", required=True)
ap.add_argument("--out_parquet", required=True)
args = ap.parse_args()

files = sorted(glob.glob(os.path.join(args.phases_dir, "*.csv")))
if not files:
    raise SystemExit(f"[ERR] no csv found in {args.phases_dir}")

dfs=[]
for p in files:
    df=pd.read_csv(p)
    df.columns=[c.strip() for c in df.columns]
    dfs.append(df)

df=pd.concat(dfs, ignore_index=True)

# canonicalize types
for c in ["dur","spkts","dpkts","sbytes","dbytes"]:
    if c in df.columns:
        df[c]=pd.to_numeric(df[c], errors="coerce").fillna(0.0)
for c in ["proto","state","saddr","daddr","sport","dport","attack_type"]:
    if c in df.columns:
        df[c]=df[c].astype(str).str.strip()

df["y_true"]=pd.to_numeric(df["y_true"], errors="coerce").fillna(0).astype(int)

df.to_parquet(args.out_parquet, index=False)
print("[OK] wrote", args.out_parquet, "rows=", len(df), "pos_rate=", df["y_true"].mean())
print(df["attack_type"].value_counts())
