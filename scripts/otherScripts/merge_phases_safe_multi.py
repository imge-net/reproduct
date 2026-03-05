#!/usr/bin/env python3
import argparse, glob, os, re
import pandas as pd

PAT = re.compile(r"^(benign|attack_exfil_burst|attack_exfil_lowslow)_(\d+)\.csv$")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phases_dir", required=True)
    ap.add_argument("--out_parquet", required=True)
    ap.add_argument("--out_csv", default="")
    args = ap.parse_args()

    files = sorted(glob.glob(os.path.join(args.phases_dir, "*.csv")))
    if not files:
        raise SystemExit(f"[ERR] no csv found in {args.phases_dir}")

    dfs = []
    kept = 0
    for fp in files:
        base = os.path.basename(fp)
        m = PAT.match(base)
        if not m:
            continue
        kept += 1
        attack_type = m.group(1)
        rep = int(m.group(2))
        df = pd.read_csv(fp)
        df.columns = [c.strip() for c in df.columns]
        df["attack_type"] = attack_type
        df["rep"] = rep
        if "y_true" not in df.columns:
            df["y_true"] = 0 if attack_type == "benign" else 1
        dfs.append(df)

    if not dfs:
        raise SystemExit(f"[ERR] no matching phase csvs (kept={kept}) in {args.phases_dir}")

    out = pd.concat(dfs, ignore_index=True)

    os.makedirs(os.path.dirname(args.out_parquet), exist_ok=True)
    out.to_parquet(args.out_parquet, index=False)

    if args.out_csv:
        os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)
        out.to_csv(args.out_csv, index=False)

    print("[OK] wrote", args.out_parquet, "rows=", len(out), "pos_rate=", float(out["y_true"].mean()))
    print(out["attack_type"].value_counts().to_string())
    print("rep min/max:", int(out["rep"].min()), int(out["rep"].max()))

if __name__ == "__main__":
    main()
