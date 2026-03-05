import argparse, pandas as pd, numpy as np

ap = argparse.ArgumentParser()
ap.add_argument("--benign_csv", required=True)
ap.add_argument("--attack_csv", required=True)
ap.add_argument("--out_csv", required=True)
args = ap.parse_args()

def load(csv_path):
    df = pd.read_csv(csv_path)
    df.columns = [c.strip() for c in df.columns]
    # rename to canonical
    ren = {
        "SrcAddr":"saddr","DstAddr":"daddr","Dur":"dur","Proto":"proto",
        "SrcPkts":"spkts","DstPkts":"dpkts","SrcBytes":"sbytes","DstBytes":"dbytes",
        "State":"state","Sport":"sport","Dport":"dport"
    }
    df = df.rename(columns={k:v for k,v in ren.items() if k in df.columns})
    # drop junk rows
    for c in ["saddr","daddr"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    df = df[(df["saddr"] != "0") & (df["daddr"] != "0")]
    df = df[df["saddr"].str.contains(":") & df["daddr"].str.contains(":")]
    # numeric cleanup
    for c in ["dur","spkts","dpkts","sbytes","dbytes"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    return df

ben = load(args.benign_csv)
att = load(args.attack_csv)

ben["y_true"] = 0
att["y_true"] = 1

df = pd.concat([ben, att], ignore_index=True)
df.to_csv(args.out_csv, index=False)

print("[OK] wrote", args.out_csv)
print("n=", len(df), "pos_rate=", float(df["y_true"].mean()))
print(df["y_true"].value_counts())
print("columns:", list(df.columns))
