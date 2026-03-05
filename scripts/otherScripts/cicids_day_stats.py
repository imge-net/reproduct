import os, glob, pandas as pd

proc = os.path.expanduser("~/work/mycelium_ids/data/cicids/processed")

def load_day(day):
    base = os.path.join(proc, f"cicids_{day}.parquet")
    parts = sorted(glob.glob(base.replace(".parquet", ".part_*.parquet")))
    dfs = []
    if os.path.exists(base): dfs.append(pd.read_parquet(base, columns=["Label"]))
    for p in parts: dfs.append(pd.read_parquet(p, columns=["Label"]))
    df = pd.concat(dfs, ignore_index=True)
    lab = df["Label"].astype(str).str.strip()
    return lab

days = ["Monday","Tuesday","Wednesday","Thursday","Friday"]
for d in days:
    lab = load_day(d)
    print("\n==", d, "==")
    print("n =", len(lab))
    vc = lab.value_counts()
    print(vc.head(15))
    benign = (lab.str.upper()=="BENIGN").mean()
    print("benign_ratio=", round(benign,4), "attack_ratio=", round(1-benign,4))
