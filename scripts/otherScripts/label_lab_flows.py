import argparse
import pandas as pd
import numpy as np

ap = argparse.ArgumentParser()
ap.add_argument("--in_csv", required=True)
ap.add_argument("--attacker_ip", required=True)
ap.add_argument("--out_csv", required=True)
args = ap.parse_args()

df = pd.read_csv(args.in_csv)
# normalize column names (strip spaces)
df.columns = [c.strip() for c in df.columns]

# Map Argus headers -> our canonical names
rename_map = {}
if "SrcAddr" in df.columns: rename_map["SrcAddr"] = "saddr"
if "DstAddr" in df.columns: rename_map["DstAddr"] = "daddr"
if "Dur" in df.columns: rename_map["Dur"] = "dur"
if "Proto" in df.columns: rename_map["Proto"] = "proto"
if "SrcPkts" in df.columns: rename_map["SrcPkts"] = "spkts"
if "DstPkts" in df.columns: rename_map["DstPkts"] = "dpkts"
if "SrcBytes" in df.columns: rename_map["SrcBytes"] = "sbytes"
if "DstBytes" in df.columns: rename_map["DstBytes"] = "dbytes"
if "State" in df.columns: rename_map["State"] = "state"
df = df.rename(columns=rename_map)

required = ["saddr","daddr","dur","proto","spkts","dpkts","sbytes","dbytes","state"]
missing = [c for c in required if c not in df.columns]
if missing:
    raise SystemExit(f"[ERR] Missing columns in flow CSV: {missing}\nColumns={list(df.columns)}")

# Drop obvious junk rows (e.g., 0 addresses)
df["saddr"] = df["saddr"].astype(str).str.strip()
df["daddr"] = df["daddr"].astype(str).str.strip()
df = df[(df["saddr"] != "0") & (df["daddr"] != "0")]

# Keep only IPv6-like flows (':' in addr)
df = df[df["saddr"].str.contains(":") & df["daddr"].str.contains(":")]

# Coerce numeric columns (blank -> NaN -> 0)
for c in ["dur","spkts","dpkts","sbytes","dbytes"]:
    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

# Label: attacker src = 1
att = str(args.attacker_ip).strip()
df["y_true"] = (df["saddr"] == att).astype(int)

df.to_csv(args.out_csv, index=False)
print("[OK] wrote", args.out_csv, "n=", len(df), "pos_rate=", float(df["y_true"].mean()))
print(df["y_true"].value_counts())
