import argparse
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score

def safe_roc_auc(y, s):
    try:
        return float(roc_auc_score(y, s))
    except Exception:
        return float("nan")

def slice_report(name, sub):
    y = sub["y_true"].to_numpy().astype(int)
    s = sub["y_score"].to_numpy().astype(float)
    rows = {
        "slice": name,
        "n": int(len(sub)),
        "pos_rate": float(y.mean()) if len(y) else float("nan"),
        "pr_auc": float(average_precision_score(y, s)) if len(y) else float("nan"),
        "roc_auc": safe_roc_auc(y, s),
        "mean_score_pos": float(s[y == 1].mean()) if (y == 1).any() else float("nan"),
        "mean_score_neg": float(s[y == 0].mean()) if (y == 0).any() else float("nan"),
    }
    return rows

ap = argparse.ArgumentParser()
ap.add_argument("--data_parquet", required=True)
ap.add_argument("--pred_parquet", required=True)  # must contain row_id,y_score (y_true optional)
ap.add_argument("--out_csv", required=True)
args = ap.parse_args()

df = pd.read_parquet(args.data_parquet).reset_index(drop=True)
if "row_id" not in df.columns:
    df["row_id"] = range(len(df))

need = {"row_id", "attack_type", "y_true"}
missing = need - set(df.columns)
if missing:
    raise SystemExit(f"[ERR] data_parquet missing columns: {sorted(missing)}")

pr = pd.read_parquet(args.pred_parquet)
needp = {"row_id", "y_score"}
missingp = needp - set(pr.columns)
if missingp:
    raise SystemExit(f"[ERR] pred_parquet missing columns: {sorted(missingp)}")

# Merge, but force y_true to be taken from data_parquet (ground truth)
m = pr.merge(df[["row_id", "attack_type", "y_true"]], on="row_id", how="left", validate="many_to_one")

if m["attack_type"].isna().any():
    bad = int(m["attack_type"].isna().sum())
    raise SystemExit(f"[ERR] row_id join failed: {bad} rows have missing attack_type")

# If pred also had y_true, pandas may create y_true_x/y_true_y; normalize:
if "y_true" not in m.columns:
    # common case: y_true_x from pred, y_true_y from data
    if "y_true_y" in m.columns:
        m["y_true"] = m["y_true_y"]
    elif "y_true_x" in m.columns:
        # fallback
        m["y_true"] = m["y_true_x"]
    else:
        raise SystemExit("[ERR] could not recover y_true after merge")

# Define slices
rows = []
rows.append(slice_report("ALL(test)", m))

# benign vs each attack type (assumes benign label exists)
if "benign" in set(m["attack_type"].astype(str).unique()):
    for at in sorted(set(m["attack_type"].astype(str).unique())):
        if at == "benign":
            continue
        sub = m[m["attack_type"].astype(str).isin(["benign", at])].copy()
        if len(sub):
            rows.append(slice_report(f"benign vs {at}", sub))

out = pd.DataFrame(rows)
out.to_csv(args.out_csv, index=False)
print("[OK] wrote", args.out_csv)
print(out)
