import argparse
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score

def safe_roc_auc(y, s):
    try:
        return float(roc_auc_score(y, s))
    except Exception:
        return float("nan")

ap = argparse.ArgumentParser()
ap.add_argument("--data_parquet", required=True)
ap.add_argument("--pred_parquet", required=True)   # must contain row_id, y_score (y_true optional)
ap.add_argument("--out_csv", required=True)
args = ap.parse_args()

df = pd.read_parquet(args.data_parquet).reset_index(drop=True)
if "row_id" not in df.columns:
    df["row_id"] = range(len(df))

need_data = {"row_id", "attack_type", "y_true"}
miss = need_data - set(df.columns)
if miss:
    raise SystemExit(f"[ERR] data_parquet missing columns: {sorted(miss)}")

pr = pd.read_parquet(args.pred_parquet)
need_pred = {"row_id", "y_score"}
miss = need_pred - set(pr.columns)
if miss:
    raise SystemExit(f"[ERR] pred_parquet missing columns: {sorted(miss)}")

# Keep only required columns from pred; avoid y_true collision
pr = pr[["row_id", "y_score"]].copy()

m = pr.merge(df[["row_id", "attack_type", "y_true"]], on="row_id", how="left", validate="many_to_one")
if m["attack_type"].isna().any():
    bad = int(m["attack_type"].isna().sum())
    raise SystemExit(f"[ERR] row_id join failed: {bad} rows missing attack_type. Did eval write row_id from the same base parquet?")

rows = []
for at, sub in m.groupby("attack_type"):
    y = sub["y_true"].to_numpy().astype(int)
    s = sub["y_score"].to_numpy().astype(float)
    rows.append({
        "attack_type": str(at),
        "n": int(len(sub)),
        "pos_rate": float(y.mean()) if len(y) else float("nan"),
        "pr_auc": float(average_precision_score(y, s)) if len(set(y)) > 1 else float("nan"),
        "roc_auc": safe_roc_auc(y, s) if len(set(y)) > 1 else float("nan"),
        "mean_score_pos": float(s[y == 1].mean()) if (y == 1).any() else float("nan"),
        "mean_score_neg": float(s[y == 0].mean()) if (y == 0).any() else float("nan"),
    })

out = pd.DataFrame(rows).sort_values(["attack_type"])
out.to_csv(args.out_csv, index=False)

print("[OK] wrote", args.out_csv)
print(out.to_string(index=False))
