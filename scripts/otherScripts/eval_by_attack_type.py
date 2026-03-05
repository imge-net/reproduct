import argparse, pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score

ap = argparse.ArgumentParser()
ap.add_argument("--data_parquet", required=True, help="merged dataset with attack_type,y_true")
ap.add_argument("--pred_parquet", required=True, help="preds with y_true,y_score (same row order)")
ap.add_argument("--out", required=True)
args = ap.parse_args()

df = pd.read_parquet(args.data_parquet)
pr = pd.read_parquet(args.pred_parquet)

# safety: align by length
n = min(len(df), len(pr))
df = df.iloc[:n].copy()
pr = pr.iloc[:n].copy()

df["y_score"] = pr["y_score"].to_numpy()
df["y_true"]  = pr["y_true"].to_numpy()

def safe_roc(y, s):
    try: return roc_auc_score(y, s)
    except: return float("nan")

rows=[]
for at in sorted(df["attack_type"].astype(str).unique()):
    sub = df[df["attack_type"].astype(str)==at]
    y = sub["y_true"].to_numpy()
    s = sub["y_score"].to_numpy()
    rows.append({
        "attack_type": at,
        "n": len(sub),
        "pos_rate": float(y.mean()),
        "pr_auc": float(average_precision_score(y,s)) if len(set(y))>1 else float("nan"),
        "roc_auc": float(safe_roc(y,s)) if len(set(y))>1 else float("nan"),
        "mean_score_pos": float(s[y==1].mean()) if (y==1).any() else float("nan"),
        "mean_score_neg": float(s[y==0].mean()) if (y==0).any() else float("nan"),
    })

out = pd.DataFrame(rows).sort_values(["attack_type"])
out.to_csv(args.out, index=False)
print(out.to_string(index=False))
print("[OK] wrote", args.out)
