import argparse, pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score

ap = argparse.ArgumentParser()
ap.add_argument("--data_parquet", required=True)   # merged dataset: attack_type, y_true
ap.add_argument("--pred_parquet", required=True)   # preds: y_true, y_score (same order)
ap.add_argument("--out_csv", required=True)
args = ap.parse_args()

df = pd.read_parquet(args.data_parquet)
pr = pd.read_parquet(args.pred_parquet)

n = min(len(df), len(pr))
df = df.iloc[:n].copy()
pr = pr.iloc[:n].copy()
df["y_score"] = pr["y_score"].to_numpy()
df["y_true"]  = pr["y_true"].to_numpy()

def safe_roc(y,s):
    try: return roc_auc_score(y,s)
    except: return float("nan")

# Evaluate each attack family against benign only
families = sorted([x for x in df["attack_type"].astype(str).unique() if x != "benign"])
rows = []
for fam in families:
    sub = df[df["attack_type"].astype(str).isin(["benign", fam])].copy()
    y = sub["y_true"].to_numpy()
    s = sub["y_score"].to_numpy()
    rows.append({
        "attack_family": fam,
        "n": len(sub),
        "pos_rate": float(y.mean()),
        "pr_auc": float(average_precision_score(y,s)),
        "roc_auc": float(safe_roc(y,s)),
        "mean_score_pos": float(s[y==1].mean()),
        "mean_score_neg": float(s[y==0].mean()),
    })

out = pd.DataFrame(rows).sort_values("attack_family")
out.to_csv(args.out_csv, index=False)
print(out.to_string(index=False))
print("[OK] wrote", args.out_csv)
