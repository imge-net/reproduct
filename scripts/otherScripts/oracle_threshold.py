import argparse, numpy as np, pandas as pd
from sklearn.metrics import f1_score, precision_score, recall_score, roc_curve

def fpr_at_tpr(y, s, tpr_target=0.95):
    fpr, tpr, thr = roc_curve(y, s)
    idx = np.where(tpr >= tpr_target)[0]
    return np.nan if len(idx)==0 else float(fpr[idx[0]])

ap = argparse.ArgumentParser()
ap.add_argument("--pred", required=True)  # parquet: y_true,y_score
args = ap.parse_args()

df = pd.read_parquet(args.pred)
y = df["y_true"].to_numpy().astype(int)
s = df["y_score"].to_numpy().astype(float)

# scan thresholds
ths = np.quantile(s, np.linspace(0.0, 1.0, 2001))
best = (-1, None)

for t in ths:
    p = (s >= t).astype(int)
    f1 = f1_score(y, p)
    if f1 > best[0]:
        best = (f1, t)

t = best[1]
p = (s >= t).astype(int)

print("[ORACLE] best_F1=", round(best[0],6), "thr=", float(t))
print(" precision=", round(precision_score(y,p),6),
      " recall=", round(recall_score(y,p),6))
print(" FPR@TPR=0.95 (score-based) =", round(float(fpr_at_tpr(y,s,0.95)),6))
