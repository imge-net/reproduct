import argparse, numpy as np, pandas as pd
from sklearn.metrics import roc_curve, precision_recall_curve, f1_score, precision_score, recall_score, average_precision_score

ap = argparse.ArgumentParser()
ap.add_argument("--pred", required=True)  # parquet y_true,y_score
args = ap.parse_args()

df = pd.read_parquet(args.pred)
y = df["y_true"].to_numpy().astype(int)
s = df["y_score"].to_numpy().astype(float)

print("n=", len(y), "pos_rate=", y.mean())
print("PR-AUC=", round(average_precision_score(y,s), 6))

# Best F1 over thresholds
ths = np.quantile(s, np.linspace(0,1,2001))
best = (-1, None)
for t in ths:
    p = (s >= t).astype(int)
    f1 = f1_score(y,p)
    if f1 > best[0]:
        best = (f1, t)
t = best[1]
p = (s >= t).astype(int)
print("\n[BestF1]")
print(" thr=", float(t), "F1=", round(best[0],6),
      "P=", round(precision_score(y,p),6),
      "R=", round(recall_score(y,p),6))

# TPR at fixed FPR
fpr, tpr, thr = roc_curve(y, s)
for alpha in [0.001, 0.01, 0.05, 0.10]:
    idx = np.where(fpr <= alpha)[0]
    if len(idx)==0:
        print(f"[TPR@FPR<= {alpha}] NA")
    else:
        j = idx[np.argmax(tpr[idx])]
        print(f"[TPR@FPR<= {alpha}] TPR={tpr[j]:.6f} thr={thr[j]:.6g}")

# FPR at fixed TPR
for beta in [0.90, 0.95, 0.99]:
    idx = np.where(tpr >= beta)[0]
    if len(idx)==0:
        print(f"[FPR@TPR>= {beta}] NA")
    else:
        j = idx[0]
        print(f"[FPR@TPR>= {beta}] FPR={fpr[j]:.6f} thr={thr[j]:.6g}")
