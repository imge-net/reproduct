import argparse, joblib
import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, classification_report, confusion_matrix, roc_curve

def fpr_at_tpr(y_true, y_score, target_tpr=0.95):
    fpr, tpr, _ = roc_curve(y_true, y_score)
    idx = np.where(tpr >= target_tpr)[0]
    return float("nan") if len(idx)==0 else float(fpr[idx[0]])

ap = argparse.ArgumentParser()
ap.add_argument("--model", required=True)
ap.add_argument("--csv", required=True)
ap.add_argument("--out_parquet", required=True)
args = ap.parse_args()

obj = joblib.load(args.model)
pipe = obj["pipe"]
feats = obj["features"]

df = pd.read_csv(args.csv)
df.columns = [c.strip() for c in df.columns]
y = df["y_true"].astype(int).to_numpy()

X = df.copy()
# Ensure required feature columns exist
for c in feats:
    if c not in X.columns:
        X[c] = 0
X = X[feats]

score = pipe.predict_proba(X)[:, 1]
pred = (score >= 0.5).astype(int)

print("[LAB] n=", len(df), "pos_rate=", y.mean())
print("[LAB] PR-AUC=", round(average_precision_score(y, score), 6),
      "FPR@TPR=0.95=", round(float(fpr_at_tpr(y, score, 0.95)), 6))
print(classification_report(y, pred, digits=4))
print("Confusion:\n", confusion_matrix(y, pred))

out = pd.DataFrame({"y_true": y, "y_score": score, "y_pred": pred})
out.to_parquet(args.out_parquet, index=False)
print("[OK] wrote", args.out_parquet)
