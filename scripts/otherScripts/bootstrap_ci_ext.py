import argparse, numpy as np, pandas as pd
from sklearn.metrics import precision_score, recall_score, f1_score, confusion_matrix

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pred", required=True, help="parquet with y_true,y_score,y_pred")
    ap.add_argument("--B", type=int, default=2000)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    df = pd.read_parquet(args.pred)
    y = df["y_true"].to_numpy().astype(int)
    p = df["y_pred"].to_numpy().astype(int)
    n = len(df)

    rng = np.random.default_rng(args.seed)

    precs=[]; recs=[]; f1s=[]; tprs=[]; fprs=[]
    for _ in range(args.B):
        idx = rng.integers(0, n, size=n)
        yb, pb = y[idx], p[idx]
        tn, fp, fn, tp = confusion_matrix(yb, pb, labels=[0,1]).ravel()
        precs.append(precision_score(yb, pb, zero_division=0))
        recs.append(recall_score(yb, pb))
        f1s.append(f1_score(yb, pb))
        tprs.append(tp / (tp + fn + 1e-12))
        fprs.append(fp / (fp + tn + 1e-12))

    def ci(arr):
        arr = np.asarray(arr, dtype=float)
        return float(np.mean(arr)), float(np.percentile(arr, 2.5)), float(np.percentile(arr, 97.5))

    mP, loP, hiP = ci(precs)
    mR, loR, hiR = ci(recs)
    mF, loF, hiF = ci(f1s)
    mTPR, loTPR, hiTPR = ci(tprs)
    mFPR, loFPR, hiFPR = ci(fprs)

    print(f"[RESULT] Bootstrap (B={args.B}) on thresholded predictions")
    print(f"  Precision mean={mP:.6f}  CI95=[{loP:.6f}, {hiP:.6f}]")
    print(f"  Recall    mean={mR:.6f}  CI95=[{loR:.6f}, {hiR:.6f}]")
    print(f"  F1        mean={mF:.6f}  CI95=[{loF:.6f}, {hiF:.6f}]")
    print(f"  TPR       mean={mTPR:.6f} CI95=[{loTPR:.6f}, {hiTPR:.6f}]")
    print(f"  FPR       mean={mFPR:.6f} CI95=[{loFPR:.6f}, {hiFPR:.6f}]")

if __name__ == "__main__":
    main()
