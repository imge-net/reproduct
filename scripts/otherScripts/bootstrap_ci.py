import argparse, numpy as np, pandas as pd
from sklearn.metrics import average_precision_score, roc_curve, accuracy_score

def fpr_at_tpr(y_true, y_score, target_tpr=0.95):
    fpr, tpr, _ = roc_curve(y_true, y_score)
    idx = np.where(tpr >= target_tpr)[0]
    return np.nan if len(idx)==0 else float(fpr[idx[0]])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pred", required=True, help="parquet with y_true,y_score,y_pred")
    ap.add_argument("--B", type=int, default=1000)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    df = pd.read_parquet(args.pred)
    y = df["y_true"].to_numpy()
    s = df["y_score"].to_numpy()
    p = df["y_pred"].to_numpy()
    n = len(df)

    rng = np.random.default_rng(args.seed)
    stats = {"ap": [], "fpr95": [], "acc": []}

    for _ in range(args.B):
        idx = rng.integers(0, n, size=n)
        yb, sb, pb = y[idx], s[idx], p[idx]
        stats["ap"].append(average_precision_score(yb, sb))
        stats["fpr95"].append(fpr_at_tpr(yb, sb, 0.95))
        stats["acc"].append(accuracy_score(yb, pb))

    def ci(arr):
        arr = np.array(arr, dtype=float)
        return float(np.nanmean(arr)), float(np.nanpercentile(arr, 2.5)), float(np.nanpercentile(arr, 97.5))

    m_ap, lo_ap, hi_ap = ci(stats["ap"])
    m_f, lo_f, hi_f = ci(stats["fpr95"])
    m_a, lo_a, hi_a = ci(stats["acc"])

    print("[RESULT] Bootstrap (B=%d)" % args.B)
    print("  PR-AUC mean=%.6f  CI95=[%.6f, %.6f]" % (m_ap, lo_ap, hi_ap))
    print("  FPR@TPR=0.95 mean=%.6f  CI95=[%.6f, %.6f]" % (m_f, lo_f, hi_f))
    print("  ACC mean=%.6f  CI95=[%.6f, %.6f]" % (m_a, lo_a, hi_a))

if __name__ == "__main__":
    main()
