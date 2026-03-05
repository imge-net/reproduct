import argparse, numpy as np, pandas as pd
from sklearn.metrics import average_precision_score, roc_curve, accuracy_score

def fpr_at_tpr(y, s, target_tpr=0.95):
    fpr, tpr, _ = roc_curve(y, s)
    idx = np.where(tpr >= target_tpr)[0]
    return np.nan if len(idx)==0 else float(fpr[idx[0]])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pred", required=True)
    ap.add_argument("--B", type=int, default=2000)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    df = pd.read_parquet(args.pred)
    y = df["y_true"].to_numpy().astype(int)
    s = df["y_score"].to_numpy().astype(float)

    n = len(y)
    rng = np.random.default_rng(args.seed)
    pr, fpr95, acc = [], [], []
    for _ in range(args.B):
        idx = rng.integers(0, n, size=n)
        yy, ss = y[idx], s[idx]
        pr.append(average_precision_score(yy, ss))
        fpr95.append(fpr_at_tpr(yy, ss, 0.95))
        acc.append(accuracy_score(yy, (ss>=0.5).astype(int)))

    def ci(arr):
        a = np.array(arr, float)
        return float(a.mean()), float(np.quantile(a, 0.025)), float(np.quantile(a, 0.975))

    pr_m, pr_l, pr_u = ci(pr)
    f_m, f_l, f_u = ci(fpr95)
    a_m, a_l, a_u = ci(acc)

    print(f"[RESULT] Bootstrap (B={args.B})")
    print(f"  PR-AUC mean={pr_m:.6f}  CI95=[{pr_l:.6f}, {pr_u:.6f}]")
    print(f"  FPR@TPR=0.95 mean={f_m:.6f}  CI95=[{f_l:.6f}, {f_u:.6f}]")
    print(f"  ACC mean={a_m:.6f}  CI95=[{a_l:.6f}, {a_u:.6f}]")

if __name__ == "__main__":
    main()
