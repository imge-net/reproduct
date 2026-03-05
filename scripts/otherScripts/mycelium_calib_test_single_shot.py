import argparse, os
import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_curve, accuracy_score

def fpr_at_tpr(y_true, y_score, target_tpr=0.95):
    fpr, tpr, _ = roc_curve(y_true, y_score)
    idx = np.where(tpr >= target_tpr)[0]
    return float("nan") if len(idx)==0 else float(fpr[idx[0]])

def thr_for_fpr(y, s, target_fpr):
    neg = s[y==0]
    nneg = len(neg)
    if nneg == 0:
        return float("inf")
    max_fp = int(np.floor(target_fpr * nneg))
    if max_fp <= 0:
        return float(np.max(neg) + 1e-12)  # FP=0
    neg_sorted = np.sort(neg)[::-1]
    return float(neg_sorted[max_fp-1])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pred_parquet", default=os.path.expanduser("~/work/mycelium_ids/lab/flows/lab_preds.parquet"))
    ap.add_argument("--calib_frac", type=float, default=0.2)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    df = pd.read_parquet(args.pred_parquet)
    y = df["y_true"].to_numpy().astype(int)
    s = df["y_score"].to_numpy().astype(float)

    n = len(df)
    rng = np.random.default_rng(args.seed)
    idx = np.arange(n)
    rng.shuffle(idx)
    n_c = int(np.floor(args.calib_frac * n))
    ic = idx[:n_c]
    it = idx[n_c:]

    y_c, s_c = y[ic], s[ic]
    y_t, s_t = y[it], s[it]

    ap_s  = average_precision_score(y_c, s_c)
    ap_fs = average_precision_score(y_c, 1.0 - s_c)
    flip = ap_fs > ap_s
    s_t2 = (1.0 - s_t) if flip else s_t

    # thresholds chosen only on calibration
    thr_fpr01 = thr_for_fpr(y_c, (1.0 - s_c) if flip else s_c, 0.01)
    thr_fpr05 = thr_for_fpr(y_c, (1.0 - s_c) if flip else s_c, 0.05)

    # point metrics on test
    pr = float(average_precision_score(y_t, s_t2))
    fpr95 = float(fpr_at_tpr(y_t, s_t2, 0.95))
    p05 = (s_t2 >= thr_fpr05).astype(int)
    p01 = (s_t2 >= thr_fpr01).astype(int)
    acc = float(accuracy_score(y_t, (s_t2>=0.5).astype(int)))

    def tpr_fpr(y_true, y_pred):
        tp = ((y_pred==1)&(y_true==1)).sum()
        fn = ((y_pred==0)&(y_true==1)).sum()
        fp = ((y_pred==1)&(y_true==0)).sum()
        tn = ((y_pred==0)&(y_true==0)).sum()
        tpr = tp / max(1, (tp+fn))
        fpr = fp / max(1, (fp+tn))
        return float(tpr), float(fpr)

    tpr01, fpr01 = tpr_fpr(y_t, p01)
    tpr05, fpr05 = tpr_fpr(y_t, p05)

    # Save test predictions (single-shot)
    out = pd.DataFrame({"y_true": y_t, "y_score": s_t2, "y_pred": (s_t2>=0.5).astype(int)})
    out.to_parquet(os.path.join(args.outdir, "mycelium_single_shot_test.parquet"), index=False)

    # Also save budget-thresholded versions for bootstrap_ci_ext.py
    pd.DataFrame({"y_true": y_t, "y_score": s_t2, "y_pred": p01}).to_parquet(
        os.path.join(args.outdir, "mycelium_single_shot_fpr01.parquet"), index=False
    )
    pd.DataFrame({"y_true": y_t, "y_score": s_t2, "y_pred": p05}).to_parquet(
        os.path.join(args.outdir, "mycelium_single_shot_fpr05.parquet"), index=False
    )

    print("[MYCELIUM single-shot]")
    print(" n_total=", n, " n_calib=", len(ic), " n_test=", len(it),
          " pos_rate_total=", round(y.mean(),4), " pos_rate_test=", round(y_t.mean(),4))
    print(" orientation_selected=", "flip(1-s)" if flip else "orig(s)",
          " calib_PR-AUC(s)=", round(ap_s,6), " calib_PR-AUC(1-s)=", round(ap_fs,6))
    print(" TEST  PR-AUC=", round(pr,6), " FPR@TPR=0.95=", round(fpr95,6), " ACC=", round(acc,6))
    print(" TEST  TPR@FPR<=1%: TPR=", round(tpr01,6), " FPR=", round(fpr01,6),
          " (if no thr satisfies constraint, thr>max(neg) => zero alarms)")
    print(" TEST  TPR@FPR<=5%: TPR=", round(tpr05,6), " FPR=", round(fpr05,6))
    print("[OK] wrote", os.path.join(args.outdir, "mycelium_single_shot_test.parquet"))

if __name__ == "__main__":
    main()
