import argparse, numpy as np, pandas as pd
from sklearn.model_selection import train_test_split

def pick_thr_for_fpr(y, s, fpr_max: float):
    """
    Pick the highest threshold t such that FPR(y, s>=t) <= fpr_max.
    If no threshold satisfies, return +inf (=> zero positives).
    """
    y = y.astype(int)
    s = s.astype(float)

    # thresholds: unique scores sorted descending + inf
    ths = np.unique(s)
    ths = np.sort(ths)[::-1]
    best = np.inf  # default: no positives
    for t in ths:
        pred = (s >= t).astype(int)
        # FPR = FP / Nneg
        neg = (y == 0)
        nneg = int(neg.sum())
        if nneg == 0:
            continue
        fp = int(((pred == 1) & neg).sum())
        fpr = fp / nneg
        if fpr <= fpr_max:
            best = t
            break
    return float(best)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scores", required=True, help="parquet with y_true,y_score")
    ap.add_argument("--out_prefix", required=True, help="output prefix path without suffix")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--test_size", type=float, default=0.30)
    ap.add_argument("--val_size", type=float, default=0.25)  # val fraction of train
    args = ap.parse_args()

    df = pd.read_parquet(args.scores)
    for c in ["y_true","y_score"]:
        if c not in df.columns:
            raise SystemExit(f"[ERR] missing column {c} in {args.scores}. cols={list(df.columns)}")

    y = df["y_true"].to_numpy().astype(int)
    s = df["y_score"].to_numpy().astype(float)

    # split train+val vs test
    idx = np.arange(len(df))
    idx_tr, idx_te = train_test_split(
        idx, test_size=args.test_size, random_state=args.seed, stratify=y
    )
    y_tr, s_tr = y[idx_tr], s[idx_tr]

    # split train vs val (within train)
    idx_tr2, idx_va = train_test_split(
        idx_tr, test_size=args.val_size, random_state=args.seed, stratify=y_tr
    )

    y_va, s_va = y[idx_va], s[idx_va]
    y_te, s_te = y[idx_te], s[idx_te]

    for name, fpr_max in [("fpr01", 0.01), ("fpr05", 0.05)]:
        thr = pick_thr_for_fpr(y_va, s_va, fpr_max)
        pred_te = (s_te >= thr).astype(int) if np.isfinite(thr) else np.zeros_like(y_te)

        out = pd.DataFrame({"y_true": y_te, "y_score": s_te, "y_pred": pred_te})
        out_path = f"{args.out_prefix}_budget_{name}_pred.parquet"
        out.to_parquet(out_path, index=False)

        # quick stats
        neg = (y_te == 0); nneg = int(neg.sum())
        fp = int(((pred_te == 1) & neg).sum())
        fpr = fp / nneg if nneg else float("nan")

        pos = (y_te == 1); npos = int(pos.sum())
        tp = int(((pred_te == 1) & pos).sum())
        tpr = tp / npos if npos else float("nan")

        print(f"[OK] wrote {out_path}")
        print(f"  thr={thr}  test_FPR={fpr:.6f}  test_TPR={tpr:.6f}  n_test={len(y_te)} pos_rate={y_te.mean():.4f}")

if __name__ == "__main__":
    main()
