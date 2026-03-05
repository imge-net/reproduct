import os, glob, argparse
import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import average_precision_score, roc_curve, precision_recall_curve, f1_score, confusion_matrix

ROOT = os.path.expanduser("~/work/mycelium_ids")

def load_day(proc_dir: str, day: str) -> pd.DataFrame:
    base = os.path.join(proc_dir, f"cicids_{day}.parquet")
    parts = sorted(glob.glob(base.replace(".parquet", ".part_*.parquet")))
    dfs = []
    if os.path.exists(base):
        dfs.append(pd.read_parquet(base))
    for p in parts:
        dfs.append(pd.read_parquet(p))
    if not dfs:
        raise FileNotFoundError(day)
    return pd.concat(dfs, ignore_index=True)

def build_preprocess(X: pd.DataFrame):
    num_cols = [c for c in X.columns if is_numeric_dtype(X[c])]
    X = X[num_cols]  # drop non-numeric defensively
    pre = ColumnTransformer(
        [("num", Pipeline([
            ("imp", SimpleImputer(strategy="median")),
            ("sc", StandardScaler(with_mean=False)),
        ]), num_cols)],
        remainder="drop"
    )
    return pre, num_cols

def bin_y(label_series: pd.Series):
    lab = label_series.astype(str).str.strip().str.upper()
    return (lab != "BENIGN").astype(int).to_numpy()

def pick_threshold(y, score, mode="f1", target=0.95):
    if mode == "f1":
        prec, rec, thr = precision_recall_curve(y, score)
        # thr has len-1 compared to prec/rec
        f1 = 2*prec[:-1]*rec[:-1] / (prec[:-1]+rec[:-1] + 1e-12)
        i = int(np.nanargmax(f1))
        return float(thr[i]), float(f1[i])
    if mode == "tpr":
        fpr, tpr, thr = roc_curve(y, score)
        idx = np.where(tpr >= target)[0]
        if len(idx)==0:  # can't reach target tpr
            return 0.0, np.nan
        return float(thr[idx[0]]), float(tpr[idx[0]])
    if mode == "fpr":
        fpr, tpr, thr = roc_curve(y, score)
        idx = np.where(fpr <= target)[0]
        if len(idx)==0:
            return 1.0, np.nan
        # take highest tpr under fpr constraint
        j = idx[np.argmax(tpr[idx])]
        return float(thr[j]), float(fpr[j])
    raise ValueError(mode)

def fpr_at_tpr(y_true, y_score, target_tpr=0.95):
    fpr, tpr, thr = roc_curve(y_true, y_score)
    idx = np.where(tpr >= target_tpr)[0]
    return np.nan if len(idx)==0 else float(fpr[idx[0]])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--proc_dir", default=os.path.join(ROOT, "data/cicids/processed"))
    ap.add_argument("--train_days", nargs="+", default=["Monday","Tuesday","Wednesday"])
    ap.add_argument("--val_days", nargs="+", default=["Thursday"])
    ap.add_argument("--test_days", nargs="+", default=["Friday"])
    ap.add_argument("--model", choices=["rf","logreg"], default="rf")
    ap.add_argument("--threshold_mode", choices=["f1","tpr","fpr"], default="f1")
    ap.add_argument("--target", type=float, default=0.95, help="target for tpr or fpr modes")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    tr = pd.concat([load_day(args.proc_dir, d) for d in args.train_days], ignore_index=True)
    va = pd.concat([load_day(args.proc_dir, d) for d in args.val_days], ignore_index=True)
    te = pd.concat([load_day(args.proc_dir, d) for d in args.test_days], ignore_index=True)

    ytr = bin_y(tr["Label"]); yva = bin_y(va["Label"]); yte = bin_y(te["Label"])
    Xtr = tr.drop(columns=["Label"]); Xva = va.drop(columns=["Label"]); Xte = te.drop(columns=["Label"])

    pre, cols = build_preprocess(Xtr)
    Xtr = Xtr[cols]; Xva = Xva[cols]; Xte = Xte[cols]

    if args.model == "rf":
        clf = RandomForestClassifier(n_estimators=400, n_jobs=-1, random_state=args.seed)
    else:
        clf = LogisticRegression(max_iter=2500, n_jobs=-1, solver="saga", class_weight="balanced")

    pipe = Pipeline([("pre", pre), ("clf", clf)])
    pipe.fit(Xtr, ytr)

    s_va = pipe.predict_proba(Xva)[:,1]
    s_te = pipe.predict_proba(Xte)[:,1]

    thr, thr_stat = pick_threshold(yva, s_va, mode=args.threshold_mode, target=args.target)
    p_te = (s_te >= thr).astype(int)

    ap_test = average_precision_score(yte, s_te)
    fpr95 = fpr_at_tpr(yte, s_te, 0.95)
    f1 = f1_score(yte, p_te)

    print("[INFO] threshold_mode=", args.threshold_mode, "thr=", thr, "val_stat=", thr_stat)
    print("[INFO] Test PR-AUC=", round(ap_test,6), "FPR@TPR=0.95=", round(float(fpr95),6), "F1@thr=", round(float(f1),6))
    print("[INFO] Confusion(test):\n", confusion_matrix(yte, p_te))

    out = pd.DataFrame({"y_true": yte, "y_score": s_te, "y_pred": p_te})
    out_path = os.path.join(args.outdir, f"cicids_binary_{args.model}_tuned.parquet")
    out.to_parquet(out_path, index=False)
    print("[OK] wrote:", out_path)

if __name__ == "__main__":
    main()
