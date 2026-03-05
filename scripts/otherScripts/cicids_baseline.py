import os, glob, argparse
import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    classification_report, confusion_matrix, average_precision_score, roc_curve
)
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier

ROOT = os.path.expanduser("~/work/mycelium_ids")

def fpr_at_tpr(y_true, y_score, target_tpr=0.95):
    fpr, tpr, _ = roc_curve(y_true, y_score)
    idx = np.where(tpr >= target_tpr)[0]
    return float("nan") if len(idx)==0 else float(fpr[idx[0]])

def load_day_parquets(proc_dir: str, day: str) -> pd.DataFrame:
    base = os.path.join(proc_dir, f"cicids_{day}.parquet")
    parts = sorted(glob.glob(base.replace(".parquet", ".part_*.parquet")))
    dfs = []
    if os.path.exists(base):
        dfs.append(pd.read_parquet(base))
    for p in parts:
        dfs.append(pd.read_parquet(p))
    if not dfs:
        raise FileNotFoundError(f"No parquet found for day={day} under {proc_dir}")
    df = pd.concat(dfs, ignore_index=True)
    return df

def build_preprocess(X: pd.DataFrame):
    num_cols = [c for c in X.columns if is_numeric_dtype(X[c])]
    # CICIDS should be almost all numeric; anything non-numeric -> drop (safe)
    other_cols = [c for c in X.columns if c not in num_cols]
    if other_cols:
        # drop any unexpected non-numeric columns deterministically
        X = X.drop(columns=other_cols)
        num_cols = [c for c in X.columns if is_numeric_dtype(X[c])]

    pre = ColumnTransformer(
        [("num", Pipeline([
            ("imp", SimpleImputer(strategy="median")),
            ("sc", StandardScaler(with_mean=False)),
        ]), num_cols)],
        remainder="drop"
    )
    return pre, X.columns.tolist()

def sample_df(df: pd.DataFrame, n: int, seed: int):
    if n <= 0 or n >= len(df):
        return df
    return df.sample(n=n, random_state=seed)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--proc_dir", default=os.path.join(ROOT, "data/cicids/processed"))
    ap.add_argument("--train_days", nargs="+", default=["Monday","Tuesday","Wednesday","Thursday"])
    ap.add_argument("--test_days", nargs="+", default=["Friday"])
    ap.add_argument("--task", choices=["binary","multiclass"], default="binary")
    ap.add_argument("--model", choices=["logreg","rf"], default="rf")
    ap.add_argument("--train_n", type=int, default=0, help="optional subsample size for train (0=all)")
    ap.add_argument("--test_n", type=int, default=0, help="optional subsample size for test (0=all)")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    # Load days
    tr_list = [load_day_parquets(args.proc_dir, d) for d in args.train_days]
    te_list = [load_day_parquets(args.proc_dir, d) for d in args.test_days]
    tr = pd.concat(tr_list, ignore_index=True)
    te = pd.concat(te_list, ignore_index=True)

    # Optional subsampling for quick runs
    tr = sample_df(tr, args.train_n, args.seed)
    te = sample_df(te, args.test_n, args.seed)

    # Labels
    tr["Label"] = tr["Label"].astype(str).str.strip()
    te["Label"] = te["Label"].astype(str).str.strip()

    if args.task == "binary":
        ytr = (tr["Label"].str.upper() != "BENIGN").astype(int)
        yte = (te["Label"].str.upper() != "BENIGN").astype(int)
    else:
        ytr = tr["Label"]
        yte = te["Label"]

    # Features
    Xtr = tr.drop(columns=["Label"])
    Xte = te.drop(columns=["Label"])

    pre, kept_cols = build_preprocess(Xtr)
    Xtr = Xtr[kept_cols]
    Xte = Xte[kept_cols]

    if args.model == "logreg":
        clf = LogisticRegression(max_iter=2000, n_jobs=-1, solver="saga", class_weight="balanced" if args.task=="binary" else None)
    else:
        clf = RandomForestClassifier(n_estimators=400, n_jobs=-1, random_state=args.seed)

    pipe = Pipeline([("pre", pre), ("clf", clf)])
    pipe.fit(Xtr, ytr)

    if args.task == "binary":
        score = pipe.predict_proba(Xte)[:,1]
        pred = (score >= 0.5).astype(int)

        apv = average_precision_score(yte, score)
        fpr95 = fpr_at_tpr(yte, score, 0.95)

        print(f"[INFO] CICIDS binary day-split train={len(Xtr)} test={len(Xte)} feats={len(kept_cols)}")
        print(classification_report(yte, pred, digits=4))
        print("Confusion:\n", confusion_matrix(yte, pred))
        print("PR-AUC(AP):", round(apv, 6))
        print("FPR@TPR=0.95:", round(fpr95, 6))

        out = pd.DataFrame({"y_true": yte.to_numpy(), "y_score": score, "y_pred": pred})
        out_path = os.path.join(args.outdir, f"cicids_binary_{args.model}.parquet")
        out.to_parquet(out_path, index=False)
        print("[OK] wrote:", out_path)

    else:
        pred = pipe.predict(Xte)
        print(f"[INFO] CICIDS multiclass day-split train={len(Xtr)} test={len(Xte)} feats={len(kept_cols)}")
        print(classification_report(yte, pred, digits=4))
        print("Confusion:\n", confusion_matrix(yte, pred))

        out = pd.DataFrame({"y_true": yte.astype(str).to_numpy(), "y_pred": pred.astype(str)})
        out_path = os.path.join(args.outdir, f"cicids_multiclass_{args.model}.parquet")
        out.to_parquet(out_path, index=False)
        print("[OK] wrote:", out_path)

if __name__ == "__main__":
    main()
