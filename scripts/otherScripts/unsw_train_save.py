import os, argparse
import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype

from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import average_precision_score, roc_curve

ROOT = os.path.expanduser("~/work/mycelium_ids")
TR = os.path.join(ROOT, "data/unsw/raw/UNSW_NB15_training-set.csv")
TE = os.path.join(ROOT, "data/unsw/raw/UNSW_NB15_testing-set.csv")

DROP_ALWAYS = {"id","ID","srcip","dstip","stime","ltime"}

def build_preprocess(X: pd.DataFrame):
    num_cols = [c for c in X.columns if is_numeric_dtype(X[c])]
    cat_cols = [c for c in X.columns if c not in num_cols]
    num_pipe = Pipeline([("imp", SimpleImputer(strategy="median")),
                         ("sc", StandardScaler(with_mean=False))])
    cat_pipe = Pipeline([("imp", SimpleImputer(strategy="most_frequent")),
                         ("oh", OneHotEncoder(handle_unknown="ignore"))])
    return ColumnTransformer([("num", num_pipe, num_cols),
                              ("cat", cat_pipe, cat_cols)],
                             remainder="drop",
                             sparse_threshold=0.3)

def fpr_at_tpr(y_true, y_score, target_tpr=0.95):
    fpr, tpr, _ = roc_curve(y_true, y_score)
    idx = np.where(tpr >= target_tpr)[0]
    return float("nan") if len(idx)==0 else float(fpr[idx[0]])

def load_binary(df: pd.DataFrame):
    y = pd.to_numeric(df["label"], errors="coerce").fillna(0).astype(int)
    y = (y != 0).astype(int)
    drop = set(DROP_ALWAYS) | {"label", "attack_cat"}  # prevent leakage
    X = df.drop(columns=[c for c in drop if c in df.columns])
    return X, y

def build_model(name: str):
    if name == "logreg":
        return LogisticRegression(max_iter=1500, n_jobs=-1, solver="saga")
    if name == "rf":
        return RandomForestClassifier(n_estimators=400, n_jobs=-1, random_state=7)
    raise ValueError("unknown model")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", choices=["logreg","rf"], default="rf")
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    tr = pd.read_csv(TR)
    te = pd.read_csv(TE)

    Xtr, ytr = load_binary(tr)
    Xte, yte = load_binary(te)

    pre = build_preprocess(Xtr)
    clf = build_model(args.model)
    pipe = Pipeline([("pre", pre), ("clf", clf)])
    pipe.fit(Xtr, ytr)

    score = pipe.predict_proba(Xte)[:,1]
    pred  = (score >= 0.5).astype(int)

    out = pd.DataFrame({"y_true": yte, "y_score": score, "y_pred": pred})
    out_path = os.path.join(args.outdir, f"unsw_binary_{args.model}.parquet")
    out.to_parquet(out_path, index=False)

    ap_score = average_precision_score(yte, score)
    fpr95 = fpr_at_tpr(yte, score, 0.95)
    print("[OK] wrote:", out_path)
    print("[INFO] PR-AUC:", round(ap_score, 6), "FPR@TPR=0.95:", round(fpr95, 6))

if __name__ == "__main__":
    main()
