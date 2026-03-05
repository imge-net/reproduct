import os, glob, argparse
import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype

from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.metrics import average_precision_score, roc_curve, classification_report, confusion_matrix
from sklearn.ensemble import RandomForestClassifier

ROOT = os.path.expanduser("~/work/mycelium_ids")

def load_all(proc_dir):
    dfs=[]
    for day in ["Monday","Tuesday","Wednesday","Thursday","Friday"]:
        base = os.path.join(proc_dir, f"cicids_{day}.parquet")
        parts = sorted(glob.glob(base.replace(".parquet", ".part_*.parquet")))
        if os.path.exists(base): dfs.append(pd.read_parquet(base))
        for p in parts: dfs.append(pd.read_parquet(p))
    df = pd.concat(dfs, ignore_index=True)
    df["Label"] = df["Label"].astype(str).str.strip()
    return df

def fpr_at_tpr(y_true, y_score, target_tpr=0.95):
    fpr, tpr, _ = roc_curve(y_true, y_score)
    idx = np.where(tpr >= target_tpr)[0]
    return np.nan if len(idx)==0 else float(fpr[idx[0]])

def build_preprocess(X):
    num_cols = [c for c in X.columns if is_numeric_dtype(X[c])]
    X = X[num_cols]
    pre = ColumnTransformer([("num", Pipeline([
        ("imp", SimpleImputer(strategy="median")),
        ("sc", StandardScaler(with_mean=False))
    ]), num_cols)], remainder="drop")
    return pre, num_cols

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--proc_dir", default=os.path.join(ROOT, "data/cicids/processed"))
    ap.add_argument("--sample_n", type=int, default=0)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    df = load_all(args.proc_dir)
    if args.sample_n and args.sample_n < len(df):
        df = df.sample(n=args.sample_n, random_state=args.seed)

    y = (df["Label"].str.upper() != "BENIGN").astype(int)
    X = df.drop(columns=["Label"])

    pre, cols = build_preprocess(X)
    X = X[cols]

    Xtr, Xte, ytr, yte = train_test_split(
        X, y, test_size=0.2, random_state=args.seed, stratify=y
    )

    clf = RandomForestClassifier(n_estimators=400, n_jobs=-1, random_state=args.seed)
    pipe = Pipeline([("pre", pre), ("clf", clf)])
    pipe.fit(Xtr, ytr)

    score = pipe.predict_proba(Xte)[:,1]
    pred = (score >= 0.5).astype(int)

    apv = average_precision_score(yte, score)
    fpr95 = fpr_at_tpr(yte, score, 0.95)

    print("[INFO] Random split 80/20")
    print("[INFO] PR-AUC=", round(apv,6), "FPR@TPR=0.95=", round(float(fpr95),6))
    print(classification_report(yte, pred, digits=4))
    print("Confusion:\n", confusion_matrix(yte, pred))

    out = pd.DataFrame({"y_true": yte.to_numpy(), "y_score": score, "y_pred": pred})
    out.to_parquet(os.path.join(args.outdir, "cicids_random_rf.parquet"), index=False)
    print("[OK] wrote cicids_random_rf.parquet")

if __name__ == "__main__":
    main()
