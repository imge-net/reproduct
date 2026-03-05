import os
import argparse
import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype

from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report, confusion_matrix, average_precision_score, roc_curve
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier

ROOT = os.path.expanduser("~/work/mycelium_ids")
TR = os.path.join(ROOT, "data/unsw/raw/UNSW_NB15_training-set.csv")
TE = os.path.join(ROOT, "data/unsw/raw/UNSW_NB15_testing-set.csv")

DROP_ALWAYS = {"id", "ID", "srcip", "dstip", "stime", "ltime"}  # if present

def build_preprocess(X: pd.DataFrame):
    num_cols = [c for c in X.columns if is_numeric_dtype(X[c])]
    cat_cols = [c for c in X.columns if c not in num_cols]

    num_pipe = Pipeline([
        ("imp", SimpleImputer(strategy="median")),
        ("sc", StandardScaler(with_mean=False)),
    ])
    cat_pipe = Pipeline([
        ("imp", SimpleImputer(strategy="most_frequent")),
        ("oh", OneHotEncoder(handle_unknown="ignore")),
    ])

    return ColumnTransformer(
        [("num", num_pipe, num_cols), ("cat", cat_pipe, cat_cols)],
        remainder="drop",
        sparse_threshold=0.3,
    )

def fpr_at_tpr(y_true, y_score, target_tpr=0.95):
    fpr, tpr, _ = roc_curve(y_true, y_score)
    idx = np.where(tpr >= target_tpr)[0]
    return float("nan") if len(idx) == 0 else float(fpr[idx[0]])

def load_task(df: pd.DataFrame, task: str):
    # task: binary (label) or multiclass (attack_cat)
    if task == "binary":
        if "label" not in df.columns:
            raise ValueError("binary task requires 'label' column")
        y = df["label"]
        drop = set(DROP_ALWAYS) | {"label", "attack_cat"}  # IMPORTANT: drop attack_cat to prevent leakage
        X = df.drop(columns=[c for c in drop if c in df.columns])
        y = pd.to_numeric(y, errors="coerce").fillna(0).astype(int)
        y = (y != 0).astype(int)
        return X, y

    if task == "multiclass":
        if "attack_cat" not in df.columns:
            raise ValueError("multiclass task requires 'attack_cat' column")
        y = df["attack_cat"].astype(str)
        drop = set(DROP_ALWAYS) | {"attack_cat", "label"}  # drop label to avoid leakage
        X = df.drop(columns=[c for c in drop if c in df.columns])
        return X, y

    raise ValueError(f"unknown task: {task}")

def run_binary_models(Xtr, ytr, Xte, yte):
    pre = build_preprocess(Xtr)

    models = [
        ("LogReg(saga)", LogisticRegression(max_iter=1500, n_jobs=-1, solver="saga")),
        ("RF", RandomForestClassifier(n_estimators=400, n_jobs=-1, random_state=7)),
    ]

    for name, clf in models:
        pipe = Pipeline([("pre", pre), ("clf", clf)])
        pipe.fit(Xtr, ytr)
        proba = pipe.predict_proba(Xte)[:, 1]
        pred = (proba >= 0.5).astype(int)

        ap = average_precision_score(yte, proba)
        fpr95 = fpr_at_tpr(yte, proba, 0.95)

        print("\n====", name, "====")
        print(classification_report(yte, pred, digits=4))
        print("Confusion:\n", confusion_matrix(yte, pred))
        print("PR-AUC(AP):", round(ap, 6))
        print("FPR@TPR=0.95:", round(fpr95, 6))

def run_multiclass_models(Xtr, ytr, Xte, yte):
    pre = build_preprocess(Xtr)

    models = [
        ("LogReg(saga)", LogisticRegression(max_iter=2000, n_jobs=-1, solver="saga")),
        ("RF", RandomForestClassifier(n_estimators=400, n_jobs=-1, random_state=7)),
    ]

    for name, clf in models:
        pipe = Pipeline([("pre", pre), ("clf", clf)])
        pipe.fit(Xtr, ytr)
        pred = pipe.predict(Xte)

        print("\n====", name, "====")
        print(classification_report(yte, pred, digits=4))
        print("Confusion:\n", confusion_matrix(yte, pred))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--task", choices=["binary", "multiclass"], default="binary")
    args = ap.parse_args()

    if not (os.path.exists(TR) and os.path.exists(TE)):
        raise SystemExit(f"Missing UNSW CSVs at:\n  {TR}\n  {TE}")

    tr = pd.read_csv(TR)
    te = pd.read_csv(TE)

    # quick leakage sanity check
    if "attack_cat" in tr.columns and "label" in tr.columns:
        tab = pd.crosstab(tr["attack_cat"].astype(str), tr["label"])
        print("[INFO] Crosstab attack_cat vs label (train):")
        print(tab.head(20))

    Xtr, ytr = load_task(tr, args.task)
    Xte, yte = load_task(te, args.task)

    print(f"[INFO] Task={args.task}  Train={len(Xtr)}  Test={len(Xte)}  Features={Xtr.shape[1]}")
    print("[INFO] Example columns:", list(Xtr.columns[:12]))

    if args.task == "binary":
        run_binary_models(Xtr, ytr, Xte, yte)
    else:
        run_multiclass_models(Xtr, ytr, Xte, yte)

if __name__ == "__main__":
    main()
