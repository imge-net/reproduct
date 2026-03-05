import os, joblib
import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import average_precision_score, roc_curve, accuracy_score

ROOT = os.path.expanduser("~/work/mycelium_ids")
TR = os.path.join(ROOT, "data/unsw/raw/UNSW_NB15_training-set.csv")
TE = os.path.join(ROOT, "data/unsw/raw/UNSW_NB15_testing-set.csv")

FEATS = ["dur","proto","state","spkts","dpkts","sbytes","dbytes"]

def fpr_at_tpr(y_true, y_score, target_tpr=0.95):
    fpr, tpr, _ = roc_curve(y_true, y_score)
    idx = np.where(tpr >= target_tpr)[0]
    return float("nan") if len(idx)==0 else float(fpr[idx[0]])

def prep(df: pd.DataFrame):
    # leakage-safe binary label
    y = (pd.to_numeric(df["label"], errors="coerce").fillna(0).astype(int) != 0).astype(int)
    X = df[[c for c in FEATS if c in df.columns]].copy()
    return X, y

def build_preprocess(X: pd.DataFrame):
    num = [c for c in X.columns if is_numeric_dtype(X[c])]
    cat = [c for c in X.columns if c not in num]
    pre = ColumnTransformer([
        ("num", Pipeline([("imp", SimpleImputer(strategy="median")),
                          ("sc", StandardScaler(with_mean=False))]), num),
        ("cat", Pipeline([("imp", SimpleImputer(strategy="most_frequent")),
                          ("oh", OneHotEncoder(handle_unknown="ignore"))]), cat),
    ], remainder="drop")
    return pre

def main():
    tr = pd.read_csv(TR)
    te = pd.read_csv(TE)

    # IMPORTANT: drop attack_cat for safety (even though we don't include it in FEATS)
    if "attack_cat" in tr.columns:
        tr = tr.drop(columns=["attack_cat"])
    if "attack_cat" in te.columns:
        te = te.drop(columns=["attack_cat"])

    Xtr, ytr = prep(tr)
    Xte, yte = prep(te)

    pre = build_preprocess(Xtr)
    clf = RandomForestClassifier(n_estimators=400, n_jobs=-1, random_state=7)
    pipe = Pipeline([("pre", pre), ("clf", clf)])
    pipe.fit(Xtr, ytr)

    score = pipe.predict_proba(Xte)[:, 1]
    pred = (score >= 0.5).astype(int)

    ap = average_precision_score(yte, score)
    acc = accuracy_score(yte, pred)
    fpr95 = fpr_at_tpr(yte, score, 0.95)

    out = os.path.join(ROOT, "runs", "unsw_argus_subset_rf.joblib")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    joblib.dump({"pipe": pipe, "features": list(Xtr.columns)}, out)

    print("[OK] wrote", out)
    print("[UNSW-subset] PR-AUC=", round(ap, 6), "ACC=", round(acc, 6), "FPR@TPR=0.95=", round(float(fpr95), 6))
    print("[UNSW-subset] features=", list(Xtr.columns))

if __name__ == "__main__":
    main()
