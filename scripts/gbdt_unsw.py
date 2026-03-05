import argparse, os
import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_curve, accuracy_score

def fpr_at_tpr(y_true, y_score, target_tpr=0.95):
    fpr, tpr, _ = roc_curve(y_true, y_score)
    idx = np.where(tpr >= target_tpr)[0]
    return float("nan") if len(idx)==0 else float(fpr[idx[0]])

def encode_categoricals(train_df, test_df, cat_cols):
    # Build mapping on concat to keep consistent codes
    for c in cat_cols:
        tr = train_df[c].astype(str).fillna("NA")
        te = test_df[c].astype(str).fillna("NA")
        allv = pd.concat([tr, te], ignore_index=True)
        uniq = pd.Index(allv.unique())
        mapping = {k:i for i,k in enumerate(uniq)}
        train_df[c] = tr.map(mapping).fillna(-1).astype(np.int32)
        test_df[c]  = te.map(mapping).fillna(-1).astype(np.int32)
    return train_df, test_df

def make_model(seed: int):
    # Prefer XGBoost if available, else fallback to sklearn HGBDT
    try:
        from xgboost import XGBClassifier
        return ("xgboost",
                XGBClassifier(
                    n_estimators=600,
                    max_depth=6,
                    learning_rate=0.05,
                    subsample=0.8,
                    colsample_bytree=0.8,
                    reg_lambda=1.0,
                    objective="binary:logistic",
                    eval_metric="aucpr",
                    tree_method="hist",
                    n_jobs=-1,
                    random_state=seed,
                ))
    except Exception:
        from sklearn.ensemble import HistGradientBoostingClassifier
        return ("hgbdt",
                HistGradientBoostingClassifier(
                    max_depth=6,
                    learning_rate=0.05,
                    max_iter=400,
                    random_state=seed
                ))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--train_csv", default=os.path.expanduser("~/work/mycelium_ids/data/unsw/raw/UNSW_NB15_training-set.csv"))
    ap.add_argument("--test_csv",  default=os.path.expanduser("~/work/mycelium_ids/data/unsw/raw/UNSW_NB15_testing-set.csv"))
    ap.add_argument("--out_parquet", required=True)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    tr = pd.read_csv(args.train_csv)
    te = pd.read_csv(args.test_csv)

    # Target
    y_tr = tr["label"].astype(int).to_numpy()
    y_te = te["label"].astype(int).to_numpy()

    # Drop leakage + identifiers + target columns
    drop_cols = [c for c in ["label", "attack_cat", "id"] if c in tr.columns]
    X_tr = tr.drop(columns=drop_cols, errors="ignore").copy()
    X_te = te.drop(columns=drop_cols, errors="ignore").copy()

    # Identify categorical columns
    cat_cols = [c for c in X_tr.columns if X_tr[c].dtype == "object"]
    X_tr, X_te = encode_categoricals(X_tr, X_te, cat_cols)

    # Coerce everything to numeric, impute median
    for c in X_tr.columns:
        X_tr[c] = pd.to_numeric(X_tr[c], errors="coerce")
        X_te[c] = pd.to_numeric(X_te[c], errors="coerce")
        med = np.nanmedian(X_tr[c].to_numpy())
        if np.isnan(med):  # all-NaN column
            med = 0.0
        X_tr[c] = X_tr[c].fillna(med)
        X_te[c] = X_te[c].fillna(med)

    model_name, clf = make_model(args.seed)
    clf.fit(X_tr.to_numpy(dtype=np.float32), y_tr)

    # Scores
    if hasattr(clf, "predict_proba"):
        s = clf.predict_proba(X_te.to_numpy(dtype=np.float32))[:,1]
    else:
        # fallback: decision_function -> sigmoid-like scaling
        raw = clf.decision_function(X_te.to_numpy(dtype=np.float32))
        s = 1/(1+np.exp(-raw))

    p = (s >= 0.5).astype(int)

    pr = float(average_precision_score(y_te, s))
    fpr95 = float(fpr_at_tpr(y_te, s, 0.95))
    acc = float(accuracy_score(y_te, p))

    out = pd.DataFrame({"y_true": y_te.astype(int), "y_score": s.astype(float), "y_pred": p.astype(int)})
    os.makedirs(os.path.dirname(args.out_parquet), exist_ok=True)
    out.to_parquet(args.out_parquet, index=False)

    print(f"[UNSW-GBDT] model={model_name} n_test={len(y_te)}")
    print("PR-AUC=", round(pr,6), "FPR@TPR=0.95=", round(fpr95,6), "ACC=", round(acc,6))
    print("[OK] wrote", args.out_parquet)

if __name__ == "__main__":
    main()
