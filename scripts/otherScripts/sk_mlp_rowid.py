import argparse, os, numpy as np, pandas as pd
from pandas.api.types import is_numeric_dtype
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.metrics import average_precision_score, roc_curve, accuracy_score
from sklearn.neural_network import MLPClassifier

PORTABLE = ["dur","proto","state","spkts","dpkts","sbytes","dbytes"]

def fpr_at_tpr(y, s, tpr_target=0.95):
    fpr, tpr, _ = roc_curve(y, s)
    idx = np.where(tpr >= tpr_target)[0]
    return float("nan") if len(idx)==0 else float(fpr[idx[0]])

def build_preprocess(X):
    num=[c for c in X.columns if is_numeric_dtype(X[c])]
    cat=[c for c in X.columns if c not in num]
    return ColumnTransformer([
        ("num", Pipeline([("imp",SimpleImputer(strategy="median"))]), num),
        ("cat", Pipeline([("imp",SimpleImputer(strategy="most_frequent")),
                          ("oh",OneHotEncoder(handle_unknown="ignore"))]), cat),
    ], remainder="drop")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--feature_set", default="portable", choices=["portable"])
    ap.add_argument("--test_size", type=float, default=0.2)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    df = pd.read_parquet(args.parquet).reset_index(drop=True)

    if "y_true" not in df.columns:
        raise SystemExit("[ERR] parquet must contain y_true")

    df["row_id"] = np.arange(len(df), dtype=np.int64)

    X = df[PORTABLE].copy()
    y = df["y_true"].astype(int).to_numpy()
    row_id = df["row_id"].to_numpy()

    Xtr, Xte, ytr, yte, rid_tr, rid_te = train_test_split(
        X, y, row_id, test_size=args.test_size, random_state=args.seed, stratify=y
    )

    pre = build_preprocess(pd.DataFrame(Xtr, columns=X.columns))
    clf = MLPClassifier(
        hidden_layer_sizes=(256,128),
        activation="relu",
        solver="adam",
        alpha=1e-4,
        batch_size=512,
        learning_rate_init=1e-3,
        max_iter=50,
        random_state=args.seed,
        early_stopping=True,
        n_iter_no_change=5,
        verbose=False
    )

    pipe = Pipeline([("pre", pre), ("clf", clf)])
    pipe.fit(pd.DataFrame(Xtr, columns=X.columns), ytr)

    s = pipe.predict_proba(pd.DataFrame(Xte, columns=X.columns))[:,1]
    p = (s >= 0.5).astype(int)

    apv = float(average_precision_score(yte, s))
    acc = float(accuracy_score(yte, p))
    fpr95 = float(fpr_at_tpr(yte, s, 0.95))

    out = pd.DataFrame({"row_id": rid_te, "y_true": yte, "y_score": s, "y_pred": p})
    out_p = os.path.join(args.outdir, "sk_mlp_portable_pred.parquet")
    out.to_parquet(out_p, index=False)

    print(f"[SK-MLP] PR-AUC={apv:.6f} FPR@TPR=0.95={fpr95:.6f} ACC={acc:.6f}")
    print("[OK] wrote", out_p)

if __name__ == "__main__":
    main()
