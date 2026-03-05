import argparse, os, numpy as np, pandas as pd
from pandas.api.types import is_numeric_dtype
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.metrics import average_precision_score, roc_curve, accuracy_score
from sklearn.ensemble import RandomForestClassifier, HistGradientBoostingClassifier

try:
    from lightgbm import LGBMClassifier
    HAS_LGBM = True
except Exception:
    HAS_LGBM = False

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

def eval_one(name, clf, Xtr, ytr, Xte, yte, rowid_te, outdir):
    pre = build_preprocess(Xtr)
    pipe = Pipeline([("pre", pre), ("clf", clf)])
    pipe.fit(Xtr, ytr)
    s = pipe.predict_proba(Xte)[:,1]
    p = (s >= 0.5).astype(int)

    ap = float(average_precision_score(yte, s))
    acc = float(accuracy_score(yte, p))
    fpr95 = float(fpr_at_tpr(yte, s, 0.95))

    out = pd.DataFrame({"row_id": rowid_te, "y_true": yte, "y_score": s, "y_pred": p})
    out_p = os.path.join(outdir, f"lab_{name}_pred.parquet")
    out.to_parquet(out_p, index=False)

    return ap, acc, fpr95, out_p

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--test_size", type=float, default=0.2)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    df = pd.read_parquet(args.parquet).reset_index(drop=True)
    if "y_true" not in df.columns:
        raise SystemExit("[ERR] parquet must contain y_true")
    df["row_id"] = np.arange(len(df), dtype=np.int64)

    # feature selection
    keep = [c for c in PORTABLE if c in df.columns]
    X = df[keep].copy()
    y = df["y_true"].astype(int).to_numpy()
    rowid = df["row_id"].to_numpy()

    # normalize categorical
    for c in ["proto","state"]:
        if c in X.columns:
            X[c] = X[c].astype(str)

    Xtr, Xte, ytr, yte, rid_tr, rid_te = train_test_split(
        X, y, rowid, test_size=args.test_size, random_state=args.seed, stratify=y
    )

    rows=[]
    # RF
    rf = RandomForestClassifier(n_estimators=600, n_jobs=-1, random_state=args.seed)
    ap_rf, acc_rf, fpr95_rf, p_rf = eval_one("rf", rf, Xtr, ytr, Xte, yte, rid_te, args.outdir)
    rows.append(("RF", ap_rf, fpr95_rf, acc_rf, p_rf))

    # HistGBDT
    hgb = HistGradientBoostingClassifier(random_state=args.seed)
    ap_h, acc_h, fpr95_h, p_h = eval_one("hgbdt", hgb, Xtr, ytr, Xte, yte, rid_te, args.outdir)
    rows.append(("HistGBDT", ap_h, fpr95_h, acc_h, p_h))

    # LightGBM (optional)
    if HAS_LGBM:
        lgbm = LGBMClassifier(
            n_estimators=600, learning_rate=0.05, num_leaves=63,
            subsample=0.9, colsample_bytree=0.9, random_state=args.seed, n_jobs=-1
        )
        ap_l, acc_l, fpr95_l, p_l = eval_one("lgbm", lgbm, Xtr, ytr, Xte, yte, rid_te, args.outdir)
        rows.append(("LightGBM", ap_l, fpr95_l, acc_l, p_l))

    # write summary
    summ = os.path.join(args.outdir, "eval_rowid_summary.csv")
    pd.DataFrame(rows, columns=["model","pr_auc","fpr_at_tpr95","acc","pred_path"]).to_csv(summ, index=False)
    print("[OK] wrote", summ)
    for r in rows:
        print(f"{r[0]:9s} PR-AUC={r[1]:.6f} FPR@TPR=0.95={r[2]:.6f} ACC={r[3]:.6f}")

if __name__ == "__main__":
    main()
