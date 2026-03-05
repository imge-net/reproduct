import argparse, os, json
import numpy as np
import pandas as pd

from pandas.api.types import is_numeric_dtype
from sklearn.model_selection import train_test_split
from sklearn.metrics import average_precision_score, roc_curve, accuracy_score
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestClassifier, HistGradientBoostingClassifier

try:
    from lightgbm import LGBMClassifier
    _HAS_LGBM = True
except Exception:
    _HAS_LGBM = False

PORTABLE = ["dur","proto","state","spkts","dpkts","sbytes","dbytes"]
WITH_PORTS = PORTABLE + ["sport","dport"]

def fpr_at_tpr(y_true, y_score, tpr_target=0.95):
    fpr, tpr, thr = roc_curve(y_true, y_score)
    idx = np.where(tpr >= tpr_target)[0]
    return float("nan") if len(idx) == 0 else float(fpr[idx[0]])

def build_preprocess(X: pd.DataFrame):
    num = [c for c in X.columns if is_numeric_dtype(X[c])]
    cat = [c for c in X.columns if c not in num]
    pre = ColumnTransformer(
        transformers=[
            ("num", Pipeline([("imp", SimpleImputer(strategy="median"))]), num),
            ("cat", Pipeline([("imp", SimpleImputer(strategy="most_frequent")),
                              ("oh", OneHotEncoder(handle_unknown="ignore"))]), cat),
        ],
        remainder="drop",
        sparse_threshold=0.3
    )
    return pre

def eval_one(name, model, Xtr, ytr, Xte, yte, row_id_te, attack_type_te, out_path):
    pre = build_preprocess(Xtr)
    pipe = Pipeline([("pre", pre), ("clf", model)])
    pipe.fit(Xtr, ytr)

    proba = pipe.predict_proba(Xte)[:, 1] if hasattr(pipe, "predict_proba") else pipe.decision_function(Xte)
    # normalize if decision_function
    if proba.min() < 0 or proba.max() > 1:
        # min-max to [0,1] just for consistent ROC/PR; this is not calibration
        a, b = float(proba.min()), float(proba.max())
        proba = (proba - a) / (b - a + 1e-12)

    y_pred = (proba >= 0.5).astype(int)

    prauc = float(average_precision_score(yte, proba))
    fpr95 = float(fpr_at_tpr(yte, proba, 0.95))
    acc   = float(accuracy_score(yte, y_pred))

    out = pd.DataFrame({
        "row_id": row_id_te,
        "attack_type": attack_type_te,
        "y_true": yte,
        "y_score": proba,
        "y_pred": y_pred
    })
    out.to_parquet(out_path, index=False)

    print(f"\n== {name} ==")
    print(f"PR-AUC={prauc:.6f} ACC={acc:.6f} FPR@TPR=0.95={fpr95:.6f}")
    print("[OK] wrote", out_path)

    return {"model": name, "prauc": prauc, "fpr_at_tpr95": fpr95, "acc": acc, "pred_path": out_path}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--with_ports", action="store_true", help="use sport/dport too (diagnostic)")
    ap.add_argument("--test_size", type=float, default=0.2)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    df = pd.read_parquet(args.parquet)
    df.columns = [c.strip() for c in df.columns]

    if "y_true" not in df.columns:
        raise SystemExit("[ERR] parquet must include y_true")
    if "attack_type" not in df.columns:
        df["attack_type"] = "unknown"

    # stable id for alignment
    df = df.reset_index(drop=True)
    df["row_id"] = np.arange(len(df), dtype=np.int64)

    feats = WITH_PORTS if args.with_ports else PORTABLE
    missing = [c for c in feats if c not in df.columns]
    if missing:
        raise SystemExit(f"[ERR] missing features: {missing}. Have={list(df.columns)}")

    X = df[feats].copy()
    y = df["y_true"].astype(int).to_numpy()
    row_id = df["row_id"].to_numpy()
    atk = df["attack_type"].astype(str).to_numpy()

    # ensure categoricals as string
    for c in ["proto","state"]:
        if c in X.columns:
            X[c] = X[c].astype(str)

    Xtr, Xte, ytr, yte, rid_tr, rid_te, atk_tr, atk_te = train_test_split(
        X, y, row_id, atk,
        test_size=args.test_size,
        random_state=args.seed,
        stratify=y
    )

    print("[INFO] n_total=", len(df), "pos_rate=", float(y.mean()))
    print("[INFO] n_test=", len(yte), "pos_rate_test=", float(yte.mean()))
    print("[INFO] feats=", feats)

    results = []

    # RF
    rf = RandomForestClassifier(n_estimators=400, n_jobs=-1, random_state=args.seed, class_weight=None)
    results.append(eval_one("rf", rf, Xtr, ytr, Xte, yte, rid_te, atk_te,
                            os.path.join(args.outdir, "lab_rf_pred.parquet")))

    # HistGBDT
    hgbdt = HistGradientBoostingClassifier(random_state=args.seed, max_depth=None)
    # note: HGBDT supports predict_proba
    results.append(eval_one("hgbdt", hgbdt, Xtr, ytr, Xte, yte, rid_te, atk_te,
                            os.path.join(args.outdir, "lab_hgbdt_pred.parquet")))

    # LightGBM (if available)
    if _HAS_LGBM:
        lgbm = LGBMClassifier(
            n_estimators=400,
            learning_rate=0.05,
            num_leaves=63,
            subsample=0.9,
            colsample_bytree=0.9,
            random_state=args.seed,
            n_jobs=-1
        )
        results.append(eval_one("lgbm", lgbm, Xtr, ytr, Xte, yte, rid_te, atk_te,
                                os.path.join(args.outdir, "lab_lgbm_pred.parquet")))
    else:
        print("[WARN] lightgbm not installed; skipping lgbm.")

    with open(os.path.join(args.outdir, "eval_summary.json"), "w") as f:
        json.dump(results, f, indent=2)

    print("[OK] wrote", os.path.join(args.outdir, "eval_summary.json"))

if __name__ == "__main__":
    main()
