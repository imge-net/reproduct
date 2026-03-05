import argparse, os, glob
import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_curve, accuracy_score, precision_score, recall_score, f1_score

def fpr_at_tpr(y_true, y_score, target_tpr=0.95):
    fpr, tpr, _ = roc_curve(y_true, y_score)
    idx = np.where(tpr >= target_tpr)[0]
    return float("nan") if len(idx)==0 else float(fpr[idx[0]])

def load_day(proc_dir, day):
    base = os.path.join(proc_dir, f"cicids_{day}.parquet")
    parts = sorted(glob.glob(base.replace(".parquet", ".part_*.parquet")))
    dfs = []
    if os.path.exists(base):
        dfs.append(pd.read_parquet(base))
    for p in parts:
        dfs.append(pd.read_parquet(p))
    if not dfs:
        raise FileNotFoundError(f"Missing day parquet for {day} under {proc_dir}")
    return pd.concat(dfs, ignore_index=True)

def make_model(seed: int):
    try:
        from xgboost import XGBClassifier
        return ("xgboost",
                XGBClassifier(
                    n_estimators=800,
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
                    max_iter=500,
                    random_state=seed
                ))

def prep_xy(df):
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]

    if "Label" not in df.columns:
        raise SystemExit("[ERR] CICIDS df must contain 'Label' column")

    y = (df["Label"].astype(str).str.strip().str.upper() != "BENIGN").astype(int).to_numpy()

    # Drop obvious identifier/leakage columns if present
    drop_like = ["Flow ID", "Timestamp", "Src IP", "Dst IP", "Source IP", "Destination IP",
                 "src_ip", "dst_ip", "flow_id", "timestamp"]
    drop_cols = [c for c in df.columns if c in drop_like]
    X = df.drop(columns=["Label"] + drop_cols, errors="ignore")

    # Keep numeric + encode any remaining object columns with factorize
    for c in X.columns:
        if X[c].dtype == "object":
            X[c] = X[c].astype(str).fillna("NA")
            X[c], _ = pd.factorize(X[c], sort=True)
        X[c] = pd.to_numeric(X[c], errors="coerce")

    # Replace inf and impute median
    X = X.replace([np.inf, -np.inf], np.nan)
    for c in X.columns:
        med = np.nanmedian(X[c].to_numpy())
        if np.isnan(med):
            med = 0.0
        X[c] = X[c].fillna(med)

    return X.to_numpy(dtype=np.float32), y

def thr_for_fpr(y, s, target_fpr):
    # choose threshold on validation such that FPR<=target_fpr (maximize TPR under constraint)
    neg = s[y==0]
    if len(neg) == 0:
        return float("inf")
    # if even threshold=+inf yields FPR=0, that's fine; but we want a finite threshold if possible.
    # For constraint FPR <= alpha, allow at most floor(alpha * n_neg) false positives.
    nneg = len(neg)
    max_fp = int(np.floor(target_fpr * nneg))
    if max_fp <= 0:
        # to ensure FP=0, set threshold above max negative score
        return float(np.max(neg) + 1e-12)
    # sort negatives descending; allow max_fp false positives => threshold at (max_fp)-th highest negative
    neg_sorted = np.sort(neg)[::-1]
    thr = float(neg_sorted[max_fp-1])  # score >= thr => predicted positive
    return thr

def eval_block(name, y, s, thr):
    p = (s >= thr).astype(int)
    return {
        "name": name,
        "thr": float(thr),
        "acc": float(accuracy_score(y, p)),
        "p": float(precision_score(y, p, zero_division=0)),
        "r": float(recall_score(y, p, zero_division=0)),
        "f1": float(f1_score(y, p, zero_division=0)),
        "fpr95": float(fpr_at_tpr(y, s, 0.95)),
        "prauc": float(average_precision_score(y, s)),
        "fpr": float(((p==1) & (y==0)).sum() / max(1, (y==0).sum()))
    }, p

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--proc_dir", default=os.path.expanduser("~/work/mycelium_ids/data/cicids/processed"))
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    # no-Monday protocol: Train=Tue+Wed, Val=Thu, Test=Fri
    df_tr = pd.concat([load_day(args.proc_dir,"Tuesday"), load_day(args.proc_dir,"Wednesday")], ignore_index=True)
    df_va = load_day(args.proc_dir,"Thursday")
    df_te = load_day(args.proc_dir,"Friday")

    Xtr, ytr = prep_xy(df_tr)
    Xva, yva = prep_xy(df_va)
    Xte, yte = prep_xy(df_te)

    model_name, clf = make_model(args.seed)
    clf.fit(Xtr, ytr)

    if hasattr(clf, "predict_proba"):
        sva = clf.predict_proba(Xva)[:,1]
        ste = clf.predict_proba(Xte)[:,1]
    else:
        raw_va = clf.decision_function(Xva)
        raw_te = clf.decision_function(Xte)
        sva = 1/(1+np.exp(-raw_va))
        ste = 1/(1+np.exp(-raw_te))

    # thresholds chosen ONLY on validation
    thr_fpr01 = thr_for_fpr(yva, sva, 0.01)
    thr_fpr05 = thr_for_fpr(yva, sva, 0.05)
    thr_def = 0.5

    # Evaluate on test
    res_def, p_def = eval_block("default0.5", yte, ste, thr_def)
    res_01,  p_01  = eval_block("valFPR01",   yte, ste, thr_fpr01)
    res_05,  p_05  = eval_block("valFPR05",   yte, ste, thr_fpr05)

    # Save parquets for CI scripts
    def save_parquet(fname, y, s, p):
        pd.DataFrame({"y_true": y.astype(int), "y_score": s.astype(float), "y_pred": p.astype(int)}).to_parquet(
            os.path.join(args.outdir, fname), index=False
        )

    save_parquet("cicids_gbdt_no_monday_scores.parquet", yte, ste, p_def)
    save_parquet("cicids_gbdt_no_monday_fpr01.parquet",  yte, ste, p_01)
    save_parquet("cicids_gbdt_no_monday_fpr05.parquet",  yte, ste, p_05)

    # Print point summary
    print(f"[CICIDS-GBDT] model={model_name} n_test={len(yte)} pos_rate={yte.mean():.4f}")
    for r in [res_def, res_01, res_05]:
        print(f"\n== {r['name']} ==")
        print(" thr=", round(r["thr"],6),
              " PR-AUC=", round(r["prauc"],6),
              " FPR@TPR=0.95=", round(r["fpr95"],6),
              " ACC=", round(r["acc"],6),
              " P=", round(r["p"],6), " R=", round(r["r"],6), " F1=", round(r["f1"],6),
              " FPR=", round(r["fpr"],6))

    print("\n[OK] wrote:",
          os.path.join(args.outdir, "cicids_gbdt_no_monday_scores.parquet"),
          os.path.join(args.outdir, "cicids_gbdt_no_monday_fpr01.parquet"),
          os.path.join(args.outdir, "cicids_gbdt_no_monday_fpr05.parquet"))

if __name__ == "__main__":
    main()
