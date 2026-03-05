import os, argparse, numpy as np, pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import average_precision_score, roc_curve, accuracy_score
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestClassifier, HistGradientBoostingClassifier

try:
    import lightgbm as lgb
    HAS_LGB=True
except Exception:
    HAS_LGB=False

CAT_COLS = ["proto","state"]
NUM_COLS = ["dur","spkts","dpkts","sbytes","dbytes"]
PORT_COLS = ["sport","dport"]

def fpr_at_tpr(y, s, target_tpr=0.95):
    fpr, tpr, _ = roc_curve(y, s)
    idx = np.where(tpr >= target_tpr)[0]
    return float("nan") if len(idx)==0 else float(fpr[idx[0]])

def choose_thr_by_fpr(yv, sv, budget):
    fpr, tpr, thr = roc_curve(yv, sv)
    ok = np.where(fpr <= budget)[0]
    if len(ok)==0:
        return float("inf")
    best = ok[np.argmax(tpr[ok])]
    return float(thr[best])

def build_pre(num_cols):
    pre=ColumnTransformer([
        ("num", Pipeline([("imp",SimpleImputer(strategy="median"))]), num_cols),
        ("cat", Pipeline([("imp",SimpleImputer(strategy="most_frequent")),
                          ("oh",OneHotEncoder(handle_unknown="ignore"))]), CAT_COLS),
    ], remainder="drop")
    return pre

def eval_one(tag, model_name, pipe, Xtr, ytr, Xv, yv, Xte, yte, outdir):
    pipe.fit(Xtr, ytr)
    sv = pipe.predict_proba(Xv)[:,1]
    st = pipe.predict_proba(Xte)[:,1]
    pr = float(average_precision_score(yte, st))
    acc = float(accuracy_score(yte, (st>=0.5).astype(int)))
    f95 = float(fpr_at_tpr(yte, st, 0.95))
    outp = os.path.join(outdir, f"{tag}_{model_name}_pred.parquet")
    pd.DataFrame({"y_true":yte.astype(int),"y_score":st.astype(float),"y_pred":(st>=0.5).astype(int)}).to_parquet(outp, index=False)
    print(f"\n== {tag} / {model_name} ==")
    print(f"PR-AUC={pr:.6f} ACC={acc:.6f} FPR@TPR=0.95={f95:.6f}")
    print(f"[OK] wrote {outp}")
    for budget in (0.01, 0.05):
        thr = choose_thr_by_fpr(yv, sv, budget)
        yh = (st >= thr).astype(int) if np.isfinite(thr) else np.zeros_like(yte)
        neg = (yte==0)
        realized_fpr = float((yh[neg]==1).mean()) if neg.any() else float("nan")
        tp = float(((yh==1)&(yte==1)).sum())
        fp = float(((yh==1)&(yte==0)).sum())
        fn = float(((yh==0)&(yte==1)).sum())
        prec = tp/(tp+fp+1e-12)
        rec  = tp/(tp+fn+1e-12)
        f1   = 2*prec*rec/(prec+rec+1e-12)
        print(f"[BUDGET FPR<={budget:.0%}] thr={thr} P={prec:.4f} R={rec:.4f} F1={f1:.4f} FPR={realized_fpr:.4f}")

def coerce(df, num_cols):
    for c in num_cols:
        if c not in df.columns: df[c]=0
        df[c]=pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    for c in CAT_COLS:
        if c not in df.columns: df[c]="unk"
        df[c]=df[c].astype(str).fillna("unk")
    return df

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--parquet", default=os.path.expanduser("~/work/mycelium_ids/lab/flows/lab_dataset_big_safe.parquet"))
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--seed", type=int, default=7)
    args=ap.parse_args()

    df=pd.read_parquet(args.parquet)
    df.columns=[c.strip().lower() for c in df.columns]
    y=df["y_true"].astype(int).to_numpy()

    # two feature sets
    setups = {
        "portable": NUM_COLS,
        "ports": NUM_COLS + PORT_COLS
    }

    os.makedirs(args.outdir, exist_ok=True)

    for tag, num_cols in setups.items():
        dfx=coerce(df.copy(), num_cols + CAT_COLS)
        X=dfx[num_cols + CAT_COLS].copy()

        Xtr, Xtmp, ytr, ytmp = train_test_split(X, y, test_size=0.4, random_state=args.seed, stratify=y)
        Xv, Xte, yv, yte = train_test_split(Xtmp, ytmp, test_size=0.5, random_state=args.seed, stratify=ytmp)

        pre=build_pre(num_cols)

        rf = RandomForestClassifier(n_estimators=600, n_jobs=-1, random_state=args.seed)
        eval_one(tag, "rf", Pipeline([("pre",pre),("clf",rf)]), Xtr,ytr,Xv,yv,Xte,yte,args.outdir)

        hgb = HistGradientBoostingClassifier(max_depth=None, learning_rate=0.05, max_iter=500, random_state=args.seed)
        eval_one(tag, "hgbdt", Pipeline([("pre",pre),("clf",hgb)]), Xtr,ytr,Xv,yv,Xte,yte,args.outdir)

        if HAS_LGB:
            lgbm = lgb.LGBMClassifier(
                n_estimators=2000, learning_rate=0.03, num_leaves=127,
                subsample=0.8, colsample_bytree=0.8, reg_lambda=1.0,
                min_child_samples=20, random_state=args.seed, n_jobs=-1
            )
            eval_one(tag, "lgbm", Pipeline([("pre",pre),("clf",lgbm)]), Xtr,ytr,Xv,yv,Xte,yte,args.outdir)
        else:
            print("[WARN] lightgbm not installed; skipping LGBM")

if __name__=="__main__":
    main()
