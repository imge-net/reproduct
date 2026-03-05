import os, argparse, numpy as np, pandas as pd
from sklearn.metrics import average_precision_score, roc_curve, accuracy_score
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer

import lightgbm as lgb

def fpr_at_tpr(y, s, target_tpr=0.95):
    fpr, tpr, _ = roc_curve(y, s)
    idx = np.where(tpr >= target_tpr)[0]
    return float("nan") if len(idx)==0 else float(fpr[idx[0]])

def choose_thr_by_fpr(yv, sv, budget):
    fpr, tpr, thr = roc_curve(yv, sv)
    ok = np.where(fpr <= budget)[0]
    if len(ok)==0: return float("inf")
    best = ok[np.argmax(tpr[ok])]
    return float(thr[best])

def load_day(path):
    df = pd.read_parquet(path)
    if "Label" in df.columns:
        y = (df["Label"].astype(str).str.upper().str.strip() != "BENIGN").astype(int).to_numpy()
        X = df.drop(columns=["Label"])
    else:
        y = df["label"].astype(int).to_numpy()
        X = df.drop(columns=["label"])
    return X, y

def build_pre(X):
    cat=[c for c in X.columns if X[c].dtype=="object"]
    num=[c for c in X.columns if c not in cat]
    pre=ColumnTransformer([
        ("num", Pipeline([("imp",SimpleImputer(strategy="median"))]), num),
        ("cat", Pipeline([("imp",SimpleImputer(strategy="most_frequent")),
                          ("oh",OneHotEncoder(handle_unknown="ignore"))]), cat)
    ], remainder="drop")
    return pre

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--proc", default=os.path.expanduser("~/work/mycelium_ids/data/cicids/processed"))
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--seed", type=int, default=7)
    args=ap.parse_args()

    proc=args.proc
    X1,y1=load_day(os.path.join(proc,"cicids_Tuesday.parquet"))
    X2,y2=load_day(os.path.join(proc,"cicids_Wednesday.parquet"))
    Xv,yv=load_day(os.path.join(proc,"cicids_Thursday.parquet"))
    Xte,yte=load_day(os.path.join(proc,"cicids_Friday.parquet"))

    Xtr=pd.concat([X1,X2], ignore_index=True)
    ytr=np.concatenate([y1,y2])

    pre=build_pre(Xtr)

    clf=lgb.LGBMClassifier(
        n_estimators=2000,
        learning_rate=0.03,
        num_leaves=127,
        max_depth=-1,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=1.0,
        min_child_samples=40,
        random_state=args.seed,
        n_jobs=-1
    )

    pipe=Pipeline([("pre",pre),("clf",clf)])
    pipe.fit(Xtr,ytr)

    sv=pipe.predict_proba(Xte)[:,1]
    pr=float(average_precision_score(yte,sv))
    acc=float(accuracy_score(yte,(sv>=0.5).astype(int)))
    f95=float(fpr_at_tpr(yte,sv,0.95))

    os.makedirs(args.outdir, exist_ok=True)
    outp=os.path.join(args.outdir,"cicids_lgbm_no_monday_pred.parquet")
    pd.DataFrame({"y_true":yte.astype(int),"y_score":sv.astype(float),"y_pred":(sv>=0.5).astype(int)}).to_parquet(outp,index=False)

    print(f"[LGBM no-Monday] n_test={len(yte)} pos_rate={float(yte.mean()):.6f}")
    print(f"[LGBM no-Monday] PR-AUC={pr:.6f} ACC={acc:.6f} FPR@TPR=0.95={f95:.6f}")
    print(f"[OK] wrote {outp}")

    # budget thresholds selected on VAL
    sv_val=pipe.predict_proba(Xv)[:,1]
    for budget in (0.01,0.05):
        thr=choose_thr_by_fpr(yv,sv_val,budget)
        yhat=(sv>=thr).astype(int) if np.isfinite(thr) else np.zeros_like(yte)
        neg=(yte==0)
        realized_fpr=float((yhat[neg]==1).mean()) if neg.any() else float("nan")
        tp=float(((yhat==1)&(yte==1)).sum())
        fp=float(((yhat==1)&(yte==0)).sum())
        fn=float(((yhat==0)&(yte==1)).sum())
        prec=tp/(tp+fp+1e-12); rec=tp/(tp+fn+1e-12)
        f1=2*prec*rec/(prec+rec+1e-12)
        print(f"[BUDGET {budget:.0%}] thr={thr} P={prec:.6f} R={rec:.6f} F1={f1:.6f} FPR={realized_fpr:.6f}")

if __name__=="__main__":
    main()
