import os, argparse, numpy as np, pandas as pd
from sklearn.model_selection import train_test_split
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
                          ("oh",OneHotEncoder(handle_unknown="ignore"))]), cat),
    ], remainder="drop")
    return pre

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--proc", default=os.path.expanduser("~/work/mycelium_ids/data/cicids/processed"))
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--seed", type=int, default=7)
    args=ap.parse_args()

    proc=args.proc
    X2,y2=load_day(os.path.join(proc,"cicids_Tuesday.parquet"))
    X3,y3=load_day(os.path.join(proc,"cicids_Wednesday.parquet"))
    X4,y4=load_day(os.path.join(proc,"cicids_Thursday.parquet"))
    X5,y5=load_day(os.path.join(proc,"cicids_Friday.parquet"))

    X=pd.concat([X2,X3,X4,X5], ignore_index=True)
    y=np.concatenate([y2,y3,y4,y5])

    Xtr, Xtmp, ytr, ytmp = train_test_split(X, y, test_size=0.4, random_state=args.seed, stratify=y)
    Xv, Xte, yv, yte = train_test_split(Xtmp, ytmp, test_size=0.5, random_state=args.seed, stratify=ytmp)

    pre=build_pre(Xtr)
    clf=lgb.LGBMClassifier(
        n_estimators=2000,
        learning_rate=0.03,
        num_leaves=127,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=1.0,
        min_child_samples=40,
        random_state=args.seed,
        n_jobs=-1
    )
    pipe=Pipeline([("pre",pre),("clf",clf)])
    pipe.fit(Xtr,ytr)

    s=pipe.predict_proba(Xte)[:,1]
    pr=float(average_precision_score(yte,s))
    acc=float(accuracy_score(yte,(s>=0.5).astype(int)))
    f95=float(fpr_at_tpr(yte,s,0.95))

    os.makedirs(args.outdir, exist_ok=True)
    outp=os.path.join(args.outdir,"cicids_lgbm_random_pred.parquet")
    pd.DataFrame({"y_true":yte.astype(int),"y_score":s.astype(float),"y_pred":(s>=0.5).astype(int)}).to_parquet(outp,index=False)

    print(f"[LGBM random] n_test={len(yte)} pos_rate={float(yte.mean()):.6f}")
    print(f"[LGBM random] PR-AUC={pr:.6f} ACC={acc:.6f} FPR@TPR=0.95={f95:.6f}")
    print(f"[OK] wrote {outp}")

if __name__=="__main__":
    main()
