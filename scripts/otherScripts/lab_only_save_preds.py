import argparse, pandas as pd, numpy as np
from pandas.api.types import is_numeric_dtype
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import average_precision_score, roc_curve

FEATS_PORTABLE = ["dur","proto","state","spkts","dpkts","sbytes","dbytes"]
FEATS_WITH_PORTS = FEATS_PORTABLE + ["sport","dport"]

def fpr_at_tpr(y_true, y_score, target_tpr=0.95):
    fpr, tpr, _ = roc_curve(y_true, y_score)
    idx = np.where(tpr >= target_tpr)[0]
    return float("nan") if len(idx)==0 else float(fpr[idx[0]])

def build_preprocess(X):
    num=[c for c in X.columns if is_numeric_dtype(X[c])]
    cat=[c for c in X.columns if c not in num]
    pre=ColumnTransformer([
        ("num", Pipeline([("imp",SimpleImputer(strategy="median")),
                          ("sc",StandardScaler(with_mean=False))]), num),
        ("cat", Pipeline([("imp",SimpleImputer(strategy="most_frequent")),
                          ("oh",OneHotEncoder(handle_unknown="ignore"))]), cat),
    ], remainder="drop")
    return pre

ap = argparse.ArgumentParser()
ap.add_argument("--csv", required=True)
ap.add_argument("--with_ports", action="store_true")
ap.add_argument("--out_parquet", required=True)
ap.add_argument("--seed", type=int, default=7)
args = ap.parse_args()

df=pd.read_csv(args.csv)
df.columns=[c.strip() for c in df.columns]
y=df["y_true"].astype(int).to_numpy()

feats = FEATS_WITH_PORTS if args.with_ports else FEATS_PORTABLE
X=df[[c for c in feats if c in df.columns]].copy()

for c in ["proto","state"]:
    if c in X.columns:
        X[c]=X[c].astype(str)

pre=build_preprocess(X)
clf=RandomForestClassifier(n_estimators=400, n_jobs=-1, random_state=args.seed)
pipe=Pipeline([("pre",pre),("clf",clf)])

Xtr,Xte,ytr,yte=train_test_split(X,y,test_size=0.3,random_state=args.seed,stratify=y)
pipe.fit(Xtr,ytr)

s=pipe.predict_proba(Xte)[:,1]
p=(s>=0.5).astype(int)

print("[LAB-only]", "with_ports" if args.with_ports else "portable_only")
print("n_test=", len(yte), "PR-AUC=", round(average_precision_score(yte,s),6),
      "FPR@TPR=0.95=", round(float(fpr_at_tpr(yte,s,0.95)),6))

out=pd.DataFrame({"y_true": yte, "y_score": s, "y_pred": p})
out.to_parquet(args.out_parquet, index=False)
print("[OK] wrote", args.out_parquet)
