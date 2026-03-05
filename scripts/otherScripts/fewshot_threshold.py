import argparse, numpy as np, pandas as pd
from sklearn.metrics import f1_score, precision_score, recall_score

ap = argparse.ArgumentParser()
ap.add_argument("--pred", required=True)   # parquet y_true,y_score
ap.add_argument("--k", type=int, default=500)  # calibration size
ap.add_argument("--reps", type=int, default=50)
ap.add_argument("--seed", type=int, default=7)
args = ap.parse_args()

df = pd.read_parquet(args.pred)
y = df["y_true"].to_numpy().astype(int)
s = df["y_score"].to_numpy().astype(float)
n = len(df)

rng = np.random.default_rng(args.seed)
f1s = []; ps=[]; rs=[]

for r in range(args.reps):
    idx = rng.permutation(n)
    cal = idx[:args.k]
    tst = idx[args.k:]

    # choose threshold on cal by maximizing F1
    ths = np.quantile(s[cal], np.linspace(0,1,1001))
    best = (-1, None)
    for t in ths:
        pcal = (s[cal] >= t).astype(int)
        f1 = f1_score(y[cal], pcal)
        if f1 > best[0]:
            best = (f1, t)

    t = best[1]
    pt = (s[tst] >= t).astype(int)
    f1s.append(f1_score(y[tst], pt))
    ps.append(precision_score(y[tst], pt, zero_division=0))
    rs.append(recall_score(y[tst], pt))

print("[FewShot] k=", args.k, "reps=", args.reps)
print(" F1 mean=", float(np.mean(f1s)), "std=", float(np.std(f1s)))
print(" P  mean=", float(np.mean(ps)),  "std=", float(np.std(ps)))
print(" R  mean=", float(np.mean(rs)),  "std=", float(np.std(rs)))
