#!/usr/bin/env bash
set -euo pipefail

ENV_NAME="${ENV_NAME:-ids_mycelium}"
ROOT="${ROOT:-$HOME/work/mycelium_ids}"
RUNS="$ROOT/runs"
TS="$(date +%Y%m%d_%H%M%S)"
OUT="$RUNS/$TS"
mkdir -p "$OUT"

echo "[INFO] outdir=$OUT"

# ---- helpers
run_py() {
  conda run -n "$ENV_NAME" python "$@"
}

need_file() {
  [[ -f "$1" ]] || { echo "[ERR] missing file: $1"; exit 1; }
}

# ---- paths
PROC="$ROOT/data/cicids/processed"
need_file "$PROC/cicids_Tuesday.parquet"
need_file "$PROC/cicids_Wednesday.parquet"
need_file "$PROC/cicids_Thursday.parquet"
need_file "$PROC/cicids_Friday.parquet"

# ---- write python: gbdt train/eval core + budgets + optional plots
cat > "$OUT/gbdt_cicids.py" <<'PY'
import argparse, os, numpy as np, pandas as pd
from sklearn.metrics import average_precision_score, roc_curve, accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.metrics import precision_recall_fscore_support

# Prefer sklearn HistGradientBoosting for zero extra deps
from sklearn.ensemble import HistGradientBoostingClassifier

def fpr_at_tpr(y, s, target_tpr=0.95):
    fpr, tpr, _ = roc_curve(y, s)
    idx = np.where(tpr >= target_tpr)[0]
    return float("nan") if len(idx)==0 else float(fpr[idx[0]])

def load_parquet(path):
    df = pd.read_parquet(path)
    # CICIDS label column naming can vary; normalize
    if "Label" in df.columns:
        y = (df["Label"].astype(str).str.upper().str.strip() != "BENIGN").astype(int).to_numpy()
        df = df.drop(columns=["Label"])
    elif "label" in df.columns:
        y = df["label"].astype(int).to_numpy()
        df = df.drop(columns=["label"])
    else:
        raise ValueError(f"no Label/label column in {path}")
    return df, y

def build_preprocess(X: pd.DataFrame):
    # categorical = object columns
    cat = [c for c in X.columns if X[c].dtype == "object"]
    num = [c for c in X.columns if c not in cat]
    pre = ColumnTransformer([
        ("num", Pipeline([("imp", SimpleImputer(strategy="median"))]), num),
        ("cat", Pipeline([("imp", SimpleImputer(strategy="most_frequent")),
                          ("oh", OneHotEncoder(handle_unknown="ignore"))]), cat),
    ], remainder="drop", sparse_threshold=0.3)
    return pre

def fit_gbdt(Xtr, ytr, seed=7):
    pre = build_preprocess(Xtr)
    # Conservative, but explicit hyperparams to avoid "what exactly?" reviewer question
    clf = HistGradientBoostingClassifier(
        learning_rate=0.05,
        max_depth=8,
        max_leaf_nodes=63,
        max_bins=255,
        min_samples_leaf=40,
        l2_regularization=1e-3,
        random_state=seed
    )
    pipe = Pipeline([("pre", pre), ("clf", clf)])
    pipe.fit(Xtr, ytr)
    return pipe

def choose_threshold_by_fpr(yv, sv, fpr_budget):
    fpr, tpr, thr = roc_curve(yv, sv)
    # thr is descending; pick the lowest threshold that satisfies fpr <= budget AND yields some recall
    ok = np.where(fpr <= fpr_budget)[0]
    if len(ok)==0:
        return float("inf")
    # among ok, prefer max tpr; tie -> highest threshold
    best = ok[np.argmax(tpr[ok])]
    return float(thr[best])

def eval_and_save(pipe, Xte, yte, out_parquet):
    sv = pipe.predict_proba(Xte)[:,1] if hasattr(pipe, "predict_proba") else pipe.decision_function(Xte)
    # HistGBDT has predict_proba
    p = (sv >= 0.5).astype(int)
    out = pd.DataFrame({"y_true": yte.astype(int), "y_score": sv.astype(float), "y_pred": p.astype(int)})
    out.to_parquet(out_parquet, index=False)
    return sv

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--train", nargs="+", required=True)
    ap.add_argument("--val", nargs="+", required=True)
    ap.add_argument("--test", nargs="+", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--tag", required=True)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--random_split", action="store_true")
    args = ap.parse_args()

    # load/concat
    def concat(paths):
        dfs, ys = [], []
        for p in paths:
            df, y = load_parquet(p)
            dfs.append(df); ys.append(y)
        X = pd.concat(dfs, ignore_index=True)
        y = np.concatenate(ys, axis=0)
        return X, y

    Xtr, ytr = concat(args.train)
    Xv, yv   = concat(args.val)
    Xte, yte = concat(args.test)

    if args.random_split:
        # ignore supplied splits; do random split on combined
        Xall = pd.concat([Xtr, Xv, Xte], ignore_index=True)
        yall = np.concatenate([ytr, yv, yte], axis=0)
        Xtr, Xtmp, ytr, ytmp = train_test_split(Xall, yall, test_size=0.4, random_state=args.seed, stratify=yall)
        Xv, Xte, yv, yte = train_test_split(Xtmp, ytmp, test_size=0.5, random_state=args.seed, stratify=ytmp)

    pipe = fit_gbdt(Xtr, ytr, seed=args.seed)

    # Save preds
    os.makedirs(args.outdir, exist_ok=True)
    pred_path = os.path.join(args.outdir, f"{args.tag}_pred.parquet")
    sv = eval_and_save(pipe, Xte, yte, pred_path)

    pr = float(average_precision_score(yte, sv))
    acc = float(accuracy_score(yte, (sv>=0.5).astype(int)))
    fpr95 = float(fpr_at_tpr(yte, sv, 0.95))

    print(f"[GBDT:{args.tag}] n_test={len(yte)} pos_rate={float(yte.mean()):.6f}")
    print(f"[GBDT:{args.tag}] PR-AUC={pr:.6f} ACC={acc:.6f} FPR@TPR=0.95={fpr95:.6f}")
    print(f"[OK] wrote {pred_path}")

    # Budget thresholds chosen on VAL, evaluated on TEST
    # Note: in random_split mode, we already have Xv,yv
    sv_val = pipe.predict_proba(Xv)[:,1]
    for budget in (0.01, 0.05):
        thr = choose_threshold_by_fpr(yv, sv_val, budget)
        yhat = (sv >= thr).astype(int) if np.isfinite(thr) else np.zeros_like(yte)
        prec, rec, f1, _ = precision_recall_fscore_support(yte, yhat, average="binary", zero_division=0)
        # realized fpr on test
        neg = (yte==0)
        realized_fpr = float((yhat[neg]==1).mean()) if neg.any() else float("nan")
        print(f"[BUDGET {budget:.2%}] thr={thr} P={prec:.6f} R={rec:.6f} F1={f1:.6f} FPR={realized_fpr:.6f}")

if __name__ == "__main__":
    main()
PY

# ---- write python: bootstrap CI on score metrics
cat > "$OUT/bootstrap_ci_score.py" <<'PY'
import argparse, numpy as np, pandas as pd
from sklearn.metrics import average_precision_score, roc_curve, accuracy_score

def fpr_at_tpr(y, s, target_tpr=0.95):
    fpr, tpr, _ = roc_curve(y, s)
    idx = np.where(tpr >= target_tpr)[0]
    return np.nan if len(idx)==0 else float(fpr[idx[0]])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pred", required=True)
    ap.add_argument("--B", type=int, default=2000)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    df = pd.read_parquet(args.pred)
    y = df["y_true"].to_numpy().astype(int)
    s = df["y_score"].to_numpy().astype(float)

    n = len(y)
    rng = np.random.default_rng(args.seed)
    pr, fpr95, acc = [], [], []
    for _ in range(args.B):
        idx = rng.integers(0, n, size=n)
        yy, ss = y[idx], s[idx]
        pr.append(average_precision_score(yy, ss))
        fpr95.append(fpr_at_tpr(yy, ss, 0.95))
        acc.append(accuracy_score(yy, (ss>=0.5).astype(int)))

    def ci(arr):
        a = np.array(arr, float)
        return float(a.mean()), float(np.quantile(a, 0.025)), float(np.quantile(a, 0.975))

    pr_m, pr_l, pr_u = ci(pr)
    f_m, f_l, f_u = ci(fpr95)
    a_m, a_l, a_u = ci(acc)

    print(f"[RESULT] Bootstrap (B={args.B})")
    print(f"  PR-AUC mean={pr_m:.6f}  CI95=[{pr_l:.6f}, {pr_u:.6f}]")
    print(f"  FPR@TPR=0.95 mean={f_m:.6f}  CI95=[{f_l:.6f}, {f_u:.6f}]")
    print(f"  ACC mean={a_m:.6f}  CI95=[{a_l:.6f}, {a_u:.6f}]")

if __name__ == "__main__":
    main()
PY

# ---- run: no-Monday (Train Tue+Wed, Val Thu, Test Fri)
echo "[RUN] no-Monday day-shift"
run_py "$OUT/gbdt_cicids.py" \
  --train "$PROC/cicids_Tuesday.parquet" "$PROC/cicids_Wednesday.parquet" \
  --val   "$PROC/cicids_Thursday.parquet" \
  --test  "$PROC/cicids_Friday.parquet" \
  --outdir "$OUT" --tag "cicids_gbdt_no_monday" \
  | tee "$OUT/cicids_gbdt_no_monday.txt"

run_py "$OUT/bootstrap_ci_score.py" \
  --pred "$OUT/cicids_gbdt_no_monday_pred.parquet" --B 2000 \
  | tee "$OUT/cicids_gbdt_no_monday_ci_score.txt"

# ---- run: random split on combined Tue+Wed+Thu+Fri
echo "[RUN] random split (combined)"
run_py "$OUT/gbdt_cicids.py" \
  --train "$PROC/cicids_Tuesday.parquet" "$PROC/cicids_Wednesday.parquet" \
  --val   "$PROC/cicids_Thursday.parquet" \
  --test  "$PROC/cicids_Friday.parquet" \
  --outdir "$OUT" --tag "cicids_gbdt_random" --random_split \
  | tee "$OUT/cicids_gbdt_random.txt"

run_py "$OUT/bootstrap_ci_score.py" \
  --pred "$OUT/cicids_gbdt_random_pred.parquet" --B 2000 \
  | tee "$OUT/cicids_gbdt_random_ci_score.txt"

# ---- per-day test: Train Tue+Wed, Val Thu, Test = each day (Tue/Wed/Thu/Fri)
echo "[RUN] per-day tests"
for DAY in Tuesday Wednesday Thursday Friday; do
  run_py "$OUT/gbdt_cicids.py" \
    --train "$PROC/cicids_Tuesday.parquet" "$PROC/cicids_Wednesday.parquet" \
    --val   "$PROC/cicids_Thursday.parquet" \
    --test  "$PROC/cicids_${DAY}.parquet" \
    --outdir "$OUT" --tag "cicids_gbdt_test_${DAY}" \
    | tee "$OUT/cicids_gbdt_test_${DAY}.txt"
done

echo "[OK] Suite complete. Outdir: $OUT"
echo "[TIP] Key files:"
ls -1 "$OUT" | sed 's/^/  - /'
