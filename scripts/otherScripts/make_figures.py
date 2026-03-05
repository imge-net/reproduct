import argparse, os, re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import precision_recall_curve, roc_curve, average_precision_score, roc_auc_score

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def load_pred(path):
    df = pd.read_parquet(path)
    # expect at least y_true, y_score
    cols = set(df.columns)
    if not {"y_true","y_score"}.issubset(cols):
        raise ValueError(f"{path}: expected columns y_true,y_score; got {list(df.columns)}")
    y = df["y_true"].astype(int).to_numpy()
    s = df["y_score"].astype(float).to_numpy()
    return y, s

def save_pr_curve(y, s, out_pdf, title=None):
    p, r, _ = precision_recall_curve(y, s)
    ap = average_precision_score(y, s)
    base = float(np.mean(y))
    plt.figure()
    plt.plot(r, p)
    plt.xlabel("Recall")
    plt.ylabel("Precision")
    t = title or "PR Curve"
    plt.title(f"{t}  (AP={ap:.4f}, no-skill={base:.4f})")
    plt.tight_layout()
    plt.savefig(out_pdf)
    plt.close()

def save_pr_curve_two(y1, s1, y2, s2, out_pdf, title, label1="portable", label2="+ports (diag)"):
    p1, r1, _ = precision_recall_curve(y1, s1)
    ap1 = average_precision_score(y1, s1)

    p2, r2, _ = precision_recall_curve(y2, s2)
    ap2 = average_precision_score(y2, s2)

    base = float(np.mean(y1))  # same dataset, same prevalence

    plt.figure()
    plt.step(r1, p1, where="post", label=f"{label1} (AP={ap1:.4f})")
    plt.step(r2, p2, where="post", label=f"{label2} (AP={ap2:.4f})")
    plt.hlines(base, 0.0, 1.0, linestyles="dashed", label=f"no-skill={base:.4f}")

    plt.xlabel("Recall")
    plt.ylabel("Precision")
    plt.title(title)
    plt.legend(loc="best", frameon=False)
    plt.tight_layout()
    plt.savefig(out_pdf)
    plt.close()

def save_roc_curve(y, s, out_pdf, title=None):
    fpr, tpr, _ = roc_curve(y, s)
    try:
        auc = roc_auc_score(y, s)
    except Exception:
        auc = float("nan")
    plt.figure()
    plt.plot(fpr, tpr)
    plt.xlabel("FPR")
    plt.ylabel("TPR")
    t = title or "ROC Curve"
    plt.title(f"{t}  (ROC-AUC={auc:.4f})")
    plt.tight_layout()
    plt.savefig(out_pdf)
    plt.close()

def fpr_at_tpr(y, s, target=0.95):
    fpr, tpr, thr = roc_curve(y, s)
    idx = np.where(tpr >= target)[0]
    if len(idx)==0:
        return np.nan, np.inf
    j = idx[0]
    return float(fpr[j]), float(thr[j])

def best_budget_threshold(y, s, fpr_budget):
    fpr, tpr, thr = roc_curve(y, s)
    # roc_curve returns thresholds descending; include inf. We find smallest FPR <= budget with max TPR.
    ok = np.where(fpr <= fpr_budget)[0]
    if len(ok)==0:
        return np.inf, 0.0, 0.0
    # choose index that maximizes tpr among ok
    j = ok[np.argmax(tpr[ok])]
    return float(thr[j]), float(tpr[j]), float(fpr[j])

def save_budget_curve(y, s, out_pdf, title=None):
    # sweep thresholds by percentiles to get a smooth-ish curve
    ths = np.unique(np.quantile(s, np.linspace(0,1,2001)))
    fprs=[]
    tprs=[]
    for t in ths:
        pred = (s >= t).astype(int)
        # compute confusion
        tp = np.sum((pred==1) & (y==1))
        fp = np.sum((pred==1) & (y==0))
        tn = np.sum((pred==0) & (y==0))
        fn = np.sum((pred==0) & (y==1))
        fpr = fp / (fp+tn) if (fp+tn)>0 else 0.0
        tpr = tp / (tp+fn) if (tp+fn)>0 else 0.0
        fprs.append(fpr); tprs.append(tpr)
    fprs=np.array(fprs); tprs=np.array(tprs)

    # mark budgets
    bud1 = best_budget_threshold(y,s,0.01)
    bud5 = best_budget_threshold(y,s,0.05)

    plt.figure()
    plt.plot(fprs, tprs)
    plt.xlabel("FPR")
    plt.ylabel("TPR / Recall")
    t = title or "Operating Points"
    plt.title(t)
    # annotate budget points
    for (b,lab) in [(bud1,"FPR<=1%"), (bud5,"FPR<=5%")]:
        thr, tpr_b, fpr_b = b
        plt.scatter([fpr_b],[tpr_b])
        plt.text(fpr_b, tpr_b, f" {lab}\n TPR={tpr_b:.3f}", fontsize=8)
    plt.tight_layout()
    plt.savefig(out_pdf)
    plt.close()

def parse_shift_diag(txt_path, out_pdf, topk=10, title="Shift diagnostics (PSI)"):
    """
    Parse a pandas-printed whitespace table that includes at least:
      feature, type, psi, ks, ...
    Feature names may contain spaces. Lines look like:
      Bwd Packet Length Std  num 3.600012 0.442312 NaN 1.155306e+03 ...
    We extract psi/ks as the first two floats after the type token.
    """
    import re

    lines = []
    with open(txt_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = [ln.rstrip("\n") for ln in f]

    # find header containing 'feature' 'psi' 'ks'
    header_idx = None
    for i, ln in enumerate(lines):
        low = ln.strip().lower()
        if low.startswith("feature") and (" psi" in low) and (" ks" in low):
            header_idx = i
            break
    if header_idx is None:
        raise RuntimeError(f"Could not parse shift diag: {txt_path}")

    rows = []
    for ln in lines[header_idx + 1 :]:
        if not ln.strip():
            break
        # skip obvious separators
        if ln.strip().startswith("#"):
            continue

        # split on whitespace
        parts = ln.strip().split()
        if len(parts) < 4:
            continue

        # locate the 'type' token (usually 'num' or 'cat')
        # we assume the first occurrence of 'num'/'cat' marks end of feature name.
        type_pos = None
        for j, tok in enumerate(parts):
            if tok in ("num", "cat"):
                type_pos = j
                break
        if type_pos is None or type_pos + 2 >= len(parts):
            continue

        feat = " ".join(parts[:type_pos])

        # after type token, the next two numeric fields are psi and ks
        # allow scientific notation, NaN, etc.
        def to_float(x):
            try:
                return float(x)
            except:
                return None

        psi = to_float(parts[type_pos + 1])
        ks  = to_float(parts[type_pos + 2])

        if psi is None or ks is None:
            # fallback: find first two floats after type_pos
            nums = []
            for tok in parts[type_pos + 1 :]:
                v = to_float(tok)
                if v is not None:
                    nums.append(v)
                if len(nums) == 2:
                    break
            if len(nums) < 2:
                continue
            psi, ks = nums[0], nums[1]

        rows.append((feat, psi, ks))

    if not rows:
        raise RuntimeError(f"Could not parse shift diag: {txt_path}")

    rows = sorted(rows, key=lambda x: x[1], reverse=True)[:topk]
    feats = [r[0] for r in rows][::-1]
    psis  = [r[1] for r in rows][::-1]

    plt.figure()
    plt.barh(feats, psis)
    plt.xlabel("PSI")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(out_pdf)
    plt.close()
    
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--outdir", default="figures")
    ap.add_argument("--cicids_rf", required=True)
    ap.add_argument("--myc_portable_rf", required=True)
    ap.add_argument("--myc_ports_rf", required=False)
    ap.add_argument("--shift_diag_txt", required=False)
    args=ap.parse_args()

    ensure_dir(args.outdir)

    # CICIDS: PR + budget curve
    y,s = load_pred(args.cicids_rf)
    save_pr_curve(y,s, os.path.join(args.outdir,"fig_cicids_nomonday_pr.pdf"),
                  title="CICIDS No-Monday (RF)")
    save_budget_curve(y,s, os.path.join(args.outdir,"fig_cicids_nomonday_budget.pdf"),
                      title="CICIDS No-Monday (RF): TPR vs FPR")
    save_roc_curve(y,s, os.path.join(args.outdir,"fig_cicids_nomonday_roc.pdf"),
                   title="CICIDS No-Monday (RF)")

    # Mycelium: portable PR + budget
    # Mycelium: portable PR + budget
    y2,s2 = load_pred(args.myc_portable_rf)
    save_budget_curve(y2,s2, os.path.join(args.outdir,"fig_myc_portable_budget.pdf"),
                      title="Mycelium overlay (portable RF): TPR vs FPR")

    # Combined PR: portable vs +ports (diagnostic)
    if args.myc_ports_rf:
        y3,s3 = load_pred(args.myc_ports_rf)
        save_pr_curve_two(
            y2, s2, y3, s3,
            os.path.join(args.outdir,"fig_myc_pr_portable_vs_ports.pdf"),
            title="Mycelium overlay: portable vs +ports (PR curve)",
            label1="portable",
            label2="+ports (diagnostic)"
        )

    # Shift diagnostics bar plot (optional)
    if args.shift_diag_txt:
        parse_shift_diag(args.shift_diag_txt,
                         os.path.join(args.outdir,"fig_shift_diag_psi.pdf"),
                         topk=10,
                         title="CICIDS Wednesday→Friday: PSI (top-10)")

    print("[OK] wrote figures to:", os.path.abspath(args.outdir))

if __name__=="__main__":
    main()
