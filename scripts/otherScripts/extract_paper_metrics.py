import argparse
import glob
import json
import os
import re
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple, List

# -------------------------
# Regex helpers
# -------------------------
RE_SCORE = {
    "prauc": re.compile(r"PR-AUC\s+mean=([0-9.]+)\s+CI95=\[([0-9.]+),\s*([0-9.]+)\]"),
    "fpr95": re.compile(r"FPR@TPR=0\.95\s+mean=([0-9.]+)\s+CI95=\[([0-9.]+),\s*([0-9.]+)\]"),
    "acc":   re.compile(r"ACC\s+mean=([0-9.]+)\s+CI95=\[([0-9.]+),\s*([0-9.]+)\]"),
}

RE_PRED = {
    "precision": re.compile(r"Precision\s+mean=([0-9.]+)\s+CI95=\[([0-9.]+),\s*([0-9.]+)\]"),
    "recall":    re.compile(r"Recall\s+mean=([0-9.]+)\s+CI95=\[([0-9.]+),\s*([0-9.]+)\]"),
    "f1":        re.compile(r"F1\s+mean=([0-9.]+)\s+CI95=\[([0-9.]+),\s*([0-9.]+)\]"),
    "tpr":       re.compile(r"TPR\s+mean=([0-9.]+)\s+CI95=\[([0-9.]+),\s*([0-9.]+)\]"),
    "fpr":       re.compile(r"FPR\s+mean=([0-9.]+)\s+CI95=\[([0-9.]+),\s*([0-9.]+)\]"),
}

RE_FEWSHOT_HDR = re.compile(r"\[FewShot\]\s+k=\s*(\d+)\s+reps=\s*(\d+)")
RE_FEWSHOT_LINE = {
    "f1": re.compile(r"F1\s+mean=\s*([0-9.eE+-]+)\s+std=\s*([0-9.eE+-]+)"),
    "p":  re.compile(r"P\s+mean=\s*([0-9.eE+-]+)\s+std=\s*([0-9.eE+-]+)"),
    "r":  re.compile(r"R\s+mean=\s*([0-9.eE+-]+)\s+std=\s*([0-9.eE+-]+)"),
}

RE_POINT_UNSW_LOG = re.compile(r"PR-AUC\(AP\):\s*([0-9.]+)\s*.*?FPR@TPR=0\.95:\s*([0-9.]+)", re.S)
RE_ORACLE = re.compile(r"\[ORACLE\]\s+best_F1=\s*([0-9.]+)\s+thr=\s*([0-9.]+).*?precision=\s*([0-9.]+).*?recall=\s*([0-9.]+)", re.S)
RE_OP_BESTF1 = re.compile(r"\[BestF1\]\s*?\n\s*thr=\s*([0-9.]+)\s+F1=\s*([0-9.]+)\s+P=\s*([0-9.]+)\s+R=\s*([0-9.]+)", re.S)

# -------------------------
# Data containers
# -------------------------
@dataclass
class CITriplet:
    mean: float
    low: float
    high: float

@dataclass
class ScoreCI:
    prauc: Optional[CITriplet] = None
    fpr95: Optional[CITriplet] = None
    acc:   Optional[CITriplet] = None

@dataclass
class PredCI:
    precision: Optional[CITriplet] = None
    recall:    Optional[CITriplet] = None
    f1:        Optional[CITriplet] = None
    tpr:       Optional[CITriplet] = None
    fpr:       Optional[CITriplet] = None

@dataclass
class FewShot:
    k: int
    reps: int
    f1_mean: float
    f1_std: float
    p_mean: float
    p_std: float
    r_mean: float
    r_std: float

# -------------------------
# Parsing functions
# -------------------------
def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()

def parse_ci_triplet(regex: re.Pattern, text: str) -> Optional[CITriplet]:
    m = regex.search(text)
    if not m:
        return None
    return CITriplet(mean=float(m.group(1)), low=float(m.group(2)), high=float(m.group(3)))

def parse_score_ci(path: str) -> ScoreCI:
    t = read_text(path)
    return ScoreCI(
        prauc=parse_ci_triplet(RE_SCORE["prauc"], t),
        fpr95=parse_ci_triplet(RE_SCORE["fpr95"], t),
        acc=parse_ci_triplet(RE_SCORE["acc"], t),
    )

def parse_pred_ci(path: str) -> PredCI:
    t = read_text(path)
    out = PredCI()
    for k, rx in RE_PRED.items():
        trip = parse_ci_triplet(rx, t)
        setattr(out, k, trip)
    return out

def parse_fewshot(path: str) -> Optional[FewShot]:
    t = read_text(path)
    mh = RE_FEWSHOT_HDR.search(t)
    if not mh:
        return None
    k = int(mh.group(1))
    reps = int(mh.group(2))
    vals = {}
    for key, rx in RE_FEWSHOT_LINE.items():
        m = rx.search(t)
        if not m:
            return None
        vals[key] = (float(m.group(1)), float(m.group(2)))
    return FewShot(
        k=k, reps=reps,
        f1_mean=vals["f1"][0], f1_std=vals["f1"][1],
        p_mean=vals["p"][0], p_std=vals["p"][1],
        r_mean=vals["r"][0], r_std=vals["r"][1],
    )

def newest_match(patterns: List[str]) -> Optional[str]:
    files = []
    for pat in patterns:
        files.extend(glob.glob(os.path.expanduser(pat)))
    if not files:
        return None
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files[0]

def fmt_ci(ci: Optional[CITriplet], nd=6) -> str:
    if ci is None:
        return "NA"
    return f"{ci.mean:.{nd}f} [{ci.low:.{nd}f}, {ci.high:.{nd}f}]"

def fmt_float(x: Optional[float], nd=6) -> str:
    return "NA" if x is None else f"{x:.{nd}f}"

# -------------------------
# Main extraction map
# -------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.path.expanduser("~/work/mycelium_ids"))
    ap.add_argument("--outdir", default=os.path.expanduser("~/work/mycelium_ids/paper_results"))
    ap.add_argument("--prefer_run", default="", help="optional run dir to prefer (e.g., runs/20260225_....)")
    args = ap.parse_args()

    root = os.path.expanduser(args.root)
    outdir = os.path.expanduser(args.outdir)
    os.makedirs(outdir, exist_ok=True)

    # Prefer a specific run dir if supplied, otherwise search all runs
    run_glob = os.path.join(root, "runs", "*")
    if args.prefer_run:
        pref = os.path.expanduser(args.prefer_run)
        run_glob = pref  # can be a single dir, still ok with patterns below

    def p(*parts): return os.path.join(root, *parts)

    # Patterns: we pick the newest file among matches
    picks = {
        # UNSW
        "unsw_rf_ci": [p("runs","*","unsw_binary_rf_ci.txt")],
        "unsw_binary_txt": [p("runs","*","unsw_binary.txt")],

        # CICIDS RF
        "cicids_rf_ci": [p("runs","*","cicids_rf_tune_no_monday_f1_ci_score.txt"),
                         p("runs","*","cicids_rf_tune_no_monday_f1_ci.txt"),
                         p("runs","*","cicids_rf_tune_no_monday_f1_ci_score.txt")],
        "cicids_rf_fpr01_pred": [p("runs","*","cicids_rf_no_monday_fpr01_ci_pred.txt")],
        "cicids_rf_fpr05_pred": [p("runs","*","cicids_rf_no_monday_fpr05_ci_pred.txt")],

        # CICIDS few-shot
        "fewshot_k200": [p("runs","*","fewshot_k200.txt")],
        "fewshot_k1000": [p("runs","*","fewshot_k1000.txt")],
        "oracle_ops": [p("runs","*","op_points.txt"), p("runs","*","lab_flip_ops.txt"), p("lab","flows","lab_flip_ops.txt")],

        # Mycelium single-shot (new script outputs)
        "myc_single_ci": [p("runs","*","mycelium_single_shot_ci_score.txt")],
        "myc_single_fpr01": [p("runs","*","mycelium_single_shot_fpr01_ci_pred.txt")],
        "myc_single_fpr05": [p("runs","*","mycelium_single_shot_fpr05_ci_pred.txt")],

        # GBDT outputs (from gbdt scripts)
        "unsw_gbdt_ci": [p("runs","*","unsw_gbdt_ci_score.txt")],
        "cicids_gbdt_ci": [p("runs","*","cicids_gbdt_no_monday_ci_score.txt")],
        "cicids_gbdt_fpr01": [p("runs","*","cicids_gbdt_no_monday_fpr01_ci_pred.txt")],
        "cicids_gbdt_fpr05": [p("runs","*","cicids_gbdt_no_monday_fpr05_ci_pred.txt")],

        # Diagnostic (older)
        "myc_diag_ci": [p("lab","flows","lab_flip_ci_score.txt")],
    }

    chosen: Dict[str, Optional[str]] = {k: newest_match(v) for k, v in picks.items()}

    # Parse
    results: Dict[str, Dict] = {"_meta": {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "root": root,
        "chosen_files": chosen
    }}

    # UNSW RF CI
    if chosen["unsw_rf_ci"]:
        results["unsw_rf_ci"] = asdict(parse_score_ci(chosen["unsw_rf_ci"]))
    else:
        results["unsw_rf_ci"] = {}

    # UNSW LogReg point (from unsw_binary.txt tail)
    # We'll just store the best-effort PR-AUC(AP) and FPR95 if found.
    if chosen["unsw_binary_txt"]:
        t = read_text(chosen["unsw_binary_txt"])
        m = RE_POINT_UNSW_LOG.search(t)
        if m:
            results["unsw_logreg_point"] = {"prauc": float(m.group(1)), "fpr95": float(m.group(2))}
        else:
            results["unsw_logreg_point"] = {}
    else:
        results["unsw_logreg_point"] = {}

    # CICIDS RF score CI
    if chosen["cicids_rf_ci"]:
        results["cicids_rf_ci"] = asdict(parse_score_ci(chosen["cicids_rf_ci"]))
    else:
        results["cicids_rf_ci"] = {}

    # CICIDS RF budget pred CIs
    for key in ["cicids_rf_fpr01_pred","cicids_rf_fpr05_pred"]:
        if chosen[key]:
            results[key] = asdict(parse_pred_ci(chosen[key]))
        else:
            results[key] = {}

    # Few-shot
    fs = []
    for key in ["fewshot_k200","fewshot_k1000"]:
        if chosen[key]:
            f = parse_fewshot(chosen[key])
            if f:
                fs.append(asdict(f))
    results["fewshot"] = fs

    # Mycelium single-shot
    if chosen["myc_single_ci"]:
        results["mycelium_single_ci"] = asdict(parse_score_ci(chosen["myc_single_ci"]))
    else:
        results["mycelium_single_ci"] = {}
    for key in ["myc_single_fpr01","myc_single_fpr05"]:
        if chosen[key]:
            results[key] = asdict(parse_pred_ci(chosen[key]))
        else:
            results[key] = {}

    # GBDT
    if chosen["unsw_gbdt_ci"]:
        results["unsw_gbdt_ci"] = asdict(parse_score_ci(chosen["unsw_gbdt_ci"]))
    else:
        results["unsw_gbdt_ci"] = {}
    if chosen["cicids_gbdt_ci"]:
        results["cicids_gbdt_ci"] = asdict(parse_score_ci(chosen["cicids_gbdt_ci"]))
    else:
        results["cicids_gbdt_ci"] = {}
    for key in ["cicids_gbdt_fpr01","cicids_gbdt_fpr05"]:
        if chosen[key]:
            results[key] = asdict(parse_pred_ci(chosen[key]))
        else:
            results[key] = {}

    # Write JSON
    jpath = os.path.join(outdir, "summary.json")
    with open(jpath, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    # Markdown summary
    md = []
    md.append(f"# Paper Metrics Summary\n\nGenerated: {results['_meta']['generated_at']}\n")
    md.append("## File picks (newest per pattern)\n")
    for k,v in chosen.items():
        md.append(f"- **{k}**: `{v}`" if v else f"- **{k}**: (missing)")
    md.append("\n## Core score metrics (mean [low, high])\n")

    def pull_score(block: Dict) -> ScoreCI:
        # block is dict(asdict(ScoreCI))
        def get_triplet(name):
            x = block.get(name)
            if not x: return None
            return CITriplet(**x)
        return ScoreCI(prauc=get_triplet("prauc"), fpr95=get_triplet("fpr95"), acc=get_triplet("acc"))

    def pull_pred(block: Dict) -> PredCI:
        def get_triplet(name):
            x = block.get(name)
            if not x: return None
            return CITriplet(**x)
        return PredCI(precision=get_triplet("precision"), recall=get_triplet("recall"), f1=get_triplet("f1"),
                      tpr=get_triplet("tpr"), fpr=get_triplet("fpr"))

    unsw_rf = pull_score(results.get("unsw_rf_ci",{}))
    cic_rf  = pull_score(results.get("cicids_rf_ci",{}))
    myc_ss  = pull_score(results.get("mycelium_single_ci",{}))
    unsw_g  = pull_score(results.get("unsw_gbdt_ci",{}))
    cic_g   = pull_score(results.get("cicids_gbdt_ci",{}))

    md.append(f"- UNSW RF: PR-AUC {fmt_ci(unsw_rf.prauc)} | FPR@TPR0.95 {fmt_ci(unsw_rf.fpr95)} | ACC {fmt_ci(unsw_rf.acc)}")
    md.append(f"- CICIDS RF (no-Monday): PR-AUC {fmt_ci(cic_rf.prauc)} | FPR@TPR0.95 {fmt_ci(cic_rf.fpr95)} | ACC {fmt_ci(cic_rf.acc)}")
    md.append(f"- Mycelium single-shot: PR-AUC {fmt_ci(myc_ss.prauc)} | FPR@TPR0.95 {fmt_ci(myc_ss.fpr95)} | ACC {fmt_ci(myc_ss.acc)}")
    md.append(f"- UNSW GBDT: PR-AUC {fmt_ci(unsw_g.prauc)} | FPR@TPR0.95 {fmt_ci(unsw_g.fpr95)} | ACC {fmt_ci(unsw_g.acc)}")
    md.append(f"- CICIDS GBDT (no-Monday): PR-AUC {fmt_ci(cic_g.prauc)} | FPR@TPR0.95 {fmt_ci(cic_g.fpr95)} | ACC {fmt_ci(cic_g.acc)}")

    md.append("\n## Alarm-budget (thresholded prediction CI)\n")
    for tag in ["cicids_rf_fpr01_pred","cicids_rf_fpr05_pred","cicids_gbdt_fpr01","cicids_gbdt_fpr05","myc_single_fpr01","myc_single_fpr05"]:
        block = results.get(tag, {})
        pc = pull_pred(block)
        if pc.f1 is None and pc.recall is None:
            md.append(f"- {tag}: (missing)")
        else:
            md.append(f"- {tag}: F1 {fmt_ci(pc.f1)} | Prec {fmt_ci(pc.precision)} | Rec/TPR {fmt_ci(pc.recall)} | FPR {fmt_ci(pc.fpr)}")

    md.append("\n## Few-shot\n")
    if results["fewshot"]:
        for row in results["fewshot"]:
            md.append(f"- k={row['k']} reps={row['reps']}: F1 {row['f1_mean']:.4f}±{row['f1_std']:.4f} | P {row['p_mean']:.4f}±{row['p_std']:.4f} | R {row['r_mean']:.4f}±{row['r_std']:.6f}")
    else:
        md.append("- (missing)")

    mpath = os.path.join(outdir, "SUMMARY.md")
    with open(mpath, "w", encoding="utf-8") as f:
        f.write("\n".join(md) + "\n")

    # LaTeX snippets (rows)
    tex = []
    tex.append("% Auto-generated snippets.tex")
    tex.append(f"% Generated: {results['_meta']['generated_at']}")
    tex.append("% Use: \\input{paper_results/snippets.tex}")
    tex.append("")
    tex.append("\\newcommand{\\CI}[3]{#1\\,[#2,#3]}")

    def tex_ci(ci: Optional[CITriplet], nd=6) -> str:
        if ci is None:
            return "NA"
        return f"\\CI{{{ci.mean:.{nd}f}}}{{{ci.low:.{nd}f}}}{{{ci.high:.{nd}f}}}"

    tex.append("")
    tex.append("% Core row fragments: PR-AUC & FPR@TPR=0.95 & ACC")
    tex.append(f"\\def\\UNSWRF{{{tex_ci(unsw_rf.prauc)} & {tex_ci(unsw_rf.fpr95)} & {tex_ci(unsw_rf.acc)}}}")
    tex.append(f"\\def\\CICIDSRF{{{tex_ci(cic_rf.prauc)} & {tex_ci(cic_rf.fpr95)} & {tex_ci(cic_rf.acc)}}}")
    tex.append(f"\\def\\MYCSINGLE{{{tex_ci(myc_ss.prauc)} & {tex_ci(myc_ss.fpr95)} & {tex_ci(myc_ss.acc)}}}")
    tex.append(f"\\def\\UNSWGBDT{{{tex_ci(unsw_g.prauc)} & {tex_ci(unsw_g.fpr95)} & {tex_ci(unsw_g.acc)}}}")
    tex.append(f"\\def\\CICIDSGBDT{{{tex_ci(cic_g.prauc)} & {tex_ci(cic_g.fpr95)} & {tex_ci(cic_g.acc)}}}")

    tex.append("")
    tex.append("% Few-shot macros")
    for row in results["fewshot"]:
        k = row["k"]
        tex.append(f"\\def\\FEWSHOT{k}{{{row['f1_mean']:.4f}\\pm{row['f1_std']:.4f} & {row['p_mean']:.4f}\\pm{row['p_std']:.4f} & {row['r_mean']:.4f}\\pm{row['r_std']:.6f}}}")

    tpath = os.path.join(outdir, "snippets.tex")
    with open(tpath, "w", encoding="utf-8") as f:
        f.write("\n".join(tex) + "\n")

    print("[OK] wrote:")
    print(" -", jpath)
    print(" -", mpath)
    print(" -", tpath)

    # Print a minimal console summary (fast copy/paste)
    print("\n[CORE]")
    print("UNSW RF:", fmt_ci(unsw_rf.prauc), fmt_ci(unsw_rf.fpr95), fmt_ci(unsw_rf.acc))
    print("CICIDS RF:", fmt_ci(cic_rf.prauc), fmt_ci(cic_rf.fpr95), fmt_ci(cic_rf.acc))
    print("MYC single:", fmt_ci(myc_ss.prauc), fmt_ci(myc_ss.fpr95), fmt_ci(myc_ss.acc))
    print("UNSW GBDT:", fmt_ci(unsw_g.prauc), fmt_ci(unsw_g.fpr95), fmt_ci(unsw_g.acc))
    print("CICIDS GBDT:", fmt_ci(cic_g.prauc), fmt_ci(cic_g.fpr95), fmt_ci(cic_g.acc))

if __name__ == "__main__":
    main()
