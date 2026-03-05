#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, os, math
import numpy as np
import pandas as pd

import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader

from sklearn.model_selection import train_test_split
from sklearn.metrics import average_precision_score, roc_curve, accuracy_score


PORTABLE_COLS = ["dur","proto","state","spkts","dpkts","sbytes","dbytes"]
PORTS_EXTRA   = ["sport","dport"]


def fpr_at_tpr(y_true, y_score, tpr_target=0.95):
    fpr, tpr, _ = roc_curve(y_true, y_score)
    idx = np.where(tpr >= tpr_target)[0]
    return float("nan") if len(idx) == 0 else float(fpr[idx[0]])


class TabularDataset(Dataset):
    def __init__(self, X_num, X_cat, y, row_id):
        self.X_num = torch.tensor(X_num, dtype=torch.float32)
        self.X_cat = torch.tensor(X_cat, dtype=torch.long)
        self.y     = torch.tensor(y, dtype=torch.float32)
        self.row_id = torch.tensor(row_id, dtype=torch.long)

    def __len__(self):
        return self.y.shape[0]

    def __getitem__(self, i):
        return self.X_num[i], self.X_cat[i], self.y[i], self.row_id[i]


class MLP(nn.Module):
    def __init__(self, n_num, cat_dims, emb_dim=16, hidden=(256,128), dropout=0.1):
        super().__init__()
        self.embs = nn.ModuleList([nn.Embedding(cd, emb_dim) for cd in cat_dims])
        in_dim = n_num + emb_dim * len(cat_dims)
        layers=[]
        prev=in_dim
        for h in hidden:
            layers += [nn.Linear(prev, h), nn.ReLU(), nn.Dropout(dropout)]
            prev=h
        layers += [nn.Linear(prev, 1)]
        self.net = nn.Sequential(*layers)

    def forward(self, x_num, x_cat):
        embs = [emb(x_cat[:,i]) for i,emb in enumerate(self.embs)]
        x = torch.cat([x_num] + embs, dim=1) if embs else x_num
        return self.net(x).squeeze(1)


class SimpleFTT(nn.Module):
    """
    Lightweight FT-Transformer style:
    - Each feature -> token embedding
    - Transformer encoder over tokens
    - CLS token pooled -> logits
    """
    def __init__(self, n_num, cat_dims, d_token=64, n_heads=4, n_layers=2, dropout=0.1):
        super().__init__()
        self.n_num = n_num
        self.n_cat = len(cat_dims)
        self.d = d_token

        # numeric features: per-feature linear projection to token
        self.num_proj = nn.ModuleList([nn.Linear(1, d_token) for _ in range(n_num)])

        # categorical features: embedding to token
        self.cat_emb = nn.ModuleList([nn.Embedding(cd, d_token) for cd in cat_dims])

        self.cls = nn.Parameter(torch.zeros(1,1,d_token))
        enc_layer = nn.TransformerEncoderLayer(
            d_model=d_token, nhead=n_heads,
            dim_feedforward=4*d_token, dropout=dropout,
            batch_first=True, activation="gelu"
        )
        self.enc = nn.TransformerEncoder(enc_layer, num_layers=n_layers)
        self.head = nn.Sequential(nn.LayerNorm(d_token), nn.Linear(d_token, 1))

    def forward(self, x_num, x_cat):
        toks=[]
        # numeric -> tokens
        for j in range(self.n_num):
            v = x_num[:, j:j+1]
            toks.append(self.num_proj[j](v).unsqueeze(1))
        # cat -> tokens
        for i in range(self.n_cat):
            toks.append(self.cat_emb[i](x_cat[:,i]).unsqueeze(1))
        x = torch.cat(toks, dim=1) if toks else x_num.unsqueeze(1)

        B = x.shape[0]
        cls = self.cls.expand(B, -1, -1)
        x = torch.cat([cls, x], dim=1)
        x = self.enc(x)
        cls_out = x[:,0,:]
        return self.head(cls_out).squeeze(1)


def fit_encoders(df, cat_cols):
    cat_maps={}
    cat_dims=[]
    for c in cat_cols:
        # string->category codes (stable)
        vals = df[c].astype(str).fillna("NA").unique().tolist()
        vals = sorted(vals)
        m = {v:i for i,v in enumerate(vals)}
        cat_maps[c]=m
        cat_dims.append(len(vals))
    return cat_maps, cat_dims


def transform(df, num_cols, cat_cols, cat_maps):
    X_num = df[num_cols].copy()
    for c in num_cols:
        X_num[c] = pd.to_numeric(X_num[c], errors="coerce")
    # numeric impute median
    X_num = X_num.fillna(X_num.median(numeric_only=True))
    X_num = X_num.to_numpy(dtype=np.float32)

    X_cat = []
    for c in cat_cols:
        m = cat_maps[c]
        x = df[c].astype(str).fillna("NA").map(lambda v: m.get(v, 0)).to_numpy(dtype=np.int64)
        X_cat.append(x)
    X_cat = np.stack(X_cat, axis=1) if len(cat_cols) else np.zeros((len(df),0), dtype=np.int64)
    return X_num, X_cat


def train_loop(model, train_loader, val_loader, device, epochs=20, lr=1e-3):
    model.to(device)
    opt = torch.optim.AdamW(model.parameters(), lr=lr)
    loss_fn = nn.BCEWithLogitsLoss()

    best_ap=-1.0
    best_state=None

    for ep in range(1, epochs+1):
        model.train()
        for xnum,xcat,y,_ in train_loader:
            xnum,xcat,y = xnum.to(device), xcat.to(device), y.to(device)
            opt.zero_grad(set_to_none=True)
            logits = model(xnum,xcat)
            loss = loss_fn(logits, y)
            loss.backward()
            opt.step()

        # validate with PR-AUC
        model.eval()
        ys=[]
        ss=[]
        with torch.no_grad():
            for xnum,xcat,y,_ in val_loader:
                xnum,xcat = xnum.to(device), xcat.to(device)
                logits = model(xnum,xcat)
                s = torch.sigmoid(logits).cpu().numpy()
                ys.append(y.numpy())
                ss.append(s)
        yv = np.concatenate(ys)
        sv = np.concatenate(ss)
        ap = float(average_precision_score(yv, sv))

        if ap > best_ap:
            best_ap = ap
            best_state = {k:v.detach().cpu().clone() for k,v in model.state_dict().items()}

    if best_state is not None:
        model.load_state_dict(best_state)
    return best_ap


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parquet", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--feature_set", choices=["portable","ports"], default="portable")
    ap.add_argument("--model", choices=["mlp","ftt"], default="mlp")
    ap.add_argument("--epochs", type=int, default=20)
    ap.add_argument("--batch", type=int, default=512)
    ap.add_argument("--lr", type=float, default=1e-3)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--device", default="cpu")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    np.random.seed(args.seed)
    torch.manual_seed(args.seed)

    df = pd.read_parquet(args.parquet).reset_index(drop=True)
    if "y_true" not in df.columns:
        raise SystemExit("[ERR] parquet must contain y_true")
    df["row_id"] = np.arange(len(df), dtype=np.int64)

    cols = PORTABLE_COLS[:] if args.feature_set == "portable" else (PORTABLE_COLS + PORTS_EXTRA)
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise SystemExit(f"[ERR] missing columns in parquet: {missing}")

    y = df["y_true"].astype(int).to_numpy()
    # choose numeric vs categorical
    cat_cols=[]
    num_cols=[]
    for c in cols:
        if c in ["proto","state"]:
            cat_cols.append(c)
        else:
            num_cols.append(c)

    # split: train/val/test (calibration-safe)
    idx = np.arange(len(df))
    idx_tr, idx_te = train_test_split(idx, test_size=0.20, random_state=args.seed, stratify=y)
    y_tr = y[idx_tr]
    idx_tr, idx_va = train_test_split(idx_tr, test_size=0.20, random_state=args.seed, stratify=y_tr)

    df_tr = df.iloc[idx_tr].copy()
    df_va = df.iloc[idx_va].copy()
    df_te = df.iloc[idx_te].copy()

    cat_maps, cat_dims = fit_encoders(df_tr, cat_cols)
    Xtr_num, Xtr_cat = transform(df_tr, num_cols, cat_cols, cat_maps)
    Xva_num, Xva_cat = transform(df_va, num_cols, cat_cols, cat_maps)
    Xte_num, Xte_cat = transform(df_te, num_cols, cat_cols, cat_maps)

    ytr = df_tr["y_true"].astype(int).to_numpy()
    yva = df_va["y_true"].astype(int).to_numpy()
    yte = df_te["y_true"].astype(int).to_numpy()
    rowid_te = df_te["row_id"].to_numpy(dtype=np.int64)

    train_loader = DataLoader(TabularDataset(Xtr_num, Xtr_cat, ytr, df_tr["row_id"].to_numpy()), batch_size=args.batch, shuffle=True, drop_last=False)
    val_loader   = DataLoader(TabularDataset(Xva_num, Xva_cat, yva, df_va["row_id"].to_numpy()), batch_size=args.batch, shuffle=False)
    test_loader  = DataLoader(TabularDataset(Xte_num, Xte_cat, yte, rowid_te), batch_size=args.batch, shuffle=False)

    device = torch.device(args.device)

    if args.model == "mlp":
        model = MLP(n_num=len(num_cols), cat_dims=cat_dims, emb_dim=16, hidden=(256,128), dropout=0.1)
        tag = "mlp"
    else:
        model = SimpleFTT(n_num=len(num_cols), cat_dims=cat_dims, d_token=64, n_heads=4, n_layers=2, dropout=0.1)
        tag = "ftt"

    best_val_ap = train_loop(model, train_loader, val_loader, device, epochs=args.epochs, lr=args.lr)

    # test scores
    model.eval()
    ys=[]
    ss=[]
    rids=[]
    with torch.no_grad():
        for xnum,xcat,y,rid in test_loader:
            xnum,xcat = xnum.to(device), xcat.to(device)
            logits = model(xnum,xcat)
            s = torch.sigmoid(logits).cpu().numpy()
            ys.append(y.numpy())
            ss.append(s)
            rids.append(rid.numpy())
    yte = np.concatenate(ys).astype(int)
    ste = np.concatenate(ss).astype(float)
    rowid_te = np.concatenate(rids).astype(np.int64)
    ypred = (ste >= 0.5).astype(int)

    prauc = float(average_precision_score(yte, ste))
    acc   = float(accuracy_score(yte, ypred))
    fpr95 = float(fpr_at_tpr(yte, ste, 0.95))

    pred = pd.DataFrame({"row_id": rowid_te, "y_true": yte, "y_score": ste, "y_pred": ypred})
    out_pred = os.path.join(args.outdir, f"dl_{tag}_{args.feature_set}_pred.parquet")
    pred.to_parquet(out_pred, index=False)

    out_txt = os.path.join(args.outdir, f"dl_{tag}_{args.feature_set}_summary.txt")
    with open(out_txt, "w", encoding="utf-8") as f:
        f.write(f"model={tag}\nfeature_set={args.feature_set}\n")
        f.write(f"val_PR-AUC(best)={best_val_ap:.6f}\n")
        f.write(f"test_PR-AUC={prauc:.6f}\n")
        f.write(f"test_FPR@TPR=0.95={fpr95:.6f}\n")
        f.write(f"test_ACC={acc:.6f}\n")
        f.write(f"pred_parquet={out_pred}\n")

    print(f"[OK] {tag} ({args.feature_set}) val_AP={best_val_ap:.6f} test_PR-AUC={prauc:.6f} FPR@TPR=0.95={fpr95:.6f} ACC={acc:.6f}")
    print("[OK] wrote", out_pred)
    print("[OK] wrote", out_txt)


if __name__ == "__main__":
    main()
