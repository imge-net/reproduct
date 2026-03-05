import os
import pandas as pd

p_in = "/home/msoylu/work/mycelium_ids/lab/flows/lab_preds.parquet"
p_out = "/home/msoylu/work/mycelium_ids/lab/flows/lab_preds_flip.parquet"

print("[INFO] in =", p_in, "exists=", os.path.exists(p_in))
print("[INFO] out dir exists=", os.path.isdir(os.path.dirname(p_out)))

df = pd.read_parquet(p_in)
print("[INFO] loaded rows=", len(df), "cols=", list(df.columns))

df["y_score"] = 1.0 - df["y_score"]
df["y_pred"]  = (df["y_score"] >= 0.5).astype(int)

# ensure directory exists
os.makedirs(os.path.dirname(p_out), exist_ok=True)

df.to_parquet(p_out, index=False)
print("[OK] wrote", p_out, "exists_now=", os.path.exists(p_out))
