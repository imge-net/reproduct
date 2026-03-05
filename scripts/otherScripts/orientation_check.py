import pandas as pd
from sklearn.metrics import average_precision_score
df=pd.read_parquet("/home/msoylu/work/mycelium_ids/lab/flows/lab_preds.parquet")
y=df.y_true.to_numpy()
s=df.y_score.to_numpy()
ap1=average_precision_score(y,s)
ap2=average_precision_score(y,1-s)
print("[orientation] PR-AUC(s)   =", round(ap1,6))
print("[orientation] PR-AUC(1-s) =", round(ap2,6))
print("[orientation] choose =", "flip" if ap2>ap1 else "orig")
