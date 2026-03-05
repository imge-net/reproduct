import pandas as pd
from sklearn.metrics import average_precision_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GroupShuffleSplit

df = pd.read_parquet("mycelium_overlay_dataset_v2.parquet")

X = df.select_dtypes(include="number").drop(columns=["label","iteration"])
y = df["label"]
groups = df["iteration"]

gss = GroupShuffleSplit(test_size=0.3, n_splits=1, random_state=42)

train_idx, test_idx = next(gss.split(X, y, groups))

X_train = X.iloc[train_idx]
y_train = y.iloc[train_idx]

X_test = X.iloc[test_idx]
y_test = y.iloc[test_idx]

print("train size:",len(X_train))
print("test size:",len(X_test))
print("train attacks:",y_train.sum())
print("test attacks:",y_test.sum())

model = RandomForestClassifier(n_estimators=200)

model.fit(X_train, y_train)

p = model.predict_proba(X_test)[:,1]

pr = average_precision_score(y_test, p)

print("\nOverlay PR-AUC:",pr)
