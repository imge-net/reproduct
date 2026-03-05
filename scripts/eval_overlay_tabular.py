import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import average_precision_score
from sklearn.ensemble import RandomForestClassifier
from lightgbm import LGBMClassifier

df = pd.read_parquet("mycelium_overlay_dataset.parquet")

X = df.select_dtypes(include="number").drop(columns=["label"])
y = df["label"]

X_train,X_test,y_train,y_test=train_test_split(X,y,test_size=0.3,random_state=42)

models = {
    "rf":RandomForestClassifier(n_estimators=200),
    "lgbm":LGBMClassifier()
}

for name,m in models.items():

    m.fit(X_train,y_train)

    p = m.predict_proba(X_test)[:,1]

    auc = average_precision_score(y_test,p)

    print(name,"PR-AUC approx:",auc)
