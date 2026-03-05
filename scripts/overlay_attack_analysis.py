import pandas as pd
from sklearn.metrics import average_precision_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

df = pd.read_parquet("mycelium_overlay_dataset.parquet")

X = df.select_dtypes(include="number").drop(columns=["label"])
y = df["label"]
attack = df["attack_type"]

X_train, X_test, y_train, y_test, attack_train, attack_test = train_test_split(
    X, y, attack,
    test_size=0.3,
    random_state=42,
    stratify=y
)

model = RandomForestClassifier(n_estimators=200)
model.fit(X_train, y_train)

probs = model.predict_proba(X_test)[:,1]

df_test = pd.DataFrame({
    "attack": attack_test,
    "prob": probs,
    "label": y_test
})

print("\nAttack-type PR-AUC:\n")

for a in sorted(df_test.attack.unique()):

    sub = df_test[df_test.attack == a]

    if sub.label.sum() > 0:

        pr = average_precision_score(sub.label, sub.prob)

        print(f"{a:15s} PR-AUC = {pr:.3f}")
