import pandas as pd
import matplotlib.pyplot as plt

# UNSW dataset
unsw = pd.read_parquet("runs/20260225_225210/unsw_gbdt.parquet")

# overlay dataset
overlay = pd.read_parquet("mycelium_overlay_dataset_v2.parquet")

features = ["dur","spkts","dpkts","sbytes","dbytes"]

plt.figure(figsize=(10,6))

for i,f in enumerate(features):

    plt.subplot(2,3,i+1)

    plt.hist(unsw[f], bins=50, alpha=0.5, density=True, label="UNSW")
    plt.hist(overlay[f], bins=50, alpha=0.5, density=True, label="Mycelium")

    plt.title(f)

plt.legend()
plt.tight_layout()

plt.savefig("feature_drift_overlay.pdf")

print("saved: feature_drift_overlay.pdf")
