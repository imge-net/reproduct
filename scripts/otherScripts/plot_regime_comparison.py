import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import roc_curve, precision_recall_curve

# Dummy curves for visualization (replace with real arrays if available)

x = np.linspace(0,1,200)

roc_unsw = x**0.2
roc_cicids = x**0.5
roc_myc = x**0.8

plt.figure(figsize=(6,5))
plt.plot(x,roc_unsw,label="UNSW")
plt.plot(x,roc_cicids,label="CICIDS day-shift")
plt.plot(x,roc_myc,label="Mycelium overlay")

plt.xlabel("FPR")
plt.ylabel("TPR")
plt.legend()
plt.tight_layout()
plt.savefig("fig_regime_roc.pdf")
