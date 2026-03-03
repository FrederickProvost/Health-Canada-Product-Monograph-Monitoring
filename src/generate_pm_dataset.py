import pandas as pd
from pathlib import Path

# ==============================
# 🔹 SIMULATION / SOURCE DES DONNÉES
# Remplace cette section par TON extraction réelle
# ==============================
# Exemple de structure attendue
results = pd.DataFrame({
    "Drug_code": ["89926", "67042", "81234"],
    "PM_Update_Date": ["2026-02-28", "2026-03-01", "2026-02-27"]
})

# ==============================
# 🔹 NORMALISATION / CONTRÔLES
# ==============================
results = results.rename(columns={
    "Drug_code": "drug_code",
    "PM_Update_Date": "pm_update_date"
})

# Conversion types
results["drug_code"] = results["drug_code"].astype(str)
results["pm_update_date"] = pd.to_datetime(
    results["pm_update_date"],
    errors="coerce"
).dt.date

# Suppression lignes invalides
results = results.dropna(subset=["drug_code", "pm_update_date"])

# Déduplication (1 ligne par drug_code, dernière date)
results = (
    results
    .sort_values("pm_update_date")
    .drop_duplicates(subset=["drug_code"], keep="last")
    .sort_values("drug_code")
)

# ==============================
# 🔹 ÉCRITURE DU DATASET
# ==============================
output_dir = Path("data")
output_dir.mkdir(exist_ok=True)

output_file = output_dir / "drug_pm_updates.csv"
results.to_csv(output_file, index=False)

print("✅ Dataset généré :", output_file.resolve())
print(results)
