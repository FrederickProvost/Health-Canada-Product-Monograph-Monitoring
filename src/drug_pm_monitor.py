import pandas as pd
from pathlib import Path
from datetime import datetime

# ==============================
# CONFIG
# ==============================
DATA_DIR = Path("Data")
INPUT_EXCEL = DATA_DIR / "Drugcode_a_verifier.xlsx"

DATASET_FILE = DATA_DIR / "drug_pm_updates.csv"
HISTORY_FILE = DATA_DIR / "dpd_pm_history.csv"

DATA_DIR.mkdir(exist_ok=True)

# ==============================
# 1️⃣ LECTURE DES DRUG CODES À VÉRIFIER
# ==============================
if not INPUT_EXCEL.exists():
    raise FileNotFoundError(f"❌ Fichier introuvable : {INPUT_EXCEL}")

df_input = pd.read_excel(INPUT_EXCEL)

if "Drug_code" not in df_input.columns:
    raise ValueError("❌ La colonne 'Drug_code' est requise dans le fichier Excel")

df_input["Drug_code"] = df_input["Drug_code"].astype(str)

# ==============================
# 2️⃣ EXTRACTION DES DATES PM
# ==============================
# 🔴 REMPLACE CE BLOC PAR TA LOGIQUE RÉELLE 🔴
# (API Health Canada, scraping, etc.)

results = []
today = datetime.today().date()

for drug_code in df_input["Drug_code"].unique():
    # ⚠️ SIMULATION
    pm_date = today

    results.append({
        "drug_code": drug_code,
        "pm_update_date": pm_date
    })

df_current = pd.DataFrame(results)

# ==============================
# 3️⃣ NORMALISATION / NETTOYAGE
# ==============================
df_current["drug_code"] = df_current["drug_code"].astype(str)
df_current["pm_update_date"] = pd.to_datetime(
    df_current["pm_update_date"],
    errors="coerce"
).dt.date

df_current = df_current.dropna(subset=["drug_code", "pm_update_date"])

# 1 ligne par drug_code (date la plus récente)
df_current = (
    df_current
    .sort_values("pm_update_date")
    .drop_duplicates(subset=["drug_code"], keep="last")
    .sort_values("drug_code")
)

# ==============================
# 4️⃣ SAUVEGARDE DU DATASET (POWER BI)
# ==============================
df_current.to_csv(DATASET_FILE, index=False)
print(f"✅ Dataset Power BI généré : {DATASET_FILE}")

# ==============================
# 5️⃣ GESTION DE L’HISTORIQUE (ROBUSTE)
# ==============================
EXPECTED_COLS = ["drug_code", "pm_update_date", "detected_on"]

if HISTORY_FILE.exists():
    df_history = pd.read_csv(HISTORY_FILE)

    # ✅ Cas : ancien format → on réinitialise proprement
    if not EXPECTED_COLS.issubset(df_history.columns):
        print("⚠️ Historique existant avec ancien format détecté → réinitialisation")
        df_history = pd.DataFrame(columns=EXPECTED_COLS)
else:
    df_history = pd.DataFrame(columns=EXPECTED_COLS)

# Normalisation types
df_history["drug_code"] = df_history["drug_code"].astype(str)
df_history["pm_update_date"] = pd.to_datetime(
    df_history["pm_update_date"], errors="coerce"
).dt.date
df_history["detected_on"] = pd.to_datetime(
    df_history["detected_on"], errors="coerce"
).dt.date

# ==============================
# Détection des changements
# ==============================
last_known = (
    df_history
    .sort_values("detected_on")
    .drop_duplicates("drug_code", keep="last")
)

merged = df_current.merge(
    last_known,
    on="drug_code",
    how="left",
    suffixes=("", "_old")
)

changed = merged[
    merged["pm_update_date"] != merged["pm_update_date_old"]
].copy()

changed["detected_on"] = today

new_history_rows = changed[[
    "drug_code",
    "pm_update_date",
    "detected_on"
]]

# Append et sauvegarde
df_history = pd.concat([df_history, new_history_rows], ignore_index=True)
df_history.to_csv(HISTORY_FILE, index=False)

print(f"✅ Historique mis à jour : {HISTORY_FILE}")

# ==============================
# 6️⃣ RÉSUMÉ
# ==============================
print("📊 Résumé exécution")
print(f"- Drug codes vérifiés : {df_current.shape[0]}")
print(f"- Changements détectés : {new_history_rows.shape[0]}")
