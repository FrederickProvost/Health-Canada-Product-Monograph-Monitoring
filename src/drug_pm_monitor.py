import pandas as pd
from pathlib import Path
from datetime import datetime
import re
import requests

# ==============================
# CONFIG
# ==============================
DATA_DIR = Path("Data")
INPUT_EXCEL = DATA_DIR / "Drugcode_a_verifier.xlsx"

DATASET_FILE = DATA_DIR / "drug_pm_updates.csv"
HISTORY_FILE = DATA_DIR / "dpd_pm_history.csv"

DATA_DIR.mkdir(exist_ok=True)

BASE_URL = "https://health-products.canada.ca/dpd-bdpp/info?lang=eng&code={code}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; PM-Monitor/1.0; +https://health-products.canada.ca/)",
    "Accept-Language": "en-CA,en;q=0.9,fr-CA;q=0.8,fr;q=0.7"
}

TIMEOUT = 30

# ==============================
# 1️⃣ LECTURE DES DRUG CODES À VÉRIFIER
# ==============================
if not INPUT_EXCEL.exists():
    raise FileNotFoundError(f"❌ Fichier introuvable : {INPUT_EXCEL}")

df_input = pd.read_excel(INPUT_EXCEL, engine="openpyxl")

if "Drug_code" not in df_input.columns:
    raise ValueError("❌ La colonne 'Drug_code' est requise dans le fichier Excel")

df_input["Drug_code"] = df_input["Drug_code"].astype(str).str.strip()

# ==============================
# 2️⃣ EXTRACTION DES DATES PM (DPD page)
# ==============================
def fetch_pm_date_from_dpd(drug_code: str):
    """
    Va sur la page DPD 'Product information' et extrait
    la date associée à 'Product Monograph/Veterinary Labelling: Date: YYYY-MM-DD'
    Retourne (pm_date_str, url, status_note)
    """
    url = BASE_URL.format(code=drug_code)

    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code != 200:
            return (None, url, f"HTTP_{r.status_code}")

        html = r.text

        # Cherche le pattern "Product Monograph/Veterinary Labelling: Date: YYYY-MM-DD"
        # (tolérant aux espaces / retours ligne)
        m = re.search(
            r"Product\s+Monograph.*?Veterinary.*?Date[^0-9]*([0-9]{4}-[0-9]{2}-[0-9]{2})",
            html,
            flags=re.IGNORECASE
        )
        if m:
            return (m.group(1), url, "OK")

        # Certains produits affichent "Electronic product monograph is not available"
        if re.search(r"Electronic\s+product\s+monograph\s+is\s+not\s+available", html, flags=re.IGNORECASE):
            return (None, url, "NO_E_PM")

        # Sinon : contenu inattendu / pas trouvé
        return (None, url, "NOT_FOUND")

    except requests.RequestException as e:
        return (None, url, f"REQUEST_ERR: {type(e).__name__}")


results = []
today = datetime.today().date()

unique_codes = df_input["Drug_code"].dropna().unique()

for drug_code in unique_codes:
    pm_date_str, url, note = fetch_pm_date_from_dpd(drug_code)

    results.append({
        "drug_code": drug_code,
        "dpd_url": url,
        "pm_update_date": pm_date_str,  # string YYYY-MM-DD ou None
        "fetch_status": note,
        "checked_on": today
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

# On garde aussi les lignes sans date (utile pour debug), mais tu peux filtrer si tu veux.
# df_current = df_current.dropna(subset=["drug_code", "pm_update_date"])

# 1 ligne par drug_code (si jamais doublons)
df_current = (
    df_current
    .sort_values(["drug_code", "pm_update_date"])
    .drop_duplicates(subset=["drug_code"], keep="last")
    .sort_values("drug_code")
)

# ==============================
# 4️⃣ SAUVEGARDE DU DATASET (POWER BI)
# ==============================
# Dataset principal (inclut url + statut + checked_on)
df_current.to_csv(DATASET_FILE, index=False)
print(f"✅ Dataset Power BI généré : {DATASET_FILE}")

# ==============================
# 5️⃣ GESTION DE L’HISTORIQUE (ROBUSTE)
# ==============================
EXPECTED_COLS = ["drug_code", "pm_update_date", "detected_on", "dpd_url"]

if HISTORY_FILE.exists():
    df_history = pd.read_csv(HISTORY_FILE)

    if not set(EXPECTED_COLS).issubset(df_history.columns):
        print("⚠️ Historique existant avec ancien format détecté → réinitialisation")
        df_history = pd.DataFrame(columns=EXPECTED_COLS)
else:
    df_history = pd.DataFrame(columns=EXPECTED_COLS)

# Normalisation des types
df_history["drug_code"] = df_history["drug_code"].astype(str)

df_history["pm_update_date"] = pd.to_datetime(
    df_history["pm_update_date"], errors="coerce"
).dt.date

df_history["detected_on"] = pd.to_datetime(
    df_history["detected_on"], errors="coerce"
).dt.date

# ==============================
# Détection des changements (vs dernière date connue)
# ==============================
last_known = (
    df_history
    .sort_values("detected_on")
    .drop_duplicates("drug_code", keep="last")
)

merged = df_current.merge(
    last_known[["drug_code", "pm_update_date", "detected_on", "dpd_url"]],
    on="drug_code",
    how="left",
    suffixes=("", "_old")
)

# Détecte :
# - UPDATED : pm_update_date différente
# - NEW : pas de date précédente (pm_update_date_old est NaN)
# On ignore les cas où pm_update_date est NaN (pas de date trouvée), pour éviter de loguer du bruit
changed = merged[
    merged["pm_update_date"].notna() &
    (
        merged["pm_update_date_old"].isna() |
        (merged["pm_update_date"] != merged["pm_update_date_old"])
    )
].copy()

changed["detected_on"] = today

new_history_rows = changed[[
    "drug_code",
    "pm_update_date",
    "detected_on",
    "dpd_url"
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
print(f"- Dates PM trouvées : {df_current['pm_update_date'].notna().sum()}")
print(f"- Changements logués (NEW/UPDATED) : {new_history_rows.shape[0]}")

# Optionnel: affiche les codes sans date (debug)
missing = df_current[df_current["pm_update_date"].isna()][["drug_code","fetch_status","dpd_url"]]
if not missing.empty:
    print("\n⚠️ Codes sans date PM trouvée (à investiguer) :")
    print(missing.to_string(index=False))
