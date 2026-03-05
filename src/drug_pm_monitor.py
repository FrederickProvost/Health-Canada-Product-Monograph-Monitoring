import pandas as pd
from pathlib import Path
from datetime import datetime
import re
import requests

# ==============================
# CONFIG
# ==============================
DATA_DIR = Path("Data")
DATA_DIR.mkdir(exist_ok=True)

# ✅ IMPORTANT: ton repo contient [Drugcode_à_vérifier.xlsx](https://mantrapharma-my.sharepoint.com/personal/fprovost_mantrapharma_ca/_layouts/15/Doc.aspx?sourcedoc=%7BF391D01C-BB7B-4F72-9E8B-E0F4EFB2D04B%7D&file=Drugcode_%C3%A0_v%C3%A9rifier.xlsx&action=default&mobileredirect=true&DefaultItemOpen=1&EntityRepresentationId=01b78b33-9ec0-4ea6-9a8f-73c67409138d) (accent) [1](https://mantrapharma-my.sharepoint.com/personal/fprovost_mantrapharma_ca/_layouts/15/Doc.aspx?sourcedoc=%7BF391D01C-BB7B-4F72-9E8B-E0F4EFB2D04B%7D&file=Drugcode_%C3%A0_v%C3%A9rifier.xlsx&action=default&mobileredirect=true&DefaultItemOpen=1)
# Pour être robuste (accent/no accent), on essaie les 2 noms.
INPUT_EXCEL_CANDIDATES = [
    DATA_DIR / "Drugcode_a_verifier.xlsx",
    DATA_DIR / "Drugcode_à_vérifier.xlsx",
]
INPUT_EXCEL = next((p for p in INPUT_EXCEL_CANDIDATES if p.exists()), INPUT_EXCEL_CANDIDATES[0])

DATASET_FILE = DATA_DIR / "drug_pm_updates.csv"
HISTORY_FILE = DATA_DIR / "dpd_pm_history.csv"

# ✅ Corriger &amp; → &
BASE_URL = "https://health-products.canada.ca/dpd-bdpp/info?lang=en&code={code}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; PM-Monitor/1.0; +https://health-products.canada.ca/)",
    "Accept-Language": "en-CA,en;q=0.9,fr-CA;q=0.8,fr;q=0.7"
}

TIMEOUT = 30


# ==============================
# 1️⃣ LECTURE DES DRUG CODES À VÉRIFIER
# ==============================
if not INPUT_EXCEL.exists():
    raise FileNotFoundError(f"❌ Fichier introuvable : {INPUT_EXCEL} (essayés: {', '.join([str(p) for p in INPUT_EXCEL_CANDIDATES])})")

print(f"📥 Lecture Excel: {INPUT_EXCEL}")

df_input = pd.read_excel(INPUT_EXCEL, engine="openpyxl")

if "Drug_code" not in df_input.columns:
    raise ValueError("❌ La colonne 'Drug_code' est requise dans le fichier Excel")

df_input["Drug_code"] = df_input["Drug_code"].astype(str).str.strip()
unique_codes = df_input["Drug_code"].dropna().unique()


# ==============================
# 2️⃣ EXTRACTION DES DATES PM (DPD page)
# ==============================
def fetch_pm_date_from_dpd(drug_code: str):
    url = BASE_URL.format(code=drug_code)

    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code != 200:
            return (None, url, f"HTTP_{r.status_code}")

        html = r.text

        # ✅ Regex (plus permissive) pour attraper YYYY-MM-DD proche de "Product Monograph" (ou Veterinary)
        # Note: la page peut contenir uniquement Product Monograph (pas forcément "Veterinary")
        patterns = [
            r"Product\s+Monograph.*?Date[^0-9]*([0-9]{4}-[0-9]{2}-[0-9]{2})",
            r"Veterinary.*?Monograph.*?Date[^0-9]*([0-9]{4}-[0-9]{2}-[0-9]{2})",
            r"Monograph.*?Date[^0-9]*([0-9]{4}-[0-9]{2}-[0-9]{2})",
        ]

        for pat in patterns:
            m = re.search(pat, html, flags=re.IGNORECASE | re.DOTALL)
            if m:
                return (m.group(1), url, "OK")

        if re.search(r"Electronic\s+product\s+monograph\s+is\s+not\s+available", html, flags=re.IGNORECASE):
            return (None, url, "NO_E_PM")

        return (None, url, "NOT_FOUND")

    except requests.RequestException as e:
        return (None, url, f"REQUEST_ERR: {type(e).__name__}")


results = []
today = datetime.today().date()

for drug_code in unique_codes:
    pm_date_str, url, note = fetch_pm_date_from_dpd(drug_code)

    results.append({
        "drug_code": str(drug_code).strip(),
        "dpd_url": url,
        "pm_update_date": pm_date_str,  # string YYYY-MM-DD ou None
        "fetch_status": note,
        "checked_on": today
    })

df_current = pd.DataFrame(results)


# ==============================
# 3️⃣ NORMALISATION / NETTOYAGE
# ==============================
df_current["drug_code"] = df_current["drug_code"].astype(str).str.strip()

df_current["pm_update_date"] = pd.to_datetime(
    df_current["pm_update_date"],
    errors="coerce"
).dt.date

# 1 ligne par drug_code (si jamais doublons)
df_current = (
    df_current
    .sort_values(["drug_code", "pm_update_date"])
    .drop_duplicates(subset=["drug_code"], keep="last")
    .sort_values("drug_code")
)


# ==============================
# 4️⃣ GESTION DE L’HISTORIQUE (ROBUSTE)
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
df_history["drug_code"] = df_history["drug_code"].astype(str).str.strip()

df_history["pm_update_date"] = pd.to_datetime(
    df_history["pm_update_date"], errors="coerce"
).dt.date

df_history["detected_on"] = pd.to_datetime(
    df_history["detected_on"], errors="coerce"
).dt.date

# Dernière valeur connue par drug_code (la plus récente)
last_known = (
    df_history
    .sort_values("detected_on")
    .drop_duplicates("drug_code", keep="last")
    .rename(columns={"pm_update_date": "pm_update_date_old"})
)

# Merge avec current
merged = df_current.merge(
    last_known[["drug_code", "pm_update_date_old"]],
    on="drug_code",
    how="left"
)

# ==============================
# ✅ has_changed : 1 si on a une date actuelle ET qu'elle diffère de l'ancienne (ou si nouvelle entrée)
# - NEW : pas d'ancienne date (pm_update_date_old est NaN) et pm_update_date existe
# - UPDATED : ancienne date existe et différente
# - sinon 0
# ==============================
merged["has_changed"] = (
    merged["pm_update_date"].notna() &
    (
        merged["pm_update_date_old"].isna() |
        (merged["pm_update_date"] != merged["pm_update_date_old"])
    )
).astype(int)

# ==============================
# 5️⃣ SAUVEGARDE DU DATASET (POWER BI) AVEC has_changed ✅
# ==============================
df_dataset = merged.copy()
df_dataset.to_csv(DATASET_FILE, index=False)
print(f"✅ Dataset Power BI généré : {DATASET_FILE}")

# ==============================
# 6️⃣ Mise à jour de l’historique (on ne logue que NEW/UPDATED, et seulement si date présente)
# ==============================
changed = merged[merged["has_changed"] == 1].copy()
changed["detected_on"] = today

new_history_rows = changed[[
    "drug_code",
    "pm_update_date",
    "detected_on",
    "dpd_url"
]]

df_history = pd.concat([df_history, new_history_rows], ignore_index=True)
df_history.to_csv(HISTORY_FILE, index=False)

print(f"✅ Historique mis à jour : {HISTORY_FILE}")

# ==============================
# 7️⃣ RÉSUMÉ
# ==============================
print("📊 Résumé exécution")
print(f"- Drug codes vérifiés : {df_current.shape[0]}")
print(f"- Dates PM trouvées : {df_current['pm_update_date'].notna().sum()}")
print(f"- Changements détectés (has_changed=1) : {int(df_dataset['has_changed'].sum())}")

missing = df_current[df_current["pm_update_date"].isna()][["drug_code","fetch_status","dpd_url"]]
if not missing.empty:
    print("\n⚠️ Codes sans date PM trouvée (à investiguer) :")
    print(missing.to_string(index=False))

# Optionnel: afficher les modifiés
mods = df_dataset[df_dataset["has_changed"] == 1][["drug_code", "pm_update_date", "dpd_url"]]
if not mods.empty:
    print("\n🔔 Drug codes modifiés (has_changed=1) :")
    print(mods.to_string(index=False))
