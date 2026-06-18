import pandas as pd
from pathlib import Path
from datetime import datetime
import re
import requests
import time

# ==============================
# CONFIG
# ==============================
DATA_DIR = Path("Data")
DATA_DIR.mkdir(exist_ok=True)

INPUT_EXCEL_CANDIDATES = [
    DATA_DIR / "Drugcode_a_verifier.xlsx",
    DATA_DIR / "Drugcode_à_vérifier.xlsx",
]

INPUT_EXCEL = next((p for p in INPUT_EXCEL_CANDIDATES if p.exists()), INPUT_EXCEL_CANDIDATES[0])

DATASET_FILE = DATA_DIR / "drug_pm_updates.csv"
HISTORY_FILE = DATA_DIR / "dpd_pm_history.csv"

BASE_URL = "https://health-products.canada.ca/dpd-bdpp/info?lang=en&code={code}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; PM-Monitor/1.0)",
    "Accept-Language": "en-CA,en;q=0.9,fr-CA;q=0.8"
}

# Session HTTP (important pour stabilité)
session = requests.Session()
session.headers.update(HEADERS)

# ==============================
# 1️⃣ LECTURE EXCEL
# ==============================
if not INPUT_EXCEL.exists():
    raise FileNotFoundError(f"❌ Fichier introuvable : {INPUT_EXCEL}")

print(f"📥 Lecture Excel: {INPUT_EXCEL}")

df_input = pd.read_excel(INPUT_EXCEL, engine="openpyxl")

if "Drug_code" not in df_input.columns:
    raise ValueError("❌ La colonne 'Drug_code' est requise")

df_input["Drug_code"] = df_input["Drug_code"].astype(str).str.strip()
unique_codes = df_input["Drug_code"].dropna().unique()

# ==============================
# 2️⃣ FETCH ROBUSTE
# ==============================
def fetch_pm_date_from_dpd(drug_code: str, max_retries=3):
    url = BASE_URL.format(code=drug_code)

    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, timeout=(10, 30))  # (connect, read)

            if r.status_code == 200:
                html = r.text

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

            elif r.status_code in [429, 403, 500, 502, 503]:
                wait = attempt * 2
                print(f"⚠️ Retry {attempt}/{max_retries} - {drug_code} (HTTP {r.status_code}) → wait {wait}s")
                time.sleep(wait)

            else:
                return (None, url, f"HTTP_{r.status_code}")

        except requests.exceptions.ConnectTimeout:
            wait = attempt * 3
            print(f"⏱️ ConnectTimeout {drug_code} → retry {attempt} wait {wait}s")
            time.sleep(wait)

        except requests.exceptions.ReadTimeout:
            wait = attempt * 3
            print(f"⏱️ ReadTimeout {drug_code} → retry {attempt} wait {wait}s")
            time.sleep(wait)

        except requests.RequestException as e:
            return (None, url, f"REQUEST_ERR: {type(e).__name__}")

    return (None, url, "FAILED_AFTER_RETRIES")

# ==============================
# 3️⃣ LOOP AVEC THROTTLE
# ==============================
results = []
today = datetime.today().date()

for i, drug_code in enumerate(unique_codes):

    # 🔥 throttle anti-blocage
    time.sleep(1.5)

    # pause supplémentaire toutes les 5 requêtes
    if i % 5 == 0 and i != 0:
        print("⏸️ Pause anti-blocage")
        time.sleep(5)

    print(f"🌍 Fetching: {drug_code}")

    pm_date_str, url, note = fetch_pm_date_from_dpd(drug_code)

    results.append({
        "drug_code": str(drug_code),
        "dpd_url": url,
        "pm_update_date": pm_date_str,
        "fetch_status": note,
        "checked_on": today
    })

df_current = pd.DataFrame(results)

# ==============================
# 4️⃣ NORMALISATION
# ==============================
df_current["pm_update_date"] = pd.to_datetime(
    df_current["pm_update_date"],
    errors="coerce"
).dt.date

df_current = (
    df_current
    .sort_values(["drug_code", "pm_update_date"])
    .drop_duplicates(subset=["drug_code"], keep="last")
)

# ==============================
# 5️⃣ HISTORIQUE
# ==============================
EXPECTED_COLS = ["drug_code", "pm_update_date", "detected_on", "dpd_url"]

if HISTORY_FILE.exists():
    df_history = pd.read_csv(HISTORY_FILE)

    if not set(EXPECTED_COLS).issubset(df_history.columns):
        print("⚠️ reset historique")
        df_history = pd.DataFrame(columns=EXPECTED_COLS)
else:
    df_history = pd.DataFrame(columns=EXPECTED_COLS)

df_history["pm_update_date"] = pd.to_datetime(df_history["pm_update_date"], errors="coerce").dt.date
df_history["detected_on"] = pd.to_datetime(df_history["detected_on"], errors="coerce").dt.date

last_known = (
    df_history
    .sort_values("detected_on")
    .drop_duplicates("drug_code", keep="last")
    .rename(columns={"pm_update_date": "pm_update_date_old"})
)

merged = df_current.merge(
    last_known[["drug_code", "pm_update_date_old"]],
    on="drug_code",
    how="left"
)

# ==============================
# 6️⃣ DETECTION CHANGEMENT
# ==============================
merged["has_changed"] = (
    merged["pm_update_date"].notna() &
    (
        merged["pm_update_date_old"].isna() |
        (merged["pm_update_date"] != merged["pm_update_date_old"])
    )
).astype(int)

# ==============================
# 7️⃣ SAVE DATASET
# ==============================
merged.to_csv(DATASET_FILE, index=False)
print(f"✅ Dataset généré : {DATASET_FILE}")

# ==============================
# 8️⃣ UPDATE HISTORIQUE
# ==============================
changed = merged[merged["has_changed"] == 1].copy()
changed["detected_on"] = today

df_history = pd.concat([
    df_history,
    changed[["drug_code", "pm_update_date", "detected_on", "dpd_url"]]
], ignore_index=True)

df_history.to_csv(HISTORY_FILE, index=False)

print(f"✅ Historique mis à jour : {HISTORY_FILE}")

# ==============================
# 9️⃣ SUMMARY
# ==============================
print("\n📊 Résumé")
print(f"- Codes testés : {df_current.shape[0]}")
print(f"- Dates trouvées : {df_current['pm_update_date'].notna().sum()}")
print(f"- Changements : {merged['has_changed'].sum()}")

errors = df_current[df_current["fetch_status"].str.contains("ERR|FAILED", na=False)]
if not errors.empty:
    print("\n⚠️ Erreurs réseau :")
    print(errors[["drug_code", "fetch_status"]].to_string(index=False))
