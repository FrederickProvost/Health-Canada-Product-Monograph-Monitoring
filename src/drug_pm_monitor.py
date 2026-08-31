```python
import pandas as pd
from pathlib import Path
from datetime import datetime
import re
import requests
import time
import random

# ==============================
# CONFIG
# ==============================

DATA_DIR = Path("Data")
DATA_DIR.mkdir(exist_ok=True)

INPUT_EXCEL_CANDIDATES = [
    DATA_DIR / "Drugcode_a_verifier.xlsx",
    DATA_DIR / "Drugcode_à_vérifier.xlsx",
]

INPUT_EXCEL = next(
    (p for p in INPUT_EXCEL_CANDIDATES if p.exists()),
    INPUT_EXCEL_CANDIDATES[0]
)

DATASET_FILE = DATA_DIR / "drug_pm_updates.csv"
HISTORY_FILE = DATA_DIR / "dpd_pm_history.csv"

BASE_URL = (
    "https://health-products.canada.ca/"
    "dpd-bdpp/info?lang=en&code={code}"
)

# ==============================
# USER AGENTS
# ==============================

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)"
]

HEADERS = {
    "User-Agent": random.choice(USER_AGENTS),
    "Accept-Language": "en-CA,en;q=0.9,fr-CA;q=0.8"
}

# ==============================
# SESSION
# ==============================

session = requests.Session()
session.headers.update(HEADERS)


# ==============================
# 1️⃣ LECTURE EXCEL
# ==============================

if not INPUT_EXCEL.exists():
    raise FileNotFoundError(
        f"❌ Fichier introuvable : {INPUT_EXCEL}"
    )

print(f"📥 Lecture Excel : {INPUT_EXCEL}")

df_input = pd.read_excel(
    INPUT_EXCEL,
    engine="openpyxl"
)

if "Drug_code" not in df_input.columns:
    raise ValueError(
        "❌ La colonne 'Drug_code' est requise"
    )

df_input["Drug_code"] = (
    df_input["Drug_code"]
    .astype(str)
    .str.strip()
)

unique_codes = (
    df_input["Drug_code"]
    .dropna()
    .unique()
)

print(f"📊 Codes uniques à vérifier : {len(unique_codes)}")


# ==============================
# 2️⃣ FETCH ROBUSTE
# ==============================

def fetch_pm_date_from_dpd(
    drug_code: str,
    max_retries=5
):

    url = BASE_URL.format(code=drug_code)

    for attempt in range(
        1,
        max_retries + 1
    ):

        try:

            r = session.get(
                url,
                timeout=(20, 60)
            )

            # ------------------------------
            # SUCCESS
            # ------------------------------

            if r.status_code == 200:

                html = r.text

                patterns = [

                    r"Product\s+Monograph.*?"
                    r"Date[^0-9]*"
                    r"([0-9]{4}-[0-9]{2}-[0-9]{2})",

                    r"Veterinary.*?"
                    r"Monograph.*?"
                    r"Date[^0-9]*"
                    r"([0-9]{4}-[0-9]{2}-[0-9]{2})",

                    r"Monograph.*?"
                    r"Date[^0-9]*"
                    r"([0-9]{4}-[0-9]{2}-[0-9]{2})",
                ]

                for pat in patterns:

                    m = re.search(
                        pat,
                        html,
                        flags=(
                            re.IGNORECASE |
                            re.DOTALL
                        )
                    )

                    if m:
                        return (
                            m.group(1),
                            url,
                            "OK"
                        )

                # ------------------------------
                # NO ELECTRONIC PM
                # ------------------------------

                if re.search(
                    r"Electronic\s+product\s+monograph"
                    r"\s+is\s+not\s+available",
                    html,
                    flags=re.IGNORECASE
                ):

                    return (
                        None,
                        url,
                        "NO_E_PM"
                    )

                # ------------------------------
                # PAGE FOUND BUT DATE NOT FOUND
                # ------------------------------

                return (
                    None,
                    url,
                    "NOT_FOUND"
                )

            # ------------------------------
            # RETRYABLE HTTP ERRORS
            # ------------------------------

            elif r.status_code in [
                429,
                403,
                500,
                502,
                503
            ]:

                wait = attempt * 4

                print(
                    f"⚠️ Retry "
                    f"{attempt}/{max_retries} - "
                    f"{drug_code} "
                    f"(HTTP {r.status_code}) "
                    f"→ wait {wait}s"
                )

                time.sleep(wait)

            # ------------------------------
            # OTHER HTTP ERROR
            # ------------------------------

            else:

                return (
                    None,
                    url,
                    f"HTTP_{r.status_code}"
                )

        # ==============================
        # TIMEOUTS
        # ==============================

        except requests.exceptions.ConnectTimeout:

            wait = attempt * 5

            print(
                f"⏱️ ConnectTimeout "
                f"{drug_code} "
                f"→ retry {attempt} "
                f"wait {wait}s"
            )

            time.sleep(wait)

        except requests.exceptions.ReadTimeout:

            wait = attempt * 5

            print(
                f"⏱️ ReadTimeout "
                f"{drug_code} "
                f"→ retry {attempt} "
                f"wait {wait}s"
            )

            time.sleep(wait)

        # ==============================
        # OTHER REQUEST ERRORS
        # ==============================

        except requests.RequestException as e:

            return (
                None,
                url,
                f"REQUEST_ERR: {type(e).__name__}"
            )

    # ==============================
    # FINAL FALLBACK
    # ==============================

    print(
        f"❌ Failed after retries: "
        f"{drug_code}"
    )

    try:

        r = session.get(
            url,
            timeout=(20, 60)
        )

        if r.status_code == 200:

            m = re.search(
                r"([0-9]{4}-[0-9]{2}-[0-9]{2})",
                r.text
            )

            if m:

                return (
                    m.group(1),
                    url,
                    "OK_FALLBACK"
                )

    except Exception:
        pass

    return (
        None,
        url,
        "FAILED"
    )


# ==============================
# 3️⃣ LOOP AVEC THROTTLE
# ==============================

results = []

today = datetime.today().date()

for i, drug_code in enumerate(unique_codes):

    # Pause entre chaque requête
    time.sleep(2.5)

    # Pause supplémentaire toutes les 3 requêtes
    if i % 3 == 0 and i != 0:

        print(
            "⏸️ Pause anti-blocage"
        )

        time.sleep(8)

    print(
        f"🌍 Fetching : {drug_code}"
    )

    (
        pm_date_str,
        url,
        note
    ) = fetch_pm_date_from_dpd(
        drug_code
    )

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

df_current["drug_code"] = (
    df_current["drug_code"]
    .astype(str)
    .str.strip()
)

df_current["pm_update_date"] = (
    pd.to_datetime(
        df_current["pm_update_date"],
        errors="coerce"
    )
    .dt.date
)

df_current = (
    df_current
    .sort_values(
        [
            "drug_code",
            "pm_update_date"
        ]
    )
    .drop_duplicates(
        subset=["drug_code"],
        keep="last"
    )
)


# ==============================
# 5️⃣ CHARGEMENT HISTORIQUE
# ==============================

EXPECTED_COLS = [
    "drug_code",
    "pm_update_date",
    "detected_on",
    "dpd_url"
]

if HISTORY_FILE.exists():

    print(
        f"📂 Lecture historique : "
        f"{HISTORY_FILE}"
    )

    df_history = pd.read_csv(
        HISTORY_FILE,
        dtype={"drug_code": str}
    )

else:

    print(
        "📂 Aucun historique trouvé. "
        "Création d'un nouveau fichier."
    )

    df_history = pd.DataFrame(
        columns=EXPECTED_COLS
    )


# ==============================
# NORMALISATION HISTORIQUE
# ==============================

for col in EXPECTED_COLS:

    if col not in df_history.columns:

        df_history[col] = None


df_history["drug_code"] = (
    df_history["drug_code"]
    .astype(str)
    .str.strip()
)

df_history["pm_update_date"] = (
    pd.to_datetime(
        df_history["pm_update_date"],
        errors="coerce"
    )
    .dt.date
)

df_history["detected_on"] = (
    pd.to_datetime(
        df_history["detected_on"],
        errors="coerce"
    )
    .dt.date
)

df_current["drug_code"] = (
    df_current["drug_code"]
    .astype(str)
    .str.strip()
)


# ==============================
# 6️⃣ DERNIÈRE DATE CONNUE
# ==============================

if not df_history.empty:

    last_known = (

        df_history

        .dropna(
            subset=["drug_code"]
        )

        .sort_values(
            [
                "drug_code",
                "detected_on"
            ]
        )

        .drop_duplicates(
            subset=["drug_code"],
            keep="last"
        )

        [
            [
                "drug_code",
                "pm_update_date"
            ]
        ]

        .rename(
            columns={
                "pm_update_date":
                    "pm_update_date_old"
            }
        )
    )

else:

    last_known = pd.DataFrame(
        columns=[
            "drug_code",
            "pm_update_date_old"
        ]
    )


# ==============================
# 7️⃣ MERGE
# ==============================

merged = df_current.merge(
    last_known,
    on="drug_code",
    how="left"
)


# ==============================
# 8️⃣ DETECTION CHANGEMENT
# ==============================

merged["has_changed"] = (

    merged["pm_update_date"].notna()

    &

    (
        merged["pm_update_date_old"].isna()

        |

        (
            merged["pm_update_date"]
            !=
            merged["pm_update_date_old"]
        )
    )

).astype(int)


# ==============================
# 9️⃣ SAVE DATASET CURRENT
# ==============================

merged.to_csv(
    DATASET_FILE,
    index=False
)

print(
    f"✅ Dataset généré : "
    f"{DATASET_FILE}"
)


# ==============================
# 🔟 UPDATE HISTORIQUE
# ==============================

# IMPORTANT :
# On ajoute maintenant TOUS les résultats
# valides à l'historique, pas seulement
# les changements.

daily_history = merged[
    merged["pm_update_date"].notna()
].copy()


daily_history["detected_on"] = today


daily_history = daily_history[
    [
        "drug_code",
        "pm_update_date",
        "detected_on",
        "dpd_url"
    ]
]


# ==============================
# AJOUT À L'HISTORIQUE
# ==============================

if not daily_history.empty:

    df_history = pd.concat(
        [
            df_history,
            daily_history
        ],
        ignore_index=True
    )


# ==============================
# REMOVE DUPLICATES
# ==============================

df_history = (
    df_history
    .drop_duplicates(
        subset=[
            "drug_code",
            "pm_update_date",
            "detected_on"
        ],
        keep="last"
    )
)


# ==============================
# SAVE HISTORY
# ==============================

df_history.to_csv(
    HISTORY_FILE,
    index=False
)


print(
    f"✅ Historique mis à jour : "
    f"{HISTORY_FILE}"
)

print(
    f"   → {len(daily_history)} "
    f"checks ajoutés aujourd'hui"
)


# ==============================
# 1️⃣1️⃣ SUMMARY
# ==============================

print("\n📊 Résumé")

print(
    f"- Codes testés : "
    f"{df_current.shape[0]}"
)

print(
    f"- Dates trouvées : "
    f"{df_current['pm_update_date'].notna().sum()}"
)

print(
    f"- Changements : "
    f"{merged['has_changed'].sum()}"
)

print(
    f"- Historique total : "
    f"{len(df_history)} lignes"
)


# ==============================
# ERREURS
# ==============================

errors = df_current[
    df_current["fetch_status"]
    .str.contains(
        "ERR|FAILED",
        na=False
    )
]


if not errors.empty:

    print(
        "\n⚠️ Erreurs réseau :"
    )

    print(
        errors[
            [
                "drug_code",
                "fetch_status"
            ]
        ]
        .to_string(
            index=False
        )
    )

