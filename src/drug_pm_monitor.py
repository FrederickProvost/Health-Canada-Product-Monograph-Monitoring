# -*- coding: utf-8 -*-
"""
Health Canada DPD - Product Monograph monitoring
- Input:  Drugcode_a_verifier.xlsx (colonne: Drug_code)
- Output: Data/drug_pm_updates.csv (avec has_changed)
- History: Data/dpd_pm_history.csv (drug_code, pm_update_date, detected_on, dpd_url)

Requirements:
  pip install pandas openpyxl requests beautifulsoup4 lxml
"""

from __future__ import annotations

import re
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional, Tuple, List

import pandas as pd
import requests
from bs4 import BeautifulSoup


# -----------------------------
# Configuration
# -----------------------------
ROOT = Path(__file__).resolve().parents[1]  # repo root assuming src/drug_pm_monitor.py
DATA_DIR = ROOT / "Data"
DATA_DIR.mkdir(exist_ok=True)

INPUT_EXCEL = ROOT / "Drugcode_a_verifier.xlsx"
HISTORY_CSV = DATA_DIR / "dpd_pm_history.csv"
OUTPUT_UPDATES_CSV = DATA_DIR / "drug_pm_updates.csv"

# DPD page template (works with drug code)
DPD_URL_TEMPLATE = "https://health-products.canada.ca/dpd-bdpp/info?lang=en&code={drug_code}"

# Requests settings
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT = 30
RETRY = 3
SLEEP_BETWEEN = 0.6  # be polite


# -----------------------------
# Helpers
# -----------------------------
def normalize_drug_code(x: str) -> str:
    """Keep digits only; returns as string."""
    if x is None:
        return ""
    s = str(x).strip()
    s = re.sub(r"[^\d]", "", s)
    return s


def normalize_date_string(s: str) -> str:
    """
    Normalize date string to ISO 'YYYY-MM-DD' if possible.
    Accepts formats like:
      - 2026-03-04
      - 2026/03/04
      - 2013 11 01
      - 2013-11-01
      - 01/11/2013 (will try)
      - March 4, 2026 (will try)
    If can't parse reliably, returns cleaned original.
    """
    if not s:
        return ""

    s0 = " ".join(str(s).strip().split())
    s0 = s0.replace(".", "").replace(",", "")

    # Common numeric patterns
    # yyyy-mm-dd / yyyy/mm/dd / yyyy mm dd
    m = re.search(r"\b(20\d{2})[\/\-\s](\d{1,2})[\/\-\s](\d{1,2})\b", s0)
    if m:
        y, mo, d = m.group(1), int(m.group(2)), int(m.group(3))
        return f"{y}-{mo:02d}-{d:02d}"

    # dd/mm/yyyy or mm/dd/yyyy (ambiguous) => try day-first if day > 12
    m = re.search(r"\b(\d{1,2})[\/\-\s](\d{1,2})[\/\-\s](20\d{2})\b", s0)
    if m:
        a, b, y = int(m.group(1)), int(m.group(2)), m.group(3)
        # Heuristic:
        # if a > 12 => a is day
        # elif b > 12 => b is day (so a is month)
        # else default to day-first (Canada)
        if a > 12:
            d, mo = a, b
        elif b > 12:
            mo, d = a, b
        else:
            d, mo = a, b
        return f"{y}-{mo:02d}-{d:02d}"

    # Month name patterns
    # e.g., March 4 2026 / 4 March 2026
    month_map = {
        "jan": 1, "january": 1,
        "feb": 2, "february": 2,
        "mar": 3, "march": 3,
        "apr": 4, "april": 4,
        "may": 5,
        "jun": 6, "june": 6,
        "jul": 7, "july": 7,
        "aug": 8, "august": 8,
        "sep": 9, "sept": 9, "september": 9,
        "oct": 10, "october": 10,
        "nov": 11, "november": 11,
        "dec": 12, "december": 12,
    }
    s_lower = s0.lower()
    # March 4 2026
    m = re.search(r"\b([a-z]{3,9})\s+(\d{1,2})\s+(20\d{2})\b", s_lower)
    if m and m.group(1) in month_map:
        mo = month_map[m.group(1)]
        d = int(m.group(2))
        y = m.group(3)
        return f"{y}-{mo:02d}-{d:02d}"
    # 4 March 2026
    m = re.search(r"\b(\d{1,2})\s+([a-z]{3,9})\s+(20\d{2})\b", s_lower)
    if m and m.group(2) in month_map:
        d = int(m.group(1))
        mo = month_map[m.group(2)]
        y = m.group(3)
        return f"{y}-{mo:02d}-{d:02d}"

    return s0


def request_with_retry(url: str) -> str:
    headers = {"User-Agent": USER_AGENT}
    last_err = None
    for i in range(1, RETRY + 1):
        try:
            r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            time.sleep(1.2 * i)
    raise RuntimeError(f"Failed to fetch {url} after {RETRY} tries. Last error: {last_err}")


def extract_pm_update_date_from_html(html: str) -> Optional[str]:
    """
    Attempt to extract PM update date from DPD info page HTML.

    Strategy:
    1) Parse text with BeautifulSoup; search around keywords.
    2) Use multiple regex patterns to catch dates near "Product Monograph" / "Veterinary Product Monograph"
       and/or "Date" labels.
    Returns ISO-like date string if found, else None.
    """
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    if not text:
        return None

    # Common keywords on DPD pages
    keywords = [
        "Product Monograph",
        "Veterinary Product Monograph",
        "Product monograph",
        "Veterinary product monograph",
        "Monograph",
        "Monographie",
        "Monographie de produit",
    ]

    # Candidate date regex (handles yyyy-mm-dd, yyyy/mm/dd, dd/mm/yyyy, Month dd yyyy)
    date_regex = r"((?:20\d{2}[\/\-\s]\d{1,2}[\/\-\s]\d{1,2})|(?:\d{1,2}[\/\-\s]\d{1,2}[\/\-\s]20\d{2})|(?:[A-Za-z]{3,9}\s+\d{1,2}\s+20\d{2})|(?:\d{1,2}\s+[A-Za-z]{3,9}\s+20\d{2}))"

    # 1) Try: keyword within N chars then date
    for kw in keywords:
        pattern = rf"{re.escape(kw)}(.{{0,120}}?){date_regex}"
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            raw_date = m.group(2)
            return normalize_date_string(raw_date)

    # 2) Try: "Date" near monograph mention
    # E.g., "... Product Monograph Date 2026-03-04 ..."
    pattern = rf"(Product\s+Monograph|Veterinary\s+Product\s+Monograph).{{0,80}}?(Date|Updated|Update|Date\s+de).{{0,40}}?{date_regex}"
    m = re.search(pattern, text, flags=re.IGNORECASE)
    if m:
        raw_date = m.group(3)
        return normalize_date_string(raw_date)

    # 3) Fallback: any date near "Monograph"
    pattern = rf"(Monograph|Monographie).{{0,80}}?{date_regex}"
    m = re.search(pattern, text, flags=re.IGNORECASE)
    if m:
        raw_date = m.group(2)
        return normalize_date_string(raw_date)

    return None


# -----------------------------
# Core logic
# -----------------------------
def load_drug_codes_from_excel(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"Input Excel not found: {path}")

    df = pd.read_excel(path, engine="openpyxl", dtype=str)
    # Accept common column names
    possible_cols = ["Drug_code", "drug_code", "Drug Code", "DRUG_CODE"]
    col = next((c for c in possible_cols if c in df.columns), None)
    if col is None:
        raise ValueError(
            f"Excel must contain a column named one of: {possible_cols}. Found: {list(df.columns)}"
        )

    codes = [normalize_drug_code(x) for x in df[col].tolist()]
    codes = [c for c in codes if c]
    # Unique, preserve order
    seen = set()
    uniq = []
    for c in codes:
        if c not in seen:
            seen.add(c)
            uniq.append(c)
    return uniq


def load_history(path: Path) -> pd.DataFrame:
    if path.exists():
        df = pd.read_csv(path, dtype=str)
        # Ensure expected columns exist
        expected = ["drug_code", "pm_update_date", "detected_on", "dpd_url"]
        for c in expected:
            if c not in df.columns:
                # If older format, add missing columns
                df[c] = ""
        df = df[expected].copy()
        df["drug_code"] = df["drug_code"].apply(normalize_drug_code)
        df["pm_update_date"] = df["pm_update_date"].fillna("").astype(str)
        return df
    else:
        return pd.DataFrame(columns=["drug_code", "pm_update_date", "detected_on", "dpd_url"])


def build_current_snapshot(drug_codes: List[str]) -> pd.DataFrame:
    rows = []
    today = date.today().isoformat()

    for idx, code in enumerate(drug_codes, start=1):
        dpd_url = DPD_URL_TEMPLATE.format(drug_code=code)
        print(f"Progress: {idx}/{len(drug_codes)} | fetching {code} ...")

        pm_date = ""
        try:
            html = request_with_retry(dpd_url)
            found = extract_pm_update_date_from_html(html)
            pm_date = found or ""
        except Exception as e:
            print(f"  ⚠️ Error fetching/parsing drug_code={code}: {e}", file=sys.stderr)

        rows.append(
            {
                "drug_code": code,
                "pm_update_date": pm_date,
                "detected_on": today,
                "dpd_url": dpd_url,
            }
        )
        time.sleep(SLEEP_BETWEEN)

    df = pd.DataFrame(rows)
    # Normalize date field
    df["pm_update_date"] = df["pm_update_date"].fillna("").apply(normalize_date_string)
    return df


def add_has_changed(df_current: pd.DataFrame, df_history: pd.DataFrame) -> pd.DataFrame:
    """
    Merge current snapshot with history on drug_code and compute has_changed.
    - has_changed = 1 if old pm_update_date exists and differs from current pm_update_date
    - else 0
    """
    hist_small = df_history[["drug_code", "pm_update_date"]].copy()
    hist_small.rename(columns={"pm_update_date": "pm_update_date_old"}, inplace=True)

    df = df_current.merge(hist_small, on="drug_code", how="left")

    df["pm_update_date_old"] = df["pm_update_date_old"].fillna("").astype(str)
    df["pm_update_date"] = df["pm_update_date"].fillna("").astype(str)

    df["has_changed"] = (
        (df["pm_update_date_old"] != "") &
        (df["pm_update_date"] != "") &
        (df["pm_update_date"] != df["pm_update_date_old"])
    ).astype(int)

    # Optional: if current is blank but old exists, you may want has_changed=0 (default here)
    return df


def main():
    print(f"📥 Lecture Excel: {INPUT_EXCEL}")
    drug_codes = load_drug_codes_from_excel(INPUT_EXCEL)
    print(f"✅ {len(drug_codes)} drug_code à vérifier")

    df_history = load_history(HISTORY_CSV)
    if len(df_history) > 0:
        print(f"📚 Historique chargé: {HISTORY_CSV} ({len(df_history)} lignes)")
    else:
        print(f"📚 Aucun historique trouvé, création: {HISTORY_CSV}")

    # Build snapshot from DPD
    df_current = build_current_snapshot(drug_codes)

    # Compute has_changed
    df_comp = add_has_changed(df_current, df_history)

    # Create updates file for Power BI
    df_updates = df_comp[["drug_code", "pm_update_date", "dpd_url", "has_changed"]].copy()
    df_updates.to_csv(OUTPUT_UPDATES_CSV, index=False, encoding="utf-8")
    print(f"✅ Dataset Power BI généré : {OUTPUT_UPDATES_CSV}")

    # Update history (store latest known values)
    df_history_new = df_current[["drug_code", "pm_update_date", "detected_on", "dpd_url"]].copy()
    df_history_new.to_csv(HISTORY_CSV, index=False, encoding="utf-8")
    print(f"✅ Historique mis à jour : {HISTORY_CSV}")

    # Summary
    changed = int(df_updates["has_changed"].sum())
    print(f"📌 Changements détectés: {changed}")
    if changed > 0:
        print("🔎 Liste des drug_code modifiés:")
        print(df_updates.loc[df_updates["has_changed"] == 1, ["drug_code", "pm_update_date", "dpd_url"]].to_string(index=False))


if __name__ == "__main__":
    main()
