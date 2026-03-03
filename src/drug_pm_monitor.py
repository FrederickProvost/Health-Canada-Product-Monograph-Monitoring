import os
import re
import time
import requests
import pandas as pd
import smtplib
from email.message import EmailMessage
from datetime import datetime

# =========================
# PATHS (repo-friendly)
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))

DATA_DIR = os.path.join(REPO_ROOT, "data")
os.makedirs(DATA_DIR, exist_ok=True)

INPUT_XLSX = os.environ.get("INPUT_XLSX", os.path.join(DATA_DIR, "Drugcode_a_verifier.xlsx"))
INPUT_SHEET = os.environ.get("INPUT_SHEET", "0")
INPUT_COLUMN = os.environ.get("INPUT_COLUMN", "Drug_code")

HISTORY_CSV = os.environ.get("HISTORY_CSV", os.path.join(DATA_DIR, "dpd_pm_history.csv"))
CURRENT_CSV = os.environ.get("CURRENT_CSV", os.path.join(DATA_DIR, "dpd_pm_current.csv"))
CHANGES_CSV = os.environ.get("CHANGES_CSV", os.path.join(DATA_DIR, "dpd_pm_changes.csv"))

# =========================
# HEALTH CANADA URL
# =========================
INFO_URL = "https://health-products.canada.ca/dpd-bdpp/info?lang=eng&code={code}"

# =========================
# EXTRACTION (robust)
# =========================
DATE_RE = re.compile(r"Date:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})")
PDF_RE = re.compile(r'href="([^"]*dpd_pm[^"]*\.pdf)"', re.IGNORECASE)

def strip_html_to_text(html: str) -> str:
    """Remove HTML tags -> plain text; makes date extraction robust even if label is split by <br>/<span>."""
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def extract_pm_date_and_pdf(html: str):
    """Extract pm_date + pm_pdf_url from DPD product info page."""
    text = strip_html_to_text(html)

    pm_date = None
    m = DATE_RE.search(text)
    if m:
        pm_date = m.group(1)

    pm_pdf_url = None
    m2 = PDF_RE.search(html)
    if m2:
        pm_pdf_url = m2.group(1)
        if pm_pdf_url.startswith("/"):
            pm_pdf_url = "https://health-products.canada.ca" + pm_pdf_url

    return pm_date, pm_pdf_url

# =========================
# EMAIL (SMTP) via ENV
# =========================
SMTP_SERVER = os.environ.get("SMTP_SERVER", "")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")  # set via GitHub Secret
EMAIL_FROM = os.environ.get("EMAIL_FROM", SMTP_USER)
EMAIL_TO = os.environ.get("EMAIL_TO", "")  # comma-separated
EMAIL_SUBJECT = os.environ.get("EMAIL_SUBJECT", "🚨 DPD PM/Vet date changed")

def send_email_alert(changes_df):
    if not (SMTP_SERVER and SMTP_USER and SMTP_PASSWORD and EMAIL_TO):
        print("ℹ️ Email non configuré — alerte ignorée")
        return

    msg = EmailMessage()
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    msg["Subject"] = "🚨 Health Canada – Product Monograph date changed"

    # ✅ UNIQUEMENT la liste des Drug_code
    drug_codes = sorted(changes_df["Drug_code"].astype(str).unique())

    body = [
        "The Product Monograph / Veterinary Labelling date has changed",
        "for the following Drug_code(s):",
        "",
    ]

    for dc in drug_codes:
        body.append(f"- {dc}")

    msg.set_content("\n".join(body))

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)

    print(f"✅ Email envoyé ({len(drug_codes)} Drug_code)")

# =========================
# MAIN
# =========================
def main():
    print(f"📥 Reading Excel: {INPUT_XLSX}")

    # sheet index or name
    sheet = int(INPUT_SHEET) if INPUT_SHEET.isdigit() else INPUT_SHEET

    df_in = pd.read_excel(INPUT_XLSX, sheet_name=sheet, dtype=str)

    if INPUT_COLUMN not in df_in.columns:
        raise KeyError(f"Column '{INPUT_COLUMN}' not found. Available: {list(df_in.columns)}")

    df_in[INPUT_COLUMN] = df_in[INPUT_COLUMN].astype(str).str.strip()
    df_in = df_in[df_in[INPUT_COLUMN].ne("")].copy()

    codes = df_in[INPUT_COLUMN].drop_duplicates().tolist()
    print(f"✅ {len(codes)} drug_code to check")

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (DPD PM date monitor)"})

    rows = []
    for i, code in enumerate(codes, start=1):
        url = INFO_URL.format(code=code)

        pm_date, pm_pdf = None, None
        status = None
        error = None

        try:
            r = session.get(url, timeout=30)
            status = r.status_code
            if status == 200:
                pm_date, pm_pdf = extract_pm_date_and_pdf(r.text)
        except Exception as e:
            error = str(e)

        rows.append({
            "Drug_code": code,
            "pm_date": pm_date,
            "pm_pdf_url": pm_pdf,
            "http_status": status,
            "info_url": url,
            "error": error
        })

        if i % 10 == 0 or i == len(codes):
            found = sum(1 for x in rows if x.get("pm_date"))
            print(f"Progress: {i}/{len(codes)} | pm_date found: {found}")

        time.sleep(0.2)

    df_current = pd.DataFrame(rows)
    df_current.to_csv(CURRENT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ Current snapshot written: {CURRENT_CSV}")

    # Load history (previous run)
    if os.path.exists(HISTORY_CSV):
        df_hist = pd.read_csv(HISTORY_CSV, dtype=str)
    else:
        df_hist = pd.DataFrame(columns=["Drug_code", "pm_date"])

    # Compare
    df_compare = df_current.merge(
        df_hist.rename(columns={"pm_date": "old_pm_date"}),
        on="Drug_code",
        how="left"
    )

    changes = df_compare[
        df_compare["pm_date"].notna() &
        (df_compare["pm_date"] != df_compare["old_pm_date"])
    ].copy()

    changes.to_csv(CHANGES_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ Changes written: {CHANGES_CSV} | count={len(changes)}")

    # Send email if changes
    if len(changes) > 0:
        send_email_alert(changes)
    else:
        print("✅ No changes detected. No email sent.")

    # Update history (store only last known pm_date)
    df_current[["Drug_code", "pm_date"]].to_csv(HISTORY_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ History updated: {HISTORY_CSV}")

if __name__ == "__main__":
    main()
