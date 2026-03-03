import os
import re
import time
import requests
import pandas as pd
from datetime import datetime

# =========================
# PATHS
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))

DATA_DIR = os.environ.get("DATA_DIR", os.path.join(REPO_ROOT, "Data"))  # <-- tu es en /Data/ dans tes logs
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
# EXTRACTION
# =========================
DATE_RE = re.compile(r"Date:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})")
PDF_RE = re.compile(r'href="([^"]*dpd_pm[^"]*\.pdf)"', re.IGNORECASE)

def strip_html_to_text(html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def extract_pm_date_and_pdf(html: str):
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
# EMAIL via MICROSOFT GRAPH (OAuth)
# =========================
# Secrets à fournir dans GitHub:
# TENANT_ID, CLIENT_ID, CLIENT_SECRET, SENDER_UPN, EMAIL_TO
# EMAIL_TO peut être: "a@x.com,b@y.com"
def send_email_alert_graph(changes_df: pd.DataFrame):
    tenant_id = os.environ.get("TENANT_ID", "")
    client_id = os.environ.get("CLIENT_ID", "")
    client_secret = os.environ.get("CLIENT_SECRET", "")
    sender_upn = os.environ.get("SENDER_UPN", "")
    email_to = os.environ.get("EMAIL_TO", "")

    if not all([tenant_id, client_id, client_secret, sender_upn, email_to]):
        print("ℹ️ Graph email not configured (missing TENANT_ID/CLIENT_ID/CLIENT_SECRET/SENDER_UPN/EMAIL_TO). Skipping email.")
        return

    # Liste des drug_codes changés (UNIQUEMENT)
    drug_codes = sorted(changes_df["Drug_code"].astype(str).unique().tolist())
    body_lines = ["Drug_code(s) with changed Product Monograph date:", ""] + [f"- {dc}" for dc in drug_codes]
    body_text = "\n".join(body_lines)

    # Acquire token (client credentials)
    import msal  # dependency

    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        authority=authority,
        client_credential=client_secret
    )
    token = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" not in token:
        raise RuntimeError(f"Unable to acquire token: {token}")

    access_token = token["access_token"]

    # SendMail endpoint for app-only: /users/{sender_upn}/sendMail
    # (client credentials -> you target a user mailbox) [4](https://stackoverflow.com/questions/76805337/python-send-email-using-graph-api-and-office365-rest-python-client)[6](https://stackoverflow.com/questions/69080522/send-mail-via-microsoft-graph-as-application-any-user)
    endpoint = f"https://graph.microsoft.com/v1.0/users/{sender_upn}/sendMail"

    recipients = [{"emailAddress": {"address": addr.strip()}} for addr in email_to.split(",") if addr.strip()]

    payload = {
        "message": {
            "subject": "🚨 Health Canada – PM date changed",
            "body": {
                "contentType": "Text",
                "content": body_text
            },
            "toRecipients": recipients
        },
        "saveToSentItems": "false"
    }

    resp = requests.post(
        endpoint,
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        json=payload,
        timeout=30
    )
    if resp.status_code >= 300:
        raise RuntimeError(f"Graph sendMail failed {resp.status_code}: {resp.text}")

    print(f"✅ Graph email sent ({len(drug_codes)} Drug_code).")

# =========================
# MAIN
# =========================
def main():
    print(f"📥 Reading Excel: {INPUT_XLSX}")

    sheet = int(INPUT_SHEET) if str(INPUT_SHEET).isdigit() else INPUT_SHEET
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
        status, error = None, None

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

        found = sum(1 for x in rows if x.get("pm_date"))
        print(f"Progress: {i}/{len(codes)} | pm_date found: {found}")
        time.sleep(0.2)

    df_current = pd.DataFrame(rows)
    df_current.to_csv(CURRENT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ Current snapshot written: {CURRENT_CSV}")

    # History
    if os.path.exists(HISTORY_CSV):
        df_hist = pd.read_csv(HISTORY_CSV, dtype=str)
    else:
        df_hist = pd.DataFrame(columns=["Drug_code", "pm_date"])

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

    # ✅ Email ONLY drug_codes that changed
    if len(changes) > 0:
        send_email_alert_graph(changes)
    else:
        print("✅ No changes detected. No email sent.")

    # Update history
    df_current[["Drug_code", "pm_date"]].to_csv(HISTORY_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ History updated: {HISTORY_CSV}")

if __name__ == "__main__":
    main()
